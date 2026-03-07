#!/usr/bin/env python3
"""Live dashboard server — shows Ray cluster topology, task placement, and benchmark results."""

import asyncio
import json
import math
import os
import time
from pathlib import Path

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, StreamingResponse

app = FastAPI()

# Async HTTP client for proxying to Ray dashboard
_ray_client = httpx.AsyncClient(base_url="http://localhost:8265", timeout=30.0)

TEMPLATE_DIR = Path(__file__).parent / "templates"

# In-memory state
state = {
    # Cluster topology
    "nodes": {},          # node_ip -> {site_id, num_cpus, num_gpus, cluster_name, scheduler_type, joined_at}
    "ray_head_ip": os.environ.get("RAY_HEAD_IP", ""),
    "head_node": {},      # {ip, cluster_name, scheduler_type} — coordinator, not a compute site
    # Workload
    "workload_type": "benchmark",  # "benchmark" or "fractal"
    "phase": "waiting",   # waiting, throughput, compute, scaling, rendering, cluster_ready, user_script, script_failed, complete
    "tasks": [],          # [{task_id, type, node_ip, site_id, duration_ms, result}]
    "total_planned": 0,
    "total_completed": 0,
    # Per-site stats
    "site_stats": {},     # site_id -> {task_count, total_ms, cluster_name, scheduler_type, num_workers, node_ips}
    "start_time": None,
    # Benchmark results
    "throughput_results": None,   # {tasks_per_sec, site_breakdown}
    "compute_results": [],        # [{node_ip, site_id, gflops, matrix_size}]
    "scaling_results": None,      # {single_site_tps, dual_site_tps, speedup}
    # Fractal state
    "grid_size": 0,
    "image_size": 0,
    "fractal_tiles": {},  # (tx, ty) string key -> tile data
    # Pending sites (dispatched but workers not yet connected)
    "pending_sites": {},  # site_id -> {cluster_name, scheduler_type}
}
connected_ws = []  # list of WebSocket


def _reset_state():
    state["nodes"] = {}
    state["workload_type"] = "benchmark"  # "benchmark", "fractal", or "cluster_only"
    state["phase"] = "waiting"  # waiting, throughput, compute, scaling, rendering, cluster_ready, user_script, script_failed, complete
    state["tasks"] = []
    state["total_planned"] = 0
    state["total_completed"] = 0
    state["site_stats"] = {}
    state["start_time"] = None
    state["throughput_results"] = None
    state["compute_results"] = []
    state["scaling_results"] = None
    state["grid_size"] = 0
    state["image_size"] = 0
    state["fractal_tiles"] = {}
    state["_seen_jobs"] = {}
    state["_ray_task_counts"] = {}
    global _dashboard_start_time
    _dashboard_start_time = time.time()
    # Preserve pending_sites and head_node across config resets — they are set
    # by dispatch/start_ray_head before the benchmark or cluster_ready sends /api/config
    # state["pending_sites"] is NOT reset here
    # state["head_node"] is NOT reset here


def _compute_throughput_history():
    """Build per-second throughput buckets from task completion times."""
    if not state["start_time"] or not state["tasks"]:
        return []
    arrivals = []
    for t in state["tasks"]:
        ts = t.get("completed_at", 0)
        if ts > 0:
            arrivals.append((ts - state["start_time"], t.get("site_id", "unknown")))
    if not arrivals:
        return []
    arrivals.sort()
    max_t = arrivals[-1][0]
    buckets = []
    bucket_start = 0
    while bucket_start <= max_t:
        bucket_end = bucket_start + 1.0
        per_site = {}
        total = 0
        for rel_t, sid in arrivals:
            if bucket_start <= rel_t < bucket_end:
                per_site[sid] = per_site.get(sid, 0) + 1
                total += 1
        buckets.append({"ts_offset": round(bucket_start, 1), "total": total, "perSite": per_site})
        bucket_start += 1.0
    return buckets


async def _broadcast(msg_dict):
    """Send JSON message to all connected WebSocket clients."""
    msg = json.dumps(msg_dict)
    stale = []
    for ws in connected_ws:
        try:
            await ws.send_text(msg)
        except Exception:
            stale.append(ws)
    for ws in stale:
        connected_ws.remove(ws)


# ---------------------------------------------------------------------------
# Background poller — fetch node & job info from Ray's built-in dashboard API
# and sync into the dashboard's state so topology, progress, and throughput
# all work for user-submitted jobs (not just the built-in benchmark).
# ---------------------------------------------------------------------------
import logging
_poll_log = logging.getLogger("dashboard.poller")

state["ray_jobs"] = []
state["ray_cluster_nodes"] = []
# Track which jobs we've already counted so we don't double-count
state["_seen_jobs"] = {}  # job_id -> last known status
state["_ray_task_counts"] = {}  # Track Ray task counts: {total, by_state, by_func}
_dashboard_start_time = time.time()  # ignore jobs that finished before dashboard started


_ray_connect_failures = 0

async def _fetch_json(path: str):
    """GET a Ray API endpoint, return parsed JSON or None."""
    global _ray_connect_failures
    try:
        resp = await _ray_client.get(path)
        if _ray_connect_failures > 0:
            _poll_log.info(f"Ray API reconnected after {_ray_connect_failures} failures")
            _ray_connect_failures = 0
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code != 404:
            _poll_log.warning(f"Ray API {path}: {resp.status_code}")
    except httpx.ConnectError:
        _ray_connect_failures += 1
        if _ray_connect_failures == 1 or _ray_connect_failures % 12 == 0:
            _poll_log.warning(f"Cannot connect to Ray dashboard at {_ray_client.base_url} ({_ray_connect_failures} failures)")
    return None


async def _fetch_jobs_list():
    """Fetch jobs from Ray API. Prefer /api/jobs/ (flat list with timestamps)."""
    # /api/jobs/ returns a flat list with start_time/end_time fields
    data = await _fetch_json("/api/jobs/")
    if isinstance(data, list) and data:
        return data
    # /api/v0/jobs returns nested: {"data": {"result": {"result": [...]}}}
    data = await _fetch_json("/api/v0/jobs")
    if isinstance(data, dict):
        jobs = data.get("jobs", [])
        if not jobs:
            # Nested v0 format
            inner = data.get("data", {})
            if isinstance(inner, dict):
                inner = inner.get("result", {})
                if isinstance(inner, dict):
                    jobs = inner.get("result", [])
        if isinstance(jobs, list):
            return jobs
    if isinstance(data, list):
        return data
    return []


async def _poll_ray_api():
    """Periodically poll Ray's REST API and sync into dashboard state."""
    logged_first = False

    while True:
        try:
            changed = False
            head_ip = state.get("ray_head_ip", "")

            # --- Fetch and sync nodes ---
            # Ray's /nodes?view=summary nests key fields under "raylet":
            #   raylet.state = "ALIVE"
            #   raylet.nodeId = "..."
            #   raylet.nodeManagerAddress = IP
            #   raylet.nodeManagerHostname = hostname
            #   raylet.isHeadNode = true/false
            #   raylet.resourcesTotal.CPU = count
            #   raylet.resourcesTotal.GPU = count
            # Top-level: ip, hostname (duplicates), cpus (array), gpus (array)
            data = await _fetch_json("/nodes?view=summary")
            if data is not None:
                nodes_summary = data.get("data", {}).get("summary", [])
                ray_info = []
                for node in nodes_summary:
                    raylet = node.get("raylet", {})
                    resources = raylet.get("resourcesTotal", {})
                    ip = node.get("ip", "") or raylet.get("nodeManagerAddress", "")
                    hostname = node.get("hostname", "") or raylet.get("nodeManagerHostname", "")
                    alive = raylet.get("state", "") == "ALIVE"
                    is_head = raylet.get("isHeadNode", False)
                    cpus = resources.get("CPU", 0)
                    if not isinstance(cpus, (int, float)):
                        cpus = 0
                    gpus = resources.get("GPU", 0)
                    if not isinstance(gpus, (int, float)):
                        gpus = 0
                    ray_info.append({
                        "node_id": raylet.get("nodeId", ""),
                        "ip": ip, "hostname": hostname,
                        "state": raylet.get("state", ""),
                        "cpus": int(cpus), "gpus": int(gpus),
                        "alive": alive, "is_head": is_head,
                    })

                    # Sync worker nodes into state["nodes"] for topology
                    # Skip the head node (coordinator)
                    if alive and ip and not is_head and cpus > 0:
                        if ip not in state["nodes"]:
                            site_id = f"site-{hostname.split('.')[0]}" if hostname else f"site-{ip}"
                            state["nodes"][ip] = {
                                "site_id": site_id,
                                "num_cpus": int(cpus),
                                "num_gpus": int(gpus),
                                "cluster_name": hostname.split(".")[0] if hostname else ip,
                                "scheduler_type": "",
                                "joined_at": time.time(),
                            }
                            # Initialize site_stats for topology rendering
                            if site_id not in state["site_stats"]:
                                state["site_stats"][site_id] = {
                                    "task_count": 0,
                                    "total_ms": 0,
                                    "cluster_name": hostname.split(".")[0] if hostname else ip,
                                    "scheduler_type": "",
                                    "num_workers": 0,
                                    "node_ips": [],
                                }
                            stats = state["site_stats"][site_id]
                            stats["num_workers"] += 1
                            if ip not in stats["node_ips"]:
                                stats["node_ips"].append(ip)
                            changed = True

                state["ray_cluster_nodes"] = ray_info
                if not logged_first and ray_info:
                    _poll_log.info(f"Ray poll: {len(ray_info)} nodes, sample keys: {list(nodes_summary[0].keys()) if nodes_summary else []}")

            # --- Fetch and sync jobs ---
            jobs_list = await _fetch_jobs_list()
            if jobs_list:
                state["ray_jobs"] = sorted(
                    jobs_list, key=lambda j: j.get("start_time", 0) or 0, reverse=True
                )[:20]

                if not logged_first:
                    _poll_log.info(f"Ray poll: {len(jobs_list)} jobs, sample keys: {list(jobs_list[0].keys()) if jobs_list else []}")

                # Sync jobs into task/progress system.
                # Only track jobs that started AFTER the dashboard launched
                # to avoid counting stale jobs from previous Ray sessions.
                for job in jobs_list:
                    job_id = job.get("job_id") or job.get("submission_id") or ""
                    if not job_id:
                        continue

                    # Skip jobs that are already finished and started before this session
                    status = job.get("status", "UNKNOWN")
                    prev_status = state["_seen_jobs"].get(job_id)
                    if prev_status is None and status in ("SUCCEEDED", "FAILED", "STOPPED"):
                        # Check if this job ended before the dashboard started
                        end_ts = job.get("end_time", 0) or 0
                        if end_ts > 1e12:
                            end_ts /= 1000  # ms -> s
                        if end_ts > 0 and end_ts < _dashboard_start_time:
                            state["_seen_jobs"][job_id] = status  # mark seen, skip
                            continue

                    if prev_status == status:
                        continue  # No change
                    state["_seen_jobs"][job_id] = status

                    # Also skip DRIVER-type jobs (e.g. ray.init() health checks)
                    if job.get("type") == "DRIVER" and not job.get("submission_id"):
                        continue

                    # Job just started running — count as a planned task
                    if status == "RUNNING" and prev_status is None:
                        state["total_planned"] += 1
                        if state["start_time"] is None:
                            state["start_time"] = time.time()
                        if state["phase"] == "waiting":
                            state["phase"] = "user_script"
                        changed = True

                    # Job completed — count as a completed task
                    if status in ("SUCCEEDED", "FAILED", "STOPPED") and prev_status not in ("SUCCEEDED", "FAILED", "STOPPED"):
                        if prev_status is None:
                            # We missed the RUNNING state — count as planned too
                            state["total_planned"] += 1
                            if state["start_time"] is None:
                                state["start_time"] = time.time()
                        state["total_completed"] += 1
                        now = time.time()
                        duration_ms = 0
                        start = job.get("start_time", 0) or 0
                        end = job.get("end_time", 0) or 0
                        if start and end:
                            # Normalize timestamps
                            if start > 1e12:
                                start /= 1000
                            if end > 1e12:
                                end /= 1000
                            duration_ms = (end - start) * 1000

                        # Pick a node to attribute this job to
                        task_node_ip = ""
                        task_site_id = "cluster"
                        for nip, ndata in state["nodes"].items():
                            if nip != head_ip:
                                task_node_ip = nip
                                task_site_id = ndata.get("site_id", "cluster")
                                break

                        task = {
                            "task_id": job_id,
                            "type": "ray_job",
                            "node_ip": task_node_ip,
                            "site_id": task_site_id,
                            "duration_ms": duration_ms,
                            "result": status,
                            "completed_at": now,
                            "cluster_name": "",
                            "scheduler_type": "",
                        }
                        state["tasks"].append(task)

                        # Update site_stats
                        if task_site_id in state["site_stats"]:
                            state["site_stats"][task_site_id]["task_count"] += 1
                            state["site_stats"][task_site_id]["total_ms"] += duration_ms

                        changed = True

                # Check if all tracked jobs are done
                running = sum(1 for j in jobs_list if j.get("status") == "RUNNING")
                if state["total_completed"] > 0 and running == 0 and state["phase"] == "user_script":
                    state["phase"] = "complete"
                    changed = True

            # --- Fetch Ray task-level counts (individual @ray.remote invocations) ---
            task_summary = await _fetch_json("/api/v0/tasks")
            if task_summary is not None:
                # /api/v0/tasks returns task list or summary depending on Ray version
                # Try to extract task counts
                tasks_data = task_summary
                if isinstance(task_summary, dict):
                    tasks_data = task_summary.get("data", task_summary)
                    if isinstance(tasks_data, dict):
                        tasks_data = tasks_data.get("result", tasks_data)

                ray_tasks_total = 0
                ray_tasks_finished = 0
                ray_tasks_by_state = {}
                ray_tasks_by_func = {}
                ray_tasks_by_node = {}

                if isinstance(tasks_data, dict) and "summary" in tasks_data:
                    # Summary format: {summary: {func_name: {state: count}}}
                    for func_name, state_counts in tasks_data["summary"].items():
                        if isinstance(state_counts, dict):
                            func_total = 0
                            for tstate, count in state_counts.items():
                                if isinstance(count, (int, float)):
                                    ray_tasks_total += int(count)
                                    func_total += int(count)
                                    ray_tasks_by_state[tstate] = ray_tasks_by_state.get(tstate, 0) + int(count)
                                    if tstate in ("FINISHED", "FAILED"):
                                        ray_tasks_finished += int(count)
                            ray_tasks_by_func[func_name] = func_total
                elif isinstance(tasks_data, list):
                    # Flat list of task objects
                    for task_obj in tasks_data:
                        ray_tasks_total += 1
                        tstate = task_obj.get("state", task_obj.get("scheduling_state", "UNKNOWN"))
                        ray_tasks_by_state[tstate] = ray_tasks_by_state.get(tstate, 0) + 1
                        if tstate in ("FINISHED", "FAILED"):
                            ray_tasks_finished += 1
                        func_name = task_obj.get("func_or_class_name", task_obj.get("name", "unknown"))
                        ray_tasks_by_func[func_name] = ray_tasks_by_func.get(func_name, 0) + 1
                        node_id = task_obj.get("node_id", "")
                        if node_id:
                            ray_tasks_by_node[node_id] = ray_tasks_by_node.get(node_id, 0) + 1

                old_counts = state.get("_ray_task_counts", {})
                if ray_tasks_total > 0 and (
                    ray_tasks_total != old_counts.get("total", 0) or
                    ray_tasks_finished != old_counts.get("finished", 0)
                ):
                    state["_ray_task_counts"] = {
                        "total": ray_tasks_total,
                        "finished": ray_tasks_finished,
                        "by_state": ray_tasks_by_state,
                        "by_func": ray_tasks_by_func,
                        "by_node": ray_tasks_by_node,
                    }
                    # Update task placement counts using real Ray task counts
                    # instead of job-level counts
                    if state["phase"] in ("user_script", "complete"):
                        state["total_planned"] = max(state["total_planned"], ray_tasks_total)
                        state["total_completed"] = max(state["total_completed"], ray_tasks_finished)
                    changed = True

            logged_first = True

            if changed:
                await _broadcast({
                    "type": "worker",
                    "nodes": state["nodes"],
                    "site_stats": _safe_site_stats(),
                    "pending_sites": state["pending_sites"],
                    "total_completed": state["total_completed"],
                    "total_planned": state["total_planned"],
                    "phase": state["phase"],
                })

            # Always broadcast the raw ray cluster data for the sidebar
            await _broadcast({
                "type": "ray_cluster",
                "ray_cluster_nodes": state.get("ray_cluster_nodes", []),
                "ray_jobs": state.get("ray_jobs", []),
                "ray_task_counts": state.get("_ray_task_counts", {}),
            })
        except Exception as e:
            _poll_log.warning(f"Ray API poll error: {e}", exc_info=True)
        await asyncio.sleep(5)

@app.on_event("startup")
async def start_poller():
    asyncio.create_task(_poll_ray_api())


@app.get("/", response_class=HTMLResponse)
async def index():
    return (TEMPLATE_DIR / "index.html").read_text()


@app.post("/api/config")
async def set_config(request: Request):
    """Set benchmark configuration. Resets state."""
    body = await request.json()
    _reset_state()
    state["workload_type"] = body.get("workload_type", "benchmark")
    state["total_planned"] = body.get("total_tasks", body.get("total_tiles", 0))
    state["ray_head_ip"] = body.get("ray_head_ip", state["ray_head_ip"])
    state["grid_size"] = body.get("grid_size", 0)
    state["image_size"] = body.get("image_size", 0)
    await _broadcast({"type": "config", **body, "workload_type": state["workload_type"]})
    return {"status": "ok"}


@app.post("/api/head")
async def register_head(request: Request):
    """Register the Ray head node (coordinator only — not a compute site)."""
    body = await request.json()
    state["head_node"] = {
        "ip": body.get("ip", ""),
        "cluster_name": body.get("cluster_name", ""),
        "scheduler_type": body.get("scheduler_type", ""),
    }
    state["ray_head_ip"] = body.get("ip", state["ray_head_ip"])
    await _broadcast({
        "type": "head",
        "head_node": state["head_node"],
    })
    return {"status": "ok"}


@app.post("/api/worker/pending")
async def worker_pending(request: Request):
    """Mark a site as pending (dispatched but workers not yet connected)."""
    body = await request.json()
    site_id = body["site_id"]
    state["pending_sites"][site_id] = {
        "cluster_name": body.get("cluster_name", ""),
        "scheduler_type": body.get("scheduler_type", ""),
    }
    await _broadcast({
        "type": "pending_site",
        "site_id": site_id,
        "cluster_name": body.get("cluster_name", ""),
        "scheduler_type": body.get("scheduler_type", ""),
    })
    return {"status": "ok"}


@app.post("/api/worker")
async def register_worker(request: Request):
    """Register a Ray worker node."""
    body = await request.json()
    node_ip = body.get("worker_ip", "unknown")
    site_id = body.get("site_id", "unknown")

    state["nodes"][node_ip] = {
        "site_id": site_id,
        "num_cpus": body.get("num_cpus", 1),
        "num_gpus": body.get("num_gpus", 0),
        "cluster_name": body.get("cluster_name", ""),
        "scheduler_type": body.get("scheduler_type", ""),
        "joined_at": time.time(),
    }

    # Remove from pending — site is now connected
    state["pending_sites"].pop(site_id, None)

    # Initialize site stats if needed
    if site_id not in state["site_stats"]:
        state["site_stats"][site_id] = {
            "task_count": 0,
            "total_ms": 0,
            "cluster_name": body.get("cluster_name", ""),
            "scheduler_type": body.get("scheduler_type", ""),
            "num_workers": 0,
            "node_ips": [],
        }
    stats = state["site_stats"][site_id]
    stats["num_workers"] += 1
    if node_ip not in stats["node_ips"]:
        stats["node_ips"].append(node_ip)
    if body.get("cluster_name"):
        stats["cluster_name"] = body["cluster_name"]
    if body.get("scheduler_type"):
        stats["scheduler_type"] = body["scheduler_type"]

    await _broadcast({
        "type": "worker",
        "node_ip": node_ip,
        "site_id": site_id,
        "num_cpus": body.get("num_cpus", 1),
        "num_gpus": body.get("num_gpus", 0),
        "cluster_name": body.get("cluster_name", ""),
        "scheduler_type": body.get("scheduler_type", ""),
        "nodes": state["nodes"],
        "site_stats": _safe_site_stats(),
        "pending_sites": state["pending_sites"],
    })
    return {"status": "ok"}


@app.post("/api/phase")
async def set_phase(request: Request):
    """Update the current benchmark phase."""
    body = await request.json()
    state["phase"] = body.get("phase", state["phase"])
    await _broadcast({"type": "phase", "phase": state["phase"]})
    return {"status": "ok"}


@app.post("/api/task")
async def receive_task(request: Request):
    """Receive a completed benchmark task result."""
    task = await request.json()
    now = time.time()

    if state["start_time"] is None:
        state["start_time"] = now

    task["completed_at"] = now
    state["tasks"].append(task)
    state["total_completed"] += 1

    site_id = task.get("site_id", "unknown")
    node_ip = task.get("node_ip", "unknown")

    # Register node if not already known
    if node_ip not in state["nodes"]:
        state["nodes"][node_ip] = {
            "site_id": site_id,
            "num_cpus": 1,
            "num_gpus": 0,
            "cluster_name": task.get("cluster_name", ""),
            "scheduler_type": task.get("scheduler_type", ""),
            "joined_at": now,
        }

    # Update site stats
    if site_id not in state["site_stats"]:
        state["site_stats"][site_id] = {
            "task_count": 0,
            "total_ms": 0,
            "cluster_name": task.get("cluster_name", ""),
            "scheduler_type": task.get("scheduler_type", ""),
            "num_workers": 0,
            "node_ips": [],
        }
    stats = state["site_stats"][site_id]
    stats["task_count"] += 1
    stats["total_ms"] += task.get("duration_ms", 0)
    if task.get("cluster_name"):
        stats["cluster_name"] = task["cluster_name"]
    # Propagate cluster_name to task result if site_stats already has one (from worker registration)
    if not task.get("cluster_name") and stats.get("cluster_name"):
        task["cluster_name"] = stats["cluster_name"]
    if task.get("scheduler_type"):
        stats["scheduler_type"] = task["scheduler_type"]
    if node_ip not in stats["node_ips"]:
        stats["node_ips"].append(node_ip)

    # Broadcast
    await _broadcast({
        "type": "task",
        "task_id": task.get("task_id"),
        "task_type": task.get("type"),
        "site_id": site_id,
        "node_ip": node_ip,
        "duration_ms": task.get("duration_ms", 0),
        "result": task.get("result"),
        "total_completed": state["total_completed"],
        "total_planned": state["total_planned"],
        "phase": state["phase"],
        "site_stats": _safe_site_stats(),
        "elapsed_s": round(now - state["start_time"], 1) if state["start_time"] else 0,
    })
    return {"status": "ok", "total_completed": state["total_completed"]}


@app.post("/api/tile")
async def receive_tile(request: Request):
    """Receive a completed fractal tile."""
    tile = await request.json()
    now = time.time()

    if state["start_time"] is None:
        state["start_time"] = now

    tx = tile.get("tile_x", 0)
    ty = tile.get("tile_y", 0)
    key = f"{tx},{ty}"
    state["fractal_tiles"][key] = {
        "tile_x": tx,
        "tile_y": ty,
        "site_id": tile.get("site_id", "unknown"),
        "render_time_ms": tile.get("render_time_ms", 0),
        "png_b64": tile.get("png_b64", ""),
        "cluster_name": tile.get("cluster_name", ""),
        "scheduler_type": tile.get("scheduler_type", ""),
        "node_ip": tile.get("node_ip", "unknown"),
    }
    state["total_completed"] += 1

    site_id = tile.get("site_id", "unknown")
    node_ip = tile.get("node_ip", "unknown")

    # Register node if not already known
    if node_ip not in state["nodes"]:
        state["nodes"][node_ip] = {
            "site_id": site_id,
            "num_cpus": 1,
            "num_gpus": 0,
            "cluster_name": tile.get("cluster_name", ""),
            "scheduler_type": tile.get("scheduler_type", ""),
            "joined_at": now,
        }

    # Update site stats
    if site_id not in state["site_stats"]:
        state["site_stats"][site_id] = {
            "task_count": 0,
            "total_ms": 0,
            "cluster_name": tile.get("cluster_name", ""),
            "scheduler_type": tile.get("scheduler_type", ""),
            "num_workers": 0,
            "node_ips": [],
        }
    stats = state["site_stats"][site_id]
    stats["task_count"] += 1
    stats["total_ms"] += tile.get("render_time_ms", 0)
    if tile.get("cluster_name"):
        stats["cluster_name"] = tile["cluster_name"]
    if tile.get("scheduler_type"):
        stats["scheduler_type"] = tile["scheduler_type"]
    if node_ip not in stats["node_ips"]:
        stats["node_ips"].append(node_ip)

    await _broadcast({
        "type": "tile",
        "tile_x": tx,
        "tile_y": ty,
        "site_id": site_id,
        "render_time_ms": tile.get("render_time_ms", 0),
        "png_b64": tile.get("png_b64", ""),
        "completed": state["total_completed"],
        "total": state["total_planned"],
        "site_stats": _safe_site_stats(),
        "elapsed_s": round(now - state["start_time"], 1) if state["start_time"] else 0,
    })
    return {"status": "ok", "total_completed": state["total_completed"]}


@app.post("/api/results")
async def receive_results(request: Request):
    """Receive benchmark phase results."""
    body = await request.json()
    result_type = body.get("type")

    if result_type == "throughput":
        state["throughput_results"] = body.get("data")
    elif result_type == "compute":
        state["compute_results"].append(body.get("data"))
    elif result_type == "scaling":
        state["scaling_results"] = body.get("data")

    await _broadcast({
        "type": "results",
        "result_type": result_type,
        "data": body.get("data"),
    })
    return {"status": "ok"}


def _safe_site_stats():
    """Return site stats without internal fields."""
    return {k: {kk: vv for kk, vv in v.items()} for k, v in state["site_stats"].items()}


@app.get("/api/debug/ray")
async def debug_ray():
    """Debug endpoint — shows raw Ray API poll results."""
    debug = {"ray_cluster_nodes": state.get("ray_cluster_nodes", []), "ray_jobs": state.get("ray_jobs", []), "ray_task_counts": state.get("_ray_task_counts", {})}
    # Also try fetching live to show raw responses
    raw = {}
    for path in ["/nodes?view=summary", "/api/v0/jobs", "/api/jobs/", "/api/v0/tasks"]:
        try:
            resp = await _ray_client.get(path)
            raw[path] = {"status": resp.status_code, "body": resp.json() if resp.status_code == 200 else resp.text[:500]}
        except Exception as e:
            raw[path] = {"error": str(e)}
    debug["raw_api_responses"] = raw
    return debug


@app.get("/api/state")
async def get_state():
    """Return full state for late-joining browsers."""
    result = {
        "nodes": state["nodes"],
        "ray_head_ip": state["ray_head_ip"],
        "head_node": state["head_node"],
        "workload_type": state["workload_type"],
        "phase": state["phase"],
        "total_completed": state["total_completed"],
        "total_planned": state["total_planned"],
        "site_stats": _safe_site_stats(),
        "throughput_results": state["throughput_results"],
        "compute_results": state["compute_results"],
        "scaling_results": state["scaling_results"],
        "elapsed_s": round(time.time() - state["start_time"], 1) if state["start_time"] else 0,
        "throughput_history": _compute_throughput_history(),
        "pending_sites": state["pending_sites"],
        "ray_cluster_nodes": state.get("ray_cluster_nodes", []),
        "ray_jobs": state.get("ray_jobs", []),
        "ray_task_counts": state.get("_ray_task_counts", {}),
    }
    if state["workload_type"] == "fractal":
        result["grid_size"] = state["grid_size"]
        result["image_size"] = state["image_size"]
        result["fractal_tiles"] = state["fractal_tiles"]
    return result


@app.api_route("/ray/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def ray_dashboard_proxy(request: Request, path: str):
    """Reverse proxy to Ray's native dashboard on port 8265."""
    url = f"/{path}"
    if request.query_params:
        url += f"?{request.query_params}"
    try:
        body = await request.body()
        resp = await _ray_client.request(
            method=request.method,
            url=url,
            headers={k: v for k, v in request.headers.items()
                     if k.lower() not in ("host", "connection")},
            content=body if body else None,
        )
        excluded = {"transfer-encoding", "connection", "content-encoding"}
        headers = {k: v for k, v in resp.headers.items() if k.lower() not in excluded}
        return StreamingResponse(
            content=iter([resp.content]),
            status_code=resp.status_code,
            headers=headers,
        )
    except httpx.ConnectError:
        return {"error": "Ray dashboard not available on port 8265"}
    except Exception as e:
        return {"error": str(e)}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    connected_ws.append(ws)
    try:
        await ws.send_text(json.dumps({
            "type": "init",
            "nodes": state["nodes"],
            "ray_head_ip": state["ray_head_ip"],
            "head_node": state["head_node"],
            "workload_type": state["workload_type"],
            "phase": state["phase"],
            "total_completed": state["total_completed"],
            "total_planned": state["total_planned"],
            "site_stats": _safe_site_stats(),
            "grid_size": state["grid_size"],
            "image_size": state["image_size"],
            "pending_sites": state["pending_sites"],
            "ray_cluster_nodes": state.get("ray_cluster_nodes", []),
            "ray_jobs": state.get("ray_jobs", []),
            "ray_task_counts": state.get("_ray_task_counts", {}),
        }))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        if ws in connected_ws:
            connected_ws.remove(ws)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("DASHBOARD_PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
