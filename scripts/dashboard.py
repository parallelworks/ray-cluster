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
    "nodes": {},          # node_ip -> {site_id, num_cpus, cluster_name, scheduler_type, joined_at}
    "ray_head_ip": os.environ.get("RAY_HEAD_IP", ""),
    # Workload
    "workload_type": "benchmark",  # "benchmark" or "fractal"
    "phase": "waiting",   # waiting, throughput, compute, scaling, rendering, complete
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
}
connected_ws = []  # list of WebSocket


def _reset_state():
    state["nodes"] = {}
    state["workload_type"] = "benchmark"
    state["phase"] = "waiting"
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


@app.post("/api/worker")
async def register_worker(request: Request):
    """Register a Ray worker node."""
    body = await request.json()
    node_ip = body.get("worker_ip", "unknown")
    site_id = body.get("site_id", "unknown")

    state["nodes"][node_ip] = {
        "site_id": site_id,
        "num_cpus": body.get("num_cpus", 1),
        "cluster_name": body.get("cluster_name", ""),
        "scheduler_type": body.get("scheduler_type", ""),
        "joined_at": time.time(),
    }

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
        "cluster_name": body.get("cluster_name", ""),
        "scheduler_type": body.get("scheduler_type", ""),
        "nodes": state["nodes"],
        "site_stats": _safe_site_stats(),
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


@app.get("/api/state")
async def get_state():
    """Return full state for late-joining browsers."""
    result = {
        "nodes": state["nodes"],
        "ray_head_ip": state["ray_head_ip"],
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
            "workload_type": state["workload_type"],
            "phase": state["phase"],
            "total_completed": state["total_completed"],
            "total_planned": state["total_planned"],
            "site_stats": _safe_site_stats(),
            "grid_size": state["grid_size"],
            "image_size": state["image_size"],
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
