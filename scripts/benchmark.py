#!/usr/bin/env python3
"""Ray distributed benchmark — runs tasks across a multi-site Ray cluster."""

import argparse
import base64
import json
import os
import socket
import struct
import time
import urllib.request
import zlib
from math import log2

import numpy as np
import ray


def post_json(url, data):
    """POST JSON to the dashboard."""
    body = json.dumps(data).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"  Warning: POST to {url} failed: {e}")


def detect_site(ray_head_ip):
    """Determine site based on node IP.

    Site mapping:
      - ray_head_ip     -> site-1 (head node)
      - 127.0.X.Y       -> site-X (multi-node: X = site index, Y = node within site)
      - 127.0.0.X       -> site-X (legacy single-node)
    """
    try:
        node_ip = ray.util.get_node_ip_address()
    except Exception:
        node_ip = socket.gethostbyname(socket.gethostname())
    if node_ip == ray_head_ip:
        return "site-1", node_ip
    elif node_ip.startswith("127."):
        parts = node_ip.split(".")
        if parts[2] == "0":
            # Legacy single-node: 127.0.0.X
            site_index = int(parts[3])
        else:
            # Multi-node: 127.0.X.Y where X is the site index
            site_index = int(parts[2])
        return f"site-{site_index}", node_ip
    else:
        return "site-2", node_ip


# =============================================================================
# Ray Remote Tasks
# =============================================================================

@ray.remote(scheduling_strategy="SPREAD")
def throughput_task(task_id, ray_head_ip):
    """Task for measuring scheduling throughput across nodes."""
    start = time.time()
    # Simulate real workload latency so Ray's scheduler distributes across nodes
    time.sleep(0.05)
    total = sum(range(100000))
    duration_ms = (time.time() - start) * 1000
    site_id, node_ip = detect_site(ray_head_ip)
    return {
        "task_id": task_id,
        "type": "throughput",
        "site_id": site_id,
        "node_ip": node_ip,
        "duration_ms": round(duration_ms, 2),
        "result": total,
    }


@ray.remote(scheduling_strategy="SPREAD")
def compute_task(task_id, matrix_size, ray_head_ip):
    """CPU compute task — matrix multiplication."""
    start = time.time()
    a = np.random.randn(matrix_size, matrix_size)
    b = np.random.randn(matrix_size, matrix_size)
    c = a @ b
    duration_ms = (time.time() - start) * 1000

    # Estimate GFLOPS: matrix mult is ~2*N^3 FLOPs
    flops = 2.0 * matrix_size ** 3
    gflops = flops / (duration_ms / 1000) / 1e9

    site_id, node_ip = detect_site(ray_head_ip)
    return {
        "task_id": task_id,
        "type": "compute",
        "site_id": site_id,
        "node_ip": node_ip,
        "duration_ms": round(duration_ms, 2),
        "result": {
            "gflops": round(gflops, 2),
            "matrix_size": matrix_size,
            "checksum": round(float(c.sum()), 4),
        },
    }


@ray.remote(scheduling_strategy="SPREAD")
def scaling_task(task_id, ray_head_ip):
    """Medium task for scaling test."""
    start = time.time()
    # Moderate workload with sleep to ensure distribution across nodes
    time.sleep(0.02)
    a = np.random.randn(200, 200)
    for _ in range(5):
        a = a @ np.random.randn(200, 200)
    duration_ms = (time.time() - start) * 1000
    site_id, node_ip = detect_site(ray_head_ip)
    return {
        "task_id": task_id,
        "type": "scaling",
        "site_id": site_id,
        "node_ip": node_ip,
        "duration_ms": round(duration_ms, 2),
        "result": round(float(a.sum()), 4),
    }


@ray.remote(scheduling_strategy="SPREAD")
def fractal_tile_task(tile_x, tile_y, grid_size, img_w, img_h, palette_name, ray_head_ip):
    """Render a Mandelbrot tile. Self-contained — no external dependencies."""
    import base64
    import socket
    import struct
    import time
    import zlib
    from math import log2

    # --- Mandelbrot parameters ---
    REGION = (-2.5, -1.25, 1.0, 1.25)
    MAX_ITER = 256
    PALETTES = {
        "electric": [(0,0,0),(0,7,100),(32,107,203),(237,255,255),(255,170,0),(0,2,0)],
        "fire": [(0,0,0),(25,7,26),(109,1,31),(189,21,2),(238,113,0),(255,255,0)],
        "ocean": [(0,0,0),(0,20,50),(0,72,120),(0,150,180),(72,210,210),(200,255,255)],
        "cosmic": [(0,0,0),(20,0,40),(80,0,120),(160,30,200),(220,100,255),(255,220,255)],
    }

    def lerp_color(palette, t):
        n = len(palette) - 1
        idx = t * n
        i = int(idx)
        if i >= n:
            return palette[-1]
        f = idx - i
        c0, c1 = palette[i], palette[i + 1]
        return (int(c0[0]+(c1[0]-c0[0])*f), int(c0[1]+(c1[1]-c0[1])*f), int(c0[2]+(c1[2]-c0[2])*f))

    start = time.time()
    palette = PALETTES.get(palette_name, PALETTES["electric"])
    xmin, ymin, xmax, ymax = REGION
    tile_w = (xmax - xmin) / grid_size
    tile_h = (ymax - ymin) / grid_size
    x0 = xmin + tile_x * tile_w
    y0 = ymin + tile_y * tile_h

    pixels = []
    for py in range(img_h):
        for px in range(img_w):
            cx = x0 + (px / img_w) * tile_w
            cy = y0 + (py / img_h) * tile_h
            zx, zy = 0.0, 0.0
            iteration = 0
            while zx * zx + zy * zy <= 4.0 and iteration < MAX_ITER:
                zx, zy = zx * zx - zy * zy + cx, 2.0 * zx * zy + cy
                iteration += 1
            if iteration == MAX_ITER:
                pixels.append((0, 0, 0))
            else:
                smooth = iteration + 1 - log2(log2(max(zx * zx + zy * zy, 1.001)))
                t = (smooth / MAX_ITER) % 1.0
                pixels.append(lerp_color(palette, t))

    # Pure-Python PNG encoder
    def make_chunk(chunk_type, data):
        c = chunk_type + data
        return struct.pack(">I", len(data)) + c + struct.pack(">I", zlib.crc32(c) & 0xFFFFFFFF)

    header = b"\x89PNG\r\n\x1a\n"
    ihdr = make_chunk(b"IHDR", struct.pack(">IIBBBBB", img_w, img_h, 8, 2, 0, 0, 0))
    raw = b""
    for y in range(img_h):
        raw += b"\x00"
        for x in range(img_w):
            r, g, b = pixels[y * img_w + x]
            raw += struct.pack("BBB", r, g, b)
    idat = make_chunk(b"IDAT", zlib.compress(raw, 9))
    iend = make_chunk(b"IEND", b"")
    png_bytes = header + ihdr + idat + iend
    png_b64 = base64.b64encode(png_bytes).decode("ascii")

    duration_ms = (time.time() - start) * 1000

    # Site detection (inline to keep task self-contained)
    try:
        node_ip = ray.util.get_node_ip_address()
    except Exception:
        node_ip = socket.gethostbyname(socket.gethostname())
    if node_ip == ray_head_ip:
        site_id = "site-1"
    elif node_ip.startswith("127."):
        parts = node_ip.split(".")
        if parts[2] == "0":
            site_id = f"site-{int(parts[3])}"
        else:
            site_id = f"site-{int(parts[2])}"
    else:
        site_id = "site-2"

    return {
        "tile_x": tile_x,
        "tile_y": tile_y,
        "grid_size": grid_size,
        "width": img_w,
        "height": img_h,
        "render_time_ms": round(duration_ms, 1),
        "site_id": site_id,
        "node_ip": node_ip,
        "palette": palette_name,
        "png_b64": png_b64,
    }


def run_fractal_mode(args, dashboard_url, ray_head_ip, alive_nodes, site_metadata):
    """Run fractal tile rendering across the Ray cluster."""
    grid_size = args.grid_size
    image_size = args.image_size
    palette = args.palette
    total_tiles = grid_size * grid_size

    print(f"\nFractal Rendering: {grid_size}x{grid_size} grid, {image_size}px tiles, {palette} palette")
    print(f"Total tiles: {total_tiles}")

    # Configure dashboard for fractal mode
    post_json(f"{dashboard_url}/api/config", {
        "workload_type": "fractal",
        "grid_size": grid_size,
        "image_size": image_size,
        "total_tiles": total_tiles,
        "ray_head_ip": ray_head_ip,
        "num_nodes": len(alive_nodes),
        "total_cpus": int(sum(n["Resources"].get("CPU", 0) for n in alive_nodes)),
    })

    # Start rendering phase
    post_json(f"{dashboard_url}/api/phase", {"phase": "rendering"})

    # Submit all tile tasks
    print(f"\nSubmitting {total_tiles} tile tasks...")
    t_start = time.time()
    futures = []
    for ty in range(grid_size):
        for tx in range(grid_size):
            f = fractal_tile_task.remote(tx, ty, grid_size, image_size, image_size, palette, ray_head_ip)
            futures.append(f)

    # Collect results and POST each tile to dashboard
    print(f"Collecting results...")
    # Re-fetch site metadata now that workers have had time to register
    fresh_meta = fetch_site_metadata(dashboard_url)
    for k, v in fresh_meta.items():
        if k not in site_metadata or not site_metadata[k].get("cluster_name"):
            site_metadata[k] = v
    completed = 0
    last_meta_refresh = time.time()
    for future in futures:
        try:
            result = ray.get(future, timeout=300)
            # Periodically re-fetch metadata for late-registering workers
            if time.time() - last_meta_refresh > 5:
                fresh = fetch_site_metadata(dashboard_url)
                for k, v in fresh.items():
                    if k not in site_metadata or not site_metadata[k].get("cluster_name"):
                        site_metadata[k] = v
                last_meta_refresh = time.time()
            # Enrich with cluster metadata
            sid = result.get("site_id", "")
            if sid in site_metadata:
                meta = site_metadata[sid]
                if meta.get("cluster_name"):
                    result["cluster_name"] = meta["cluster_name"]
                if meta.get("scheduler_type"):
                    result["scheduler_type"] = meta["scheduler_type"]
            post_json(f"{dashboard_url}/api/tile", result)
            completed += 1
            if completed % 50 == 0 or completed == total_tiles:
                elapsed = time.time() - t_start
                rate = completed / elapsed if elapsed > 0 else 0
                print(f"  {completed}/{total_tiles} tiles ({rate:.1f} tiles/sec)")
        except Exception as e:
            print(f"  Warning: Tile task failed: {e}")

    t_duration = time.time() - t_start
    rate = completed / t_duration if t_duration > 0 else 0

    print(f"\n{'='*60}")
    print(f"Fractal Rendering Complete")
    print(f"{'='*60}")
    print(f"  {completed}/{total_tiles} tiles in {t_duration:.1f}s ({rate:.1f} tiles/sec)")

    post_json(f"{dashboard_url}/api/phase", {"phase": "complete"})


def fetch_site_metadata(dashboard_url):
    """Fetch site metadata (cluster_name, scheduler_type) from dashboard state."""
    try:
        req = urllib.request.Request(f"{dashboard_url}/api/state")
        resp = urllib.request.urlopen(req, timeout=10)
        data = json.loads(resp.read().decode())
        site_stats = data.get("site_stats", {})
        # Build site_id -> {cluster_name, scheduler_type} mapping
        meta = {}
        for site_id, stats in site_stats.items():
            meta[site_id] = {
                "cluster_name": stats.get("cluster_name", ""),
                "scheduler_type": stats.get("scheduler_type", ""),
            }
        return meta
    except Exception as e:
        print(f"  Warning: Could not fetch site metadata: {e}")
        return {}


def run_phase(phase_name, futures, dashboard_url, ray_head_ip, site_metadata=None):
    """Collect results from futures, POSTing each to dashboard."""
    if site_metadata is None:
        site_metadata = {}
    print(f"\n  Collecting {len(futures)} {phase_name} results...")
    completed = 0
    for future in futures:
        try:
            result = ray.get(future, timeout=120)
            # Enrich with cluster metadata from site_metadata mapping
            site_id = result.get("site_id", "")
            if site_id in site_metadata:
                meta = site_metadata[site_id]
                if meta.get("cluster_name"):
                    result["cluster_name"] = meta["cluster_name"]
                if meta.get("scheduler_type"):
                    result["scheduler_type"] = meta["scheduler_type"]
            post_json(f"{dashboard_url}/api/task", result)
            completed += 1
            if completed % 50 == 0:
                print(f"    {completed}/{len(futures)} tasks complete")
        except Exception as e:
            print(f"  Warning: Task failed: {e}")
    return completed


def main():
    parser = argparse.ArgumentParser(description="Ray distributed benchmark")
    parser.add_argument("--dashboard-url", required=True)
    parser.add_argument("--ray-head-ip", required=True)
    parser.add_argument("--workload-type", default="benchmark", choices=["benchmark", "fractal"])
    parser.add_argument("--num-tasks", type=int, default=500)
    parser.add_argument("--matrix-size", type=int, default=500)
    parser.add_argument("--grid-size", type=int, default=16)
    parser.add_argument("--image-size", type=int, default=256)
    parser.add_argument("--palette", default="electric")
    parser.add_argument("--onprem-cluster-name", default="")
    parser.add_argument("--onprem-scheduler-type", default="ssh")
    args = parser.parse_args()

    dashboard_url = args.dashboard_url.rstrip("/")
    ray_head_ip = args.ray_head_ip
    num_tasks = args.num_tasks
    matrix_size = args.matrix_size

    # Initialize Ray (connect to existing cluster)
    ray.init(address="auto")

    # Report cluster info
    nodes = ray.nodes()
    alive_nodes = [n for n in nodes if n["Alive"]]
    total_cpus = sum(n["Resources"].get("CPU", 0) for n in alive_nodes)
    print(f"\nRay cluster: {len(alive_nodes)} nodes, {total_cpus} total CPUs")
    for n in alive_nodes:
        ip = n.get("NodeManagerAddress", "unknown")
        cpus = n["Resources"].get("CPU", 0)
        print(f"  Node {ip}: {cpus} CPUs")

    # Register head node as site-1 worker
    post_json(f"{dashboard_url}/api/worker", {
        "site_id": "site-1",
        "worker_ip": ray_head_ip,
        "num_cpus": int(sum(
            n["Resources"].get("CPU", 0) for n in alive_nodes
            if n.get("NodeManagerAddress") == ray_head_ip
        )),
        "cluster_name": args.onprem_cluster_name,
        "scheduler_type": args.onprem_scheduler_type,
    })

    # Fetch site metadata from dashboard (includes worker registrations from dispatch_workers)
    # This maps site_id -> {cluster_name, scheduler_type} for ALL sites
    site_metadata = fetch_site_metadata(dashboard_url)
    # Ensure head node metadata is included
    site_metadata["site-1"] = {
        "cluster_name": args.onprem_cluster_name,
        "scheduler_type": args.onprem_scheduler_type,
    }
    print(f"\nSite metadata: {json.dumps(site_metadata, indent=2)}")

    # Branch on workload type
    if args.workload_type == "fractal":
        run_fractal_mode(args, dashboard_url, ray_head_ip, alive_nodes, site_metadata)
        ray.shutdown()
        return

    # Configure dashboard for benchmark mode
    post_json(f"{dashboard_url}/api/config", {
        "workload_type": "benchmark",
        "total_tasks": num_tasks + 20 + num_tasks,  # throughput + compute + scaling
        "ray_head_ip": ray_head_ip,
        "num_nodes": len(alive_nodes),
        "total_cpus": int(total_cpus),
    })

    overall_start = time.time()

    # =========================================================================
    # Phase 1: Task Throughput
    # =========================================================================
    print(f"\n{'='*60}")
    print(f"Phase 1: Task Throughput ({num_tasks} tasks)")
    print(f"{'='*60}")
    post_json(f"{dashboard_url}/api/phase", {"phase": "throughput"})

    t1_start = time.time()
    futures = [throughput_task.remote(i, ray_head_ip) for i in range(num_tasks)]
    # Re-fetch site metadata in case workers registered after initial fetch
    fresh_meta = fetch_site_metadata(dashboard_url)
    for k, v in fresh_meta.items():
        if k not in site_metadata or not site_metadata[k].get("cluster_name"):
            site_metadata[k] = v
    print(f"Site metadata (updated): {json.dumps(site_metadata, indent=2)}")
    completed = run_phase("throughput", futures, dashboard_url, ray_head_ip, site_metadata)
    t1_duration = time.time() - t1_start
    tasks_per_sec = completed / t1_duration if t1_duration > 0 else 0

    # Count per-site
    site_counts = {}
    for t in futures:
        try:
            r = ray.get(t, timeout=1)
            sid = r["site_id"]
            site_counts[sid] = site_counts.get(sid, 0) + 1
        except:
            pass

    throughput_data = {
        "tasks_per_sec": round(tasks_per_sec, 1),
        "total_tasks": completed,
        "duration_s": round(t1_duration, 2),
        "site_breakdown": site_counts,
    }
    post_json(f"{dashboard_url}/api/results", {"type": "throughput", "data": throughput_data})
    print(f"  Throughput: {tasks_per_sec:.1f} tasks/sec ({completed} tasks in {t1_duration:.1f}s)")

    # =========================================================================
    # Phase 2: CPU Compute
    # =========================================================================
    num_compute = min(20, len(alive_nodes) * 4)
    print(f"\n{'='*60}")
    print(f"Phase 2: CPU Compute ({num_compute} tasks, {matrix_size}x{matrix_size} matrices)")
    print(f"{'='*60}")
    post_json(f"{dashboard_url}/api/phase", {"phase": "compute"})

    futures = [compute_task.remote(num_tasks + i, matrix_size, ray_head_ip) for i in range(num_compute)]
    run_phase("compute", futures, dashboard_url, ray_head_ip, site_metadata)

    # Collect compute results
    for f in futures:
        try:
            r = ray.get(f, timeout=1)
            post_json(f"{dashboard_url}/api/results", {
                "type": "compute",
                "data": {
                    "node_ip": r["node_ip"],
                    "site_id": r["site_id"],
                    "gflops": r["result"]["gflops"],
                    "matrix_size": matrix_size,
                },
            })
        except:
            pass

    # =========================================================================
    # Phase 3: Scaling Test
    # =========================================================================
    print(f"\n{'='*60}")
    print(f"Phase 3: Scaling Test ({num_tasks} tasks)")
    print(f"{'='*60}")
    post_json(f"{dashboard_url}/api/phase", {"phase": "scaling"})

    t3_start = time.time()
    futures = [scaling_task.remote(num_tasks + num_compute + i, ray_head_ip) for i in range(num_tasks)]
    completed = run_phase("scaling", futures, dashboard_url, ray_head_ip, site_metadata)
    t3_duration = time.time() - t3_start
    dual_site_tps = completed / t3_duration if t3_duration > 0 else 0

    # Estimate single-site throughput from the faster site
    site_times = {}
    for t in futures:
        try:
            r = ray.get(t, timeout=1)
            sid = r["site_id"]
            if sid not in site_times:
                site_times[sid] = {"count": 0, "total_ms": 0}
            site_times[sid]["count"] += 1
            site_times[sid]["total_ms"] += r["duration_ms"]
        except:
            pass

    single_site_estimate = 0
    if site_times:
        # Estimate: if all tasks ran on the faster site
        fastest = max(site_times.values(), key=lambda x: x["count"] / (x["total_ms"] / 1000) if x["total_ms"] > 0 else 0)
        single_site_tps = fastest["count"] / (fastest["total_ms"] / 1000) if fastest["total_ms"] > 0 else 0
        single_site_estimate = single_site_tps

    speedup = dual_site_tps / single_site_estimate if single_site_estimate > 0 else 1.0

    scaling_data = {
        "single_site_tps": round(single_site_estimate, 1),
        "dual_site_tps": round(dual_site_tps, 1),
        "speedup": round(speedup, 2),
        "duration_s": round(t3_duration, 2),
    }
    post_json(f"{dashboard_url}/api/results", {"type": "scaling", "data": scaling_data})
    print(f"  Scaling: {speedup:.2f}x speedup ({dual_site_tps:.1f} vs {single_site_estimate:.1f} tasks/sec)")

    # =========================================================================
    # Complete
    # =========================================================================
    total_duration = time.time() - overall_start
    print(f"\n{'='*60}")
    print(f"Benchmark Complete ({total_duration:.1f}s)")
    print(f"{'='*60}")
    post_json(f"{dashboard_url}/api/phase", {"phase": "complete"})

    ray.shutdown()


if __name__ == "__main__":
    main()
