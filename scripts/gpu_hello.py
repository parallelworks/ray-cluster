#!/usr/bin/env python3
"""GPU hello world for Ray clusters.

Detects all GPUs in the cluster and queries device info on each worker
via nvidia-smi (no extra packages required). If torch is available
(add 'torch' to Extra pip Packages in the workflow form), also runs a
matrix multiply benchmark and reports TFLOPS.

Usage:
    ray job submit --address http://127.0.0.1:8265 --working-dir . -- python scripts/gpu_hello.py
"""

import ray
import socket
import time


ray.init()

resources = ray.cluster_resources()
total_gpus = int(resources.get("GPU", 0))
nodes = ray.nodes()

print(f"Cluster: {len(nodes)} node(s), {total_gpus} GPU(s)")
print(f"Resources: { {k: v for k, v in sorted(resources.items())} }")
print()

if total_gpus == 0:
    print("No GPUs available in this cluster.")
    print("To add GPUs, set 'GPUs per Node (gres)' in the workflow form.")
    ray.shutdown()
    exit(0)


@ray.remote(num_gpus=1)
def gpu_hello(task_id):
    """Run on a single GPU and report what we find."""
    import os
    import subprocess

    host = socket.gethostname()
    gpu_ids = os.environ.get("CUDA_VISIBLE_DEVICES", "none")
    result = {"task_id": task_id, "host": host, "gpu_ids": gpu_ids}

    # --- Try nvidia-smi (always available with NVIDIA drivers) ---
    try:
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=name,memory.total,driver_version",
             "--format=csv,noheader,nounits",
             f"--id={gpu_ids.split(',')[0]}"],
            text=True, timeout=10,
        ).strip()
        name, mem_mb, driver = [x.strip() for x in out.split(",")]
        result["gpu_name"] = name
        result["gpu_mem_gb"] = round(float(mem_mb) / 1024, 1)
        result["driver"] = driver
        result["status"] = "ok"
    except Exception as e:
        result["status"] = f"nvidia-smi failed: {e}"
        return result

    # --- Optional: torch benchmark ---
    try:
        import torch
        if torch.cuda.is_available():
            size = 4096
            a = torch.randn(size, size, device="cuda")
            b = torch.randn(size, size, device="cuda")
            torch.cuda.synchronize()

            t0 = time.perf_counter()
            torch.mm(a, b)
            torch.cuda.synchronize()
            elapsed = time.perf_counter() - t0

            tflops = 2 * size**3 / elapsed / 1e12
            result["matmul_ms"] = round(elapsed * 1000, 1)
            result["tflops"] = round(tflops, 2)
            result["cuda_version"] = torch.version.cuda
    except ImportError:
        pass  # torch not required

    return result


print(f"Submitting {total_gpus} GPU task(s)...")
futures = [gpu_hello.remote(i) for i in range(total_gpus)]
results = ray.get(futures)

print()
header = f"{'Task':<6} {'Host':<20} {'GPU':<6} {'Device':<28} {'Mem':>7} {'Driver':>10}"
has_bench = any("tflops" in r for r in results)
if has_bench:
    header += f"  {'Matmul':>9} {'TFLOPS':>7}"
header += "  Status"
print(header)
print("-" * len(header))

for r in results:
    line = f"{r['task_id']:<6} {r['host']:<20} {r['gpu_ids']:<6} {r.get('gpu_name', '-'):<28} "
    line += f"{f'{r[\"gpu_mem_gb\"]} GB' if 'gpu_mem_gb' in r else '-':>7} "
    line += f"{r.get('driver', '-'):>10}"
    if has_bench:
        matmul = f"{r['matmul_ms']} ms" if "matmul_ms" in r else "-"
        tflops = str(r.get("tflops", "-"))
        line += f"  {matmul:>9} {tflops:>7}"
    line += f"  {r['status']}"
    print(line)

print()
if has_bench:
    print(f"All {total_gpus} GPU(s) verified with torch matmul benchmark.")
else:
    print(f"All {total_gpus} GPU(s) detected. Install torch for matmul benchmark.")

ray.shutdown()
