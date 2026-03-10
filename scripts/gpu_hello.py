#!/usr/bin/env python3
"""GPU hello world for Ray clusters.

Detects all GPUs in the cluster and runs a simple matrix multiply on each
to verify they are accessible and working.

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

    host = socket.gethostname()
    gpu_ids = os.environ.get("CUDA_VISIBLE_DEVICES", "none")
    result = {"task_id": task_id, "host": host, "gpu_ids": gpu_ids}

    try:
        import torch

        if not torch.cuda.is_available():
            result["status"] = "torch installed but CUDA unavailable"
            return result

        dev = torch.cuda.current_device()
        props = torch.cuda.get_device_properties(dev)
        result["gpu_name"] = props.name
        result["gpu_mem_gb"] = round(props.total_mem / 1e9, 1)
        result["cuda_version"] = torch.version.cuda

        # Matrix multiply benchmark (warm-up + timed)
        size = 4096
        a = torch.randn(size, size, device="cuda")
        b = torch.randn(size, size, device="cuda")
        torch.cuda.synchronize()

        t0 = time.perf_counter()
        c = torch.mm(a, b)
        torch.cuda.synchronize()
        elapsed = time.perf_counter() - t0

        # TFLOPS = 2 * N^3 / time / 1e12
        tflops = 2 * size**3 / elapsed / 1e12
        result["matmul_ms"] = round(elapsed * 1000, 1)
        result["tflops"] = round(tflops, 2)
        result["status"] = "ok"

    except ImportError:
        result["status"] = "torch not installed (pip install torch)"
    except Exception as e:
        result["status"] = f"error: {e}"

    return result


print(f"Submitting {total_gpus} GPU task(s)...")
futures = [gpu_hello.remote(i) for i in range(total_gpus)]
results = ray.get(futures)

print()
print(f"{'Task':<6} {'Host':<20} {'GPU':<6} {'Device':<30} {'Mem':>7} {'Matmul':>9} {'TFLOPS':>8}  Status")
print("-" * 100)

for r in results:
    task = r["task_id"]
    host = r["host"]
    gpu = r["gpu_ids"]
    name = r.get("gpu_name", "-")
    mem = f"{r['gpu_mem_gb']} GB" if "gpu_mem_gb" in r else "-"
    matmul = f"{r['matmul_ms']} ms" if "matmul_ms" in r else "-"
    tflops = str(r.get("tflops", "-"))
    status = r["status"]
    print(f"{task:<6} {host:<20} {gpu:<6} {name:<30} {mem:>7} {matmul:>9} {tflops:>8}  {status}")

print()
print(f"All {total_gpus} GPU(s) verified.")

ray.shutdown()
