#!/usr/bin/env python3
"""Diagnose Ray cluster connectivity and task scheduling.

Run on the Ray head node to check:
1. Node visibility and resource allocation
2. Task placement across nodes
3. Network reachability between nodes
4. Ray internal port connectivity
"""

import argparse
import json
import socket
import time

import ray


def check_nodes():
    """Report all nodes, their IPs, resources, and status."""
    nodes = ray.nodes()
    print(f"\n{'='*60}")
    print(f"RAY CLUSTER NODES ({len(nodes)} total)")
    print(f"{'='*60}")

    alive = 0
    for n in nodes:
        status = "ALIVE" if n["Alive"] else "DEAD"
        ip = n.get("NodeManagerAddress", "unknown")
        node_id = n.get("NodeID", "?")[:12]
        cpus = n["Resources"].get("CPU", 0)
        gpus = n["Resources"].get("GPU", 0)

        # Get raylet port and object store port
        raylet_port = n.get("NodeManagerPort", "?")
        obj_store = n.get("ObjectStoreSocketName", "?")

        print(f"\n  Node {node_id}... [{status}]")
        print(f"    IP:           {ip}")
        print(f"    CPUs:         {cpus}")
        print(f"    GPUs:         {gpus}")
        print(f"    Raylet port:  {raylet_port}")
        print(f"    Obj store:    {obj_store}")

        if n["Alive"]:
            alive += 1
            # Test TCP connectivity to the node's raylet port
            test_connectivity(ip, raylet_port)

    print(f"\n  Summary: {alive} alive, {len(nodes) - alive} dead")
    return nodes


def test_connectivity(ip, port):
    """Test if we can reach a node's raylet port."""
    if port == "?" or not port:
        print(f"    Connectivity: SKIP (no port info)")
        return
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect((ip, int(port)))
        s.close()
        print(f"    Connectivity: OK (reached {ip}:{port})")
    except Exception as e:
        print(f"    Connectivity: FAILED ({ip}:{port} - {e})")


@ray.remote
def get_node_info():
    """Remote task that reports which node it runs on."""
    import os
    ip = ray.util.get_node_ip_address()
    hostname = socket.gethostname()
    pid = os.getpid()
    return {
        "ip": ip,
        "hostname": hostname,
        "pid": pid,
    }


@ray.remote
def heavy_task(task_id):
    """A task heavy enough to force scheduling across nodes."""
    import numpy as np
    start = time.time()
    time.sleep(0.1)  # Ensure minimum duration
    a = np.random.randn(300, 300)
    for _ in range(3):
        a = a @ np.random.randn(300, 300)
    duration = time.time() - start
    ip = ray.util.get_node_ip_address()
    return {"task_id": task_id, "ip": ip, "duration_ms": round(duration * 1000, 1)}


@ray.remote(scheduling_strategy="SPREAD")
def spread_task(task_id):
    """Same as heavy_task but with SPREAD scheduling."""
    import numpy as np
    start = time.time()
    time.sleep(0.1)
    a = np.random.randn(300, 300)
    for _ in range(3):
        a = a @ np.random.randn(300, 300)
    duration = time.time() - start
    ip = ray.util.get_node_ip_address()
    return {"task_id": task_id, "ip": ip, "duration_ms": round(duration * 1000, 1)}


@ray.remote(num_cpus=1)
def ping_task():
    """Minimal task — just report node IP."""
    return ray.util.get_node_ip_address()


def test_placement(label, task_fn, count=20):
    """Submit tasks and report placement distribution."""
    print(f"\n{'='*60}")
    print(f"TEST: {label} ({count} tasks)")
    print(f"{'='*60}")

    futures = [task_fn.remote(i) for i in range(count)]

    ip_counts = {}
    for i, f in enumerate(futures):
        try:
            result = ray.get(f, timeout=30)
            if isinstance(result, dict):
                ip = result["ip"]
            else:
                ip = result
            ip_counts[ip] = ip_counts.get(ip, 0) + 1
        except Exception as e:
            print(f"  Task {i} FAILED: {e}")
            ip_counts["FAILED"] = ip_counts.get("FAILED", 0) + 1

    print(f"\n  Placement results:")
    for ip, count in sorted(ip_counts.items(), key=lambda x: -x[1]):
        print(f"    {ip}: {count} tasks")

    return ip_counts


def test_node_affinity(nodes):
    """Submit tasks with explicit node affinity to each alive node."""
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    alive_nodes = [n for n in nodes if n["Alive"]]
    print(f"\n{'='*60}")
    print(f"TEST: Explicit Node Affinity ({len(alive_nodes)} nodes)")
    print(f"{'='*60}")

    for n in alive_nodes:
        node_id = n["NodeID"]
        ip = n.get("NodeManagerAddress", "unknown")
        cpus = n["Resources"].get("CPU", 0)

        print(f"\n  Targeting node {node_id[:12]}... (IP: {ip}, CPUs: {cpus})")

        @ray.remote(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=node_id, soft=False
            )
        )
        def affinity_task():
            return {
                "ip": ray.util.get_node_ip_address(),
                "hostname": socket.gethostname(),
            }

        try:
            result = ray.get(affinity_task.remote(), timeout=15)
            print(f"    Result: ran on {result['ip']} ({result['hostname']})")
        except Exception as e:
            print(f"    FAILED: {e}")


def test_ping_all_nodes(nodes):
    """Flood ping tasks to see if any reach remote nodes."""
    print(f"\n{'='*60}")
    print(f"TEST: Ping flood (100 minimal tasks)")
    print(f"{'='*60}")

    futures = [ping_task.remote() for _ in range(100)]
    ip_counts = {}
    failed = 0
    for f in futures:
        try:
            ip = ray.get(f, timeout=10)
            ip_counts[ip] = ip_counts.get(ip, 0) + 1
        except:
            failed += 1

    print(f"  Results:")
    for ip, c in sorted(ip_counts.items(), key=lambda x: -x[1]):
        print(f"    {ip}: {c} tasks")
    if failed:
        print(f"    FAILED: {failed} tasks")


def main():
    parser = argparse.ArgumentParser(description="Diagnose Ray cluster")
    parser.add_argument("--num-tasks", type=int, default=20)
    args = parser.parse_args()

    ray.init(address="auto")

    print("Connected to Ray cluster")
    print(f"  Dashboard: {ray.get_dashboard_url()}")

    # 1. Check nodes
    nodes = check_nodes()

    alive_nodes = [n for n in nodes if n["Alive"]]
    if len(alive_nodes) < 2:
        print("\n[WARN] Only 1 alive node — task distribution tests will be trivial")

    # 2. Test basic ping placement
    test_ping_all_nodes(nodes)

    # 3. Test default scheduling
    test_placement("Default scheduling (heavy tasks)", heavy_task, args.num_tasks)

    # 4. Test SPREAD scheduling
    test_placement("SPREAD scheduling (heavy tasks)", spread_task, args.num_tasks)

    # 5. Test explicit node affinity
    test_node_affinity(nodes)

    print(f"\n{'='*60}")
    print("DIAGNOSIS COMPLETE")
    print(f"{'='*60}")

    ray.shutdown()


if __name__ == "__main__":
    main()
