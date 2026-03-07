# Roadmap

Prioritized improvements for the multi-site Ray cluster workflow.

## In Progress

- **PBS worker dispatch** — Implement `qsub`/`pbsdsh` dispatch in `dispatch_workers.sh`
- **Custom user script execution** — Script editor in form, runs on head node after cluster setup
- **GPU support** — Detect GPUs, pass `--num-gpus` to Ray, show in dashboard, GPU benchmark

## Reliability

- **Tunnel health monitoring** — Detect SSH drops, log clearly, attempt reconnection
- **Worker cleanup on workflow completion** — Signal workers to shut down gracefully
- **Dashboard state persistence** — Write state to disk for long-running sessions

## UX & Examples

- **Ray Train / Ray Serve examples** — Code examples in Connect tab
- **Pre-built script templates** — Dropdown of common workloads (PyTorch DDP, Ray Serve, MPI, GPU benchmark) that pre-fill the custom script editor
- **Data staging guidance** — Document data movement options (cloud buckets, NFS, Ray object store)

## Demo & Polish

- **Fault tolerance demo** — Dashboard button to kill a worker, show Ray redistributing tasks
- **Fix scaling benchmark methodology** — Current speedup metric overestimates due to tunnel overhead

## Strategic

- **Overlay network option (Tailscale/WireGuard)** — Replace SSH tunnels for large-scale deployments
- **Standalone mode** — Decouple from ACTIVATE `pw` CLI for broader adoption
