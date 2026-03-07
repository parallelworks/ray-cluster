# Multi-Site Ray Cluster

Unified compute fabric across ACTIVATE resources using [Ray](https://ray.io). Deploys a Ray head node on any resource, connects workers from one or more additional sites via SSH tunnels, and provides a live dashboard showing cluster topology and task placement.

## Architecture

```
                ACTIVATE Workflow
                      |
           +----------+----------+------------ ...
           v          v          v
        Site 1      Site 2     Site N
       Ray HEAD    Ray WORKER  Ray WORKER
       Dashboard   (SSH/SLURM) (SSH/SLURM)
           |          |          |
           +--- Ray Cluster -----+
                      |
              Workload Options:
              - Fractal rendering (visual)
              - Mathematical benchmark (charts)
              - Cluster only (bring your own workload)
```

## Quick Start

1. Add this workflow to your ACTIVATE account
2. Configure:
   - **Head Node**: Select any resource — runs the Ray coordinator + dashboard (no compute)
   - **Compute Workers**: Add one or more worker sites (SSH, SLURM, or PBS resources)
   - **Workload**: Choose fractal rendering, benchmark, or cluster-only mode
3. Click **Execute**
4. Open the session link to view the live dashboard

## Workload Modes

### Fractal Rendering
Distributes Mandelbrot tile rendering across all sites. Tiles appear live on a canvas, color-coded by which site rendered them.

### Mathematical Benchmark
Three phases:
1. **Task Throughput** — Bursts many small tasks to measure scheduling rate and placement distribution
2. **CPU Compute** — Matrix multiplications (NumPy) measuring GFLOPS per node
3. **Scaling Test** — Compares multi-site vs single-site throughput

### Cluster Only
Deploys the Ray cluster with no demo workload. The dashboard shows connection instructions with copy-paste commands for SSH tunnels, Ray job submission, and direct head node access. Use this mode to run your own Ray jobs, training scripts, or interactive workloads.

## Worker Dispatch

Each worker node registers **1 task slot** with Ray. Ray handles placement across nodes; tasks use internal parallelism (OpenMP, MPI, PyTorch, etc.) for multi-core/GPU work within each node.

Worker dispatch modes:
- **SSH**: Direct connection to the remote host (single node per site)
- **SLURM**: Submit via `srun` with configurable partition, account, QoS, nodes, and walltime
- **PBS**: Submit via `qsub` with configurable directives

Cross-site workers connect through SSH tunnels with unique loopback IPs (`127.0.X.Y`) for multi-node support.

## Dashboard

- **Cluster tab** — Node topology grouped by site, task placement bar chart, throughput over time
- **Connect tab** — SSH tunnel commands, Python examples, Ray Jobs CLI, cluster info table (cluster_only mode)
- **Ray Dashboard tab** — Proxied Ray native dashboard (port 8265)

## Files

```
ray-cluster-demo/
├── workflow.yaml              # Multi-site workflow definition
├── README.md
├── ROADMAP.md                 # Future improvements and priorities
├── thumbnail.png
└── scripts/
    ├── setup.sh               # Install Ray + NumPy via uv/pip (handles old Python)
    ├── start_ray_head.sh      # Start Ray head (--num-cpus=0) + dashboard
    ├── dispatch_workers.sh    # Connect workers from all sites (SSH/SLURM/PBS)
    ├── run_benchmark.sh       # Run benchmark, POST results to dashboard
    ├── benchmark.py           # Ray distributed benchmark + fractal tasks
    ├── dashboard.py           # FastAPI live dashboard server (WebSocket)
    ├── diagnose.sh            # Cluster diagnostic tool
    ├── diagnose_cluster.py    # Ray cluster health checker
    └── templates/
        └── index.html         # Dashboard UI
```
