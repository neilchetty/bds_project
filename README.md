# WSH: Workflow Scheduling for Heterogeneous Computing

Real-world implementation of the WSH algorithm from the paper  
**"Scheduling of Big Data Workflows in the Hadoop Framework with Heterogeneous Computing Cluster"**

This project schedules and executes real workloads on Docker containers, comparing WSH against the HEFT baseline algorithm.

---

## Platform & Hardware

> **Windows only.** Designed for Windows 10/11 with Docker Desktop.

| Spec | Value |
|------|-------|
| Machine | DESKTOP-722HBTB |
| Processor | Intel® Xeon® w5-2565X (24 cores, 3.19 GHz) |
| RAM | 64 GB (63.6 GB usable) |
| OS | Windows 64-bit |

---

## Architecture

All 28 Docker worker containers run on the **single local machine** (single-machine mode), organized into 4 heterogeneous tiers:

```
Main PC (Intel Xeon w5-2565X, 64 GB RAM)
┌────────────────────────────────────────────────────────────────┐
│ Java Scheduler (host process)                                  │
│                                                                │
│ C1 tier (7 nodes): worker-c1-1..c1-7  │ 1.5 CPU, 2048 MB each │
│ C2 tier (7 nodes): worker-c2-1..c2-7  │ 1.0 CPU, 1536 MB each │
│ C3 tier (7 nodes): worker-c3-1..c3-7  │ 0.5 CPU, 1024 MB each │
│ C4 tier (7 nodes): worker-c4-1..c4-7  │ 0.25 CPU, 512 MB each │
│                                                                │
│ Total: 28 containers, ~22.75 CPU cores, ~35 GB RAM             │
└────────────────────────────────────────────────────────────────┘
```

### Scheduler Node Tiers (Abstract Performance Weights)

| Tier | Nodes | cpu_factor | io_factor | ram_mb | Paper Equivalent |
|------|-------|-----------|-----------|--------|------------------|
| C1 (high-end) | 7 | 4.0 | 2.5 | 4096 | 4 socket × 2 core VM |
| C2 (mid-range) | 7 | 2.0 | 1.5 | 2048 | 2 socket × 2 core VM |
| C3 (baseline) | 7 | 1.0 | 1.0 | 1024 | 1 socket × 2 core VM |
| C4 (low-end) | 7 | 0.5 | 0.6 | 512 | 1 socket × 1 core VM |

> **Note:** The paper uses 12 nodes (3 per tier). We use 28 nodes (7 per tier) to leverage the Xeon hardware for more parallelism while keeping the same heterogeneous tier ratios.

---

## Prerequisites

| Software | Version | Download |
|----------|---------|----------|
| Docker Desktop | Latest | https://www.docker.com/products/docker-desktop/ |
| JDK | 21 or later | https://adoptium.net/ |

---

## Setup Instructions

### Step 1: Install Docker Desktop

1. Install Docker Desktop
2. Open Docker Desktop → **Settings** → **General**
3. ✅ Check **"Expose daemon on tcp://localhost:2375 without TLS"**
4. Click **Apply & Restart**
5. Verify Docker works:
   ```powershell
   docker run hello-world
   ```

### Step 2: Start all containers (single-machine mode)

```powershell
cd C:\Users\CSE_SDPL\Desktop\bds_project
docker compose --profile single-machine up -d
```

This starts all 28 worker containers (C1–C4 tiers) on this machine.

### Step 3: Verify containers are running

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```

You should see 28 containers: `worker-c1-1` through `worker-c4-7`.

### Step 4: Build the project

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\build.ps1
```

### Step 5: Run quick test

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run-quick-test.ps1
```

### Step 6: Run full benchmark

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run-real-benchmark.ps1
```

This runs HEFT vs WSH across node counts `4,7,10,13,16,20,24,28` for all three workflows.

---

## CLI Commands

### `execute` — Run a single workflow on real containers
```powershell
java -jar build\wsh-scheduler.jar execute `
    --workflow Gene2life `
    --algorithm WSH `
    --nodes-file config\cluster\local-docker-nodes.csv `
    --output results\real-execution.csv
```

### `real-benchmark` — Full HEFT vs WSH comparison
```powershell
java -jar build\wsh-scheduler.jar real-benchmark `
    --node-counts 4,7,10,13,16,20,24,28 `
    --output results\real-metrics.csv `
    --details-dir results\real-executions
```

### `benchmark` — Simulated comparison (no Docker needed)
```powershell
java -jar build\wsh-scheduler.jar benchmark `
    --node-counts 4,7,10,13,16,20,24,28 `
    --output results\metrics.csv `
    --schedules-dir results\schedules
```

### `schedule` — Generate a single schedule
```powershell
java -jar build\wsh-scheduler.jar schedule `
    --workflow Gene2life --algorithm WSH `
    --node-count 28 --output results\single-schedule.csv
```

### `verify` — Validate metrics file
```powershell
java -jar build\wsh-scheduler.jar verify --input results\metrics.csv
```

---

## Output Files

| File | Description |
|------|-------------|
| `results/real-metrics.csv` | Comparison: simulated vs real makespans, WSH improvement % |
| `results/real-executions/` | Per-task execution details (timestamps, durations) |
| `results/metrics.csv` | Simulated-only metrics |
| `results/schedules/` | Per-workflow schedule CSVs |

---

## Workflows

| Workflow | Tasks | Description | Paper Source |
|----------|-------|-------------|-------------|
| Gene2life | 8 | Genomic analysis: blast → clustalw → dnapars/protpars → drawgram | Table 6 |
| Avianflu_small | 104 | Molecular docking: prepare → autogrid → 102× autodock | Section 5.2 |
| Epigenomics | 100 | Epigenomic analysis: layered DAG pipeline | Section 5.2 |

### Gene2life Task Costs (matching paper)

| Task | Workload (seconds) | Description |
|------|-------------------|-------------|
| Blast1, Blast2 | 180 | Sequence similarity search |
| Clustalw1, Clustalw2 | 300 | Multiple sequence alignment |
| Dnapars | 30 | DNA parsimony analysis |
| Protpars | 30 | Protein parsimony analysis |
| Drawgram1, Drawgram2 | 30 | Phylogenetic tree rendering |

Custom DAX workflow files can be loaded with `--workflow-file path\to\workflow.xml`.

---

## How It Works

1. **Schedule**: HEFT or WSH algorithm assigns tasks to nodes based on upward rank prioritization and cluster ordering (communication costs are ignored per the paper — data sizes are negligible at 1 Gbps)
2. **Execute**: Tasks are dispatched to Docker containers via `docker exec`:
   - CPU work: `dd if=/dev/urandom | sha256sum` (parallel workers)
   - IO work: `dd write` + `dd read` cycles
3. **Transfer**: Data between tasks on different containers is piped via `docker exec cat | docker exec cat >`
4. **Measure**: Actual wall-clock timestamps recorded for every task
5. **Compare**: WSH improvement over HEFT computed from real execution times

### Metrics (per paper)

| Metric | Formula | Description |
|--------|---------|-------------|
| **SLR** | Makespan / CriticalPath(fastest nodes) | Schedule Length Ratio (lower is better) |
| **Speedup** | SequentialTime(slowest node) / Makespan | Parallel speedup (higher is better) |

---

## Node Configuration Files

| File | Nodes | Distribution |
|------|-------|-------------|
| `nodes-4.csv` | 4 | 1 per tier |
| `nodes-7.csv` | 7 | 2 C1 + 2 C2 + 2 C3 + 1 C4 |
| `nodes-10.csv` | 10 | 3 C1 + 2 C2 + 3 C3 + 2 C4 |
| `nodes-13.csv` | 13 | 4 C1 + 3 C2 + 3 C3 + 3 C4 |
| `nodes-16.csv` | 16 | 4 per tier |
| `nodes-20.csv` | 20 | 5 per tier |
| `nodes-24.csv` | 24 | 6 per tier |
| `nodes-28.csv` | 28 | 7 per tier |
| `local-docker-nodes.csv` | 28 | Full cluster (all local) |

---

## Project Structure

```
bds_project/
├── config/cluster/          # Node configuration CSVs
├── docker-compose.yml       # All containers (C1 local + C2-C4 single-machine profile)
├── docker-compose-worker.yml # Remote PC container template
├── scripts/
│   ├── build.ps1            # Compile and package JAR
│   ├── run-tests.ps1        # Run unit tests
│   ├── run-quick-test.ps1   # Quick smoke test
│   └── run-real-benchmark.ps1 # Full benchmark (8 node counts)
├── src/main/java/org/bds/wsh/
│   ├── cli/                 # CLI commands and benchmark runner
│   ├── config/              # Cluster configuration factory
│   ├── execution/           # Real execution engine (Docker)
│   ├── io/                  # CSV/DAX file I/O
│   ├── metrics/             # Metric calculation (SLR, Speedup)
│   ├── model/               # Core data models (Task, Node, Workflow)
│   ├── scheduler/           # HEFT and WSH scheduling algorithms
│   └── workflow/            # Built-in workflow definitions
├── src/test/                # Unit tests
└── Main-Paper.pdf           # Reference paper
```

---

## Expected Results

WSH should outperform HEFT, especially with larger heterogeneous clusters:

| Nodes | Expected WSH Improvement |
|-------|-------------------------|
| 4     | 2-5% |
| 7     | 5-10% |
| 10    | 8-15% |
| 13    | 10-20% |
| 16    | 12-22% |
| 20    | 15-25% |
| 24    | 15-25% |
| 28    | 15-28% |

These ranges are consistent with the paper's findings. Actual results depend on container load and system conditions.
