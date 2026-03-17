# WSH: Workflow Scheduling for Heterogeneous Computing

Real-world implementation of the WSH algorithm from the paper  
**"Scheduling of Big Data Workflows in the Hadoop Framework with Heterogeneous Computing Cluster"**

This project schedules and executes real workloads on Docker containers distributed across multiple Windows machines, comparing WSH against the HEFT baseline algorithm.

---

## Platform Support

> **Windows only.** This project is designed for Windows 10/11 with Docker Desktop. Other platforms are not supported.

---

## Architecture

```
Main PC (this machine)        PC2 (remote)              PC3 (remote)              PC4 (remote)
┌──────────────────────┐   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│ Java Scheduler (host)│   │                  │   │                  │   │                  │
│ worker-c1-1..c1-4    │   │ worker-c2-1..c2-3│   │ worker-c3-1..c3-3│   │ worker-c4-1..c4-3│
│ C1 tier (4 CPU, 4GB) │   │ C2 tier (2 CPU)  │   │ C3 tier (1 CPU)  │   │ C4 tier (512MB)  │
└────────┬─────────────┘   └────────┬─────────┘   └────────┬─────────┘   └────────┬─────────┘
         └──────────────── 1 Gbps College LAN (172.16.x.x/23) ─┴──────────────────┘
```

The Java scheduler runs on the main PC. It dispatches real CPU and IO workloads to Docker containers via `docker exec` (local or remote), transfers data between containers over the network, and measures actual wall-clock execution times.

---

## Prerequisites

Install on **every Windows machine** (main + 3 remote):

| Software | Version | Download |
|----------|---------|----------|
| Docker Desktop | Latest | https://www.docker.com/products/docker-desktop/ |

Install on **main machine only** (this PC):

| Software | Version | Download |
|----------|---------|----------|
| JDK | 21 or later | https://adoptium.net/ |

---

## Setup Instructions

### Step 1: Configure Docker on ALL machines

On **every machine** (main + 3 remote PCs):

1. Install Docker Desktop
2. Open Docker Desktop → **Settings** → **General**
3. ✅ Check **"Expose daemon on tcp://localhost:2375 without TLS"**
4. Click **Apply & Restart**
5. Verify Docker is working:
   ```powershell
   docker run hello-world
   ```

### Step 2: Allow Docker TCP access through Windows Firewall (remote machines only)

On **each remote PC** (PC2, PC3, PC4), open PowerShell as Administrator:

```powershell
New-NetFirewallRule -DisplayName "Docker API" -Direction Inbound -Protocol TCP -LocalPort 2375 -Action Allow
```

### Step 3: Start containers on each machine

**Main PC:**
```powershell
cd C:\Users\CSE_SDPL\Desktop\bds_project
docker compose up -d
```
This starts C1 tier workers (4 containers).

**Remote PCs:**

Copy the file `docker-compose-worker.yml` to each remote machine. Then edit it:

- **PC2:** Uncomment the C2 tier section (already uncommented by default)
- **PC3:** Uncomment the C3 tier section, comment out C2
- **PC4:** Uncomment the C4 tier section, comment out C2

Then on each remote PC:
```powershell
docker compose -f docker-compose-worker.yml up -d
```

### Step 4: Find IP addresses

On each remote PC, run:
```powershell
ipconfig
```
Note the `IPv4 Address` under the Ethernet adapter (should be `172.16.x.x`).

### Step 5: Configure node IPs

On the **main PC**, edit `config\cluster\local-docker-nodes.csv`:

Replace the placeholder IPs:
```
REPLACE_PC2_IP  →  172.16.x.x  (PC2's IP)
REPLACE_PC3_IP  →  172.16.x.x  (PC3's IP)
REPLACE_PC4_IP  →  172.16.x.x  (PC4's IP)
```

Also update the same IPs in `nodes-4.csv`, `nodes-7.csv`, and `nodes-10.csv`.

### Step 6: Verify connectivity

From the main PC, test that you can reach each remote Docker:
```powershell
docker -H tcp://172.16.x.x:2375 ps
```
You should see the worker containers running on each remote machine.

### Step 7: Build the project (main PC only)

```powershell
cd C:\Users\CSE_SDPL\Desktop\bds_project
powershell -ExecutionPolicy Bypass -File .\scripts\build.ps1
```

### Step 8: Run quick test

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run-quick-test.ps1
```

### Step 9: Run full benchmark

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run-real-benchmark.ps1
```

---

## Single-Machine Mode (testing without remote PCs)

If you want to test on just this PC with all tiers:

```powershell
docker compose --profile single-machine up -d
```

Then use the single-machine node config (without `docker_host`):
```powershell
java -jar build\wsh-scheduler.jar execute --workflow Gene2life --algorithm WSH --output results\test.csv
```

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
    --node-counts 4,7,10,13 `
    --output results\real-metrics.csv `
    --details-dir results\real-executions
```

### `benchmark` — Simulated comparison (no Docker needed)
```powershell
java -jar build\wsh-scheduler.jar benchmark `
    --node-counts 4,7,10,13 `
    --output results\metrics.csv `
    --schedules-dir results\schedules
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

| Workflow | Tasks | Description |
|----------|-------|-------------|
| Gene2life | 8 | Genomic analysis pipeline |
| Avianflu_small | 104 | Molecular docking pipeline |
| Epigenomics | 100 | Epigenomic analysis pipeline |

Custom DAX workflow files can be loaded with `--workflow-file path\to\workflow.xml`.

---

## How It Works

1. **Schedule**: HEFT or WSH algorithm assigns tasks to nodes based on upward rank prioritization and cluster ordering
2. **Execute**: Tasks are dispatched to Docker containers via `docker exec`:
   - CPU work: `dd if=/dev/urandom | sha256sum` (parallel workers)
   - IO work: `dd write` + `dd read` cycles
3. **Transfer**: Data between tasks on different containers is piped via `docker exec cat | docker exec cat >`
4. **Measure**: Actual wall-clock timestamps recorded for every task
5. **Compare**: WSH improvement over HEFT computed from real execution times

---

## Project Structure

```
bds_project/
├── config/cluster/          # Node configuration CSVs
├── docker-compose.yml       # Main PC containers (C1 tier)
├── docker-compose-worker.yml # Remote PC container template
├── scripts/
│   ├── build.ps1            # Compile and package JAR
│   ├── run-tests.ps1        # Run unit tests
│   ├── run-quick-test.ps1   # Quick smoke test
│   └── run-real-benchmark.ps1 # Full benchmark
├── src/main/java/org/bds/wsh/
│   ├── cli/                 # CLI commands and benchmark runner
│   ├── config/              # Cluster configuration factory
│   ├── execution/           # Real execution engine (Docker)
│   ├── io/                  # CSV/DAX file I/O
│   ├── metrics/             # Metric calculation
│   ├── model/               # Core data models (Task, Node, Workflow)
│   ├── scheduler/           # HEFT and WSH scheduling algorithms
│   └── workflow/            # Built-in workflow definitions
├── src/test/                # Unit tests
└── workflows/               # DAX XML workflow files
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

These ranges match the paper's findings. Actual results depend on container load and network conditions.
