# WSH: Workflow Scheduling for Heterogeneous Hadoop Clusters

Implementation of the **WSH (Workflow Scheduling for Heterogeneous computing)** algorithm from the paper *"Scheduling of Big Data Workflows in the Hadoop Framework with Heterogeneous Computing Cluster"* (2025).

This project compares **WSH** against **HEFT** (Heterogeneous Earliest Finish Time) using three bioinformatics workflows (**Gene2life**, **Avian Flu**, **Epigenomics**) across a Docker-based heterogeneous cluster with 4 performance tiers (C1–C4, 28 nodes total).

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Project Structure](#project-structure)
3. [Workflows](#workflows)
4. [Phase 1: Single Machine Setup](#phase-1-single-machine-setup)
5. [Phase 2: Multi-Machine Setup (4 PCs)](#phase-2-multi-machine-setup-4-pcs)
6. [Running Modes](#running-modes)
7. [Big Data Pipeline (HDFS)](#big-data-pipeline-hdfs)
8. [Full Command Reference](#full-command-reference)
9. [Troubleshooting](#troubleshooting)

---

## Prerequisites

| Requirement | Details |
|---|---|
| **OS** | Windows 10/11 (both phases) |
| **Docker Desktop** | v4.0+ with WSL2 backend |
| **JDK** | 21 or newer |
| **RAM** | 36 GB+ (single machine) / 8 GB per PC (multi-machine) |
| **CPU** | 8+ cores (single machine) / 4+ cores per PC (multi-machine) |
| **Disk** | 10 GB (simulation) / 80 GB+ (big data mode) |
| **Network** | — (single machine) / 1 Gbps Ethernet, `172.16.x.x/23` subnet (multi-machine) |

---

## Project Structure

```
bds_project/
├── config/
│   ├── cluster/
│   │   ├── local-docker-nodes.csv     # 28 nodes for single machine
│   │   ├── multi-machine-nodes.csv    # 28 nodes across 4 PCs
│   │   └── nodes-4.csv               # Minimal 4-node config
│   └── ip.txt                         # Remote PC IPs (multi-machine)
├── scripts/
│   ├── build.ps1                      # Compile Java project
│   ├── run-tests.ps1                  # Run unit tests
│   ├── run-quick-test.ps1             # Quick smoke test (Gene2life)
│   ├── run-real-benchmark.ps1         # Full HEFT vs WSH comparison
│   ├── start-single-machine.ps1       # Start 28 containers locally
│   ├── start-multi-machine.ps1        # Start C1 tier on main PC
│   ├── stop-all-containers.ps1        # Stop all containers
│   ├── setup-remote-machine.ps1       # Configure remote PCs
│   ├── configure-cluster.ps1          # Auto-configure IPs from ip.txt
│   ├── setup-hadoop.ps1               # Set up HDFS cluster
│   ├── generate-bigdata.ps1           # Generate 30-60 GB test data
│   ├── upload-to-hdfs.ps1             # Upload data to HDFS
│   └── run-bigdata-gene2life.ps1      # Full big data pipeline
├── src/main/java/org/bds/wsh/         # Java source code
├── src/test/java/org/bds/wsh/tests/   # Unit tests
├── workflows/                          # DAX workflow XML files
├── docker-compose.yml                  # Docker services definition
└── README.md                           # This file
```

---

## Workflows

| Workflow | Tasks | Critical Path | Estimated Makespan (28 nodes) |
|---|---|---|---|
| **Gene2life** | 8 | blast→clustalw→pars→drawgram | ~510s (simulation) |
| **Avianflu_small** | 102 | prepare→autogrid→100×autodock | ~180,000s ⚠️ |
| **Epigenomics** | 100 | split→filter→convert→map→merge | ~400,000s ⚠️ |
| **Avianflu_fast** ★ | 102 | Same DAG, 163× compressed | ~1,100s |
| **Epigenomics_fast** ★ | 100 | Same DAG, 160× compressed | ~1,100s |

> ★ **Fast variants** have identical DAG topology but scaled runtimes for feasible testing. Use these for simulation runs.

---

## Phase 1: Single Machine Setup

All 28 containers run on a single Windows PC with Docker Desktop.

### Step 1: Install Prerequisites

```powershell
# Verify Java version (must be 21+)
java --version

# Verify Docker is running
docker version
```

### Step 2: Start All Containers

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start-single-machine.ps1
```

This starts 28 containers (7 per tier: C1–C4). Wait for "All 28 containers running!" message.

### Step 3: Build the Project

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\build.ps1
```

### Step 4: Run Unit Tests

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run-tests.ps1
```

Expected output: `All Java scheduler tests passed.`

### Step 5: Run Quick Smoke Test

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run-quick-test.ps1
```

Executes Gene2life with WSH on all 28 nodes. Takes ~2 minutes.

### Step 6: Run Simulation Benchmark

```powershell
# All 3 original workflows (Gene2life runs fast, others are long)
java -jar build\wsh-scheduler.jar benchmark --output results\metrics.csv --schedules-dir results\schedules

# Just Gene2life + fast variants (recommended for quick testing)
java -jar build\wsh-scheduler.jar execute --workflow Gene2life --algorithm WSH --nodes-file config\cluster\local-docker-nodes.csv --output results\gene2life-wsh.csv
java -jar build\wsh-scheduler.jar execute --workflow Avianflu_fast --algorithm WSH --nodes-file config\cluster\local-docker-nodes.csv --output results\avianflu-fast-wsh.csv
java -jar build\wsh-scheduler.jar execute --workflow Epigenomics_fast --algorithm WSH --nodes-file config\cluster\local-docker-nodes.csv --output results\epigenomics-fast-wsh.csv
```

### Step 7: Run Full Real-World Benchmark (HEFT vs WSH)

```powershell
# Default: fast workflows only (Gene2life + Avianflu_fast + Epigenomics_fast)
powershell -ExecutionPolicy Bypass -File .\scripts\run-real-benchmark.ps1

# To run original workflows (WARNING: takes many hours)
powershell -ExecutionPolicy Bypass -File .\scripts\run-real-benchmark.ps1 -SlowWorkflows
```

This runs HEFT vs WSH across all 8 node counts (4, 7, 10, 13, 16, 20, 24, 28) and produces a comparison table.


### Step 8: Stop Containers When Done

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stop-all-containers.ps1
```

---

## Phase 2: Multi-Machine Setup (4 PCs)

Distributes the 28 containers across 4 Windows PCs connected via Ethernet.

| PC | Role | Tier | Containers | IP Example |
|---|---|---|---|---|
| **PC1** (Main) | Scheduler + C1 | C1 | worker-c1-1 to c1-7 | 172.16.0.1 |
| **PC2** | Worker | C2 | worker-c2-1 to c2-7 | 172.16.0.2 |
| **PC3** | Worker | C3 | worker-c3-1 to c3-7 | 172.16.0.3 |
| **PC4** | Worker | C4 | worker-c4-1 to c4-7 | 172.16.0.4 |

### Step 1: Setup Remote PCs (PC2, PC3, PC4)

On **each remote PC**, copy `scripts/setup-remote-machine.ps1` and run as **Administrator**:

```powershell
# On PC2:
powershell -ExecutionPolicy Bypass -File setup-remote-machine.ps1 -Tier C2

# On PC3:
powershell -ExecutionPolicy Bypass -File setup-remote-machine.ps1 -Tier C3

# On PC4:
powershell -ExecutionPolicy Bypass -File setup-remote-machine.ps1 -Tier C4
```

> **Important:** Also enable Docker TCP API in Docker Desktop:
> Settings → General → ✅ "Expose daemon on tcp://localhost:2375 without TLS"

Note the IP address displayed by each script.

### Step 2: Configure IPs on Main PC

Edit `config/ip.txt` with the 3 remote PC IPs (one per line):

```
172.16.0.2
172.16.0.3
172.16.0.4
```

Then auto-configure the CSV files:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\configure-cluster.ps1
```

### Step 3: Start C1 Tier on Main PC

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start-multi-machine.ps1
```

### Step 4: Verify Connectivity

```powershell
docker -H tcp://172.16.0.2:2375 ps
docker -H tcp://172.16.0.3:2375 ps
docker -H tcp://172.16.0.4:2375 ps
```

### Step 5: Build and Run

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\build.ps1

# Quick test
java -jar build\wsh-scheduler.jar execute --workflow Gene2life --algorithm WSH --nodes-file config\cluster\multi-machine-nodes.csv --output results\multi-gene2life.csv

# Full benchmark
powershell -ExecutionPolicy Bypass -File .\scripts\run-real-benchmark.ps1
```

---

## Running Modes

### Mode 1: Simulation (No Docker Required)

Calculates theoretical schedules and metrics without executing on containers.

```powershell
java -jar build\wsh-scheduler.jar benchmark --node-counts 4,7,10,13,16,20,24,28 --output results\metrics.csv --schedules-dir results\schedules
```

### Mode 2: Real Execution (Docker Required)

Runs actual workloads inside Docker containers with real CPU/IO stress.

```powershell
java -jar build\wsh-scheduler.jar execute --workflow Gene2life --algorithm WSH --nodes-file config\cluster\local-docker-nodes.csv --output results\execution.csv
```

### Mode 3: Full Benchmark Comparison

Compares WSH vs HEFT across multiple node configurations.

```powershell
java -jar build\wsh-scheduler.jar real-benchmark --node-counts 4,7,10,13,16,20,24,28 --output results\real-metrics.csv --details-dir results\real-executions
```

---

## Big Data Pipeline (HDFS)

Run the Gene2life workflow with 30-60 GB of synthetic data on Hadoop HDFS.

### Quick Start (Single Command)

```powershell
# This handles everything: HDFS setup, data generation, upload, and execution.
powershell -ExecutionPolicy Bypass -File .\scripts\run-bigdata-gene2life.ps1 -SizeGB 30
```

### Step-by-Step

#### 1. Setup Hadoop HDFS

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup-hadoop.ps1
# Web UI: http://localhost:9870
```

#### 2. Generate Synthetic Data

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\generate-bigdata.ps1 -SizeGB 30
# Creates: bigdata/blast_input1.dat, blast_input2.dat, clustalw_ref.dat, etc.
```

#### 3. Upload to HDFS

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\upload-to-hdfs.ps1
# Uploads to: hdfs:///wsh/gene2life/
```

#### 4. Run Gene2life on Big Data

```powershell
java -jar build\wsh-scheduler.jar execute --workflow Gene2life --algorithm WSH --nodes-file config\cluster\local-docker-nodes.csv --output results\bigdata-gene2life-wsh.csv
```

### Multi-Machine Big Data

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run-bigdata-gene2life.ps1 -Phase multi -SizeGB 60
```

---

## Full Command Reference

| Command | Description |
|---|---|
| `benchmark` | Simulated benchmark (no Docker) |
| `schedule` | Generate single schedule |
| `verify` | Validate metrics CSV |
| `execute` | Run workflow on Docker containers |
| `real-benchmark` | Full HEFT vs WSH comparison on Docker |

### Common Options

| Option | Description | Default |
|---|---|---|
| `--workflow` | Workflow name | `Gene2life` |
| `--algorithm` | `WSH` or `HEFT` | `WSH` |
| `--node-counts` | Comma-separated node counts | `4,7,10,13,16,20,24,28` |
| `--nodes-file` | Path to node CSV | Paper-faithful cluster |
| `--output` | Output CSV path | `results/metrics.csv` |
| `--training-file` | Training profile CSV | None (static model) |

### Available Workflows

| Name | Built-in | Description |
|---|---|---|
| `Gene2life` | ✅ | 8-task genomic pipeline (~510s) |
| `Avianflu_small` | ✅ | 102-task molecular docking (~180,000s) |
| `Avianflu_fast` | ✅ | Fast variant (~1,100s) |
| `Epigenomics` | ✅ | 100-task epigenomic pipeline (~400,000s) |
| `Epigenomics_fast` | ✅ | Fast variant (~1,100s) |

---

## Troubleshooting

### Docker containers won't start

```powershell
# Check Docker Desktop is running
docker version

# Check available resources
docker system info | Select-String -Pattern "Total Memory|CPUs"

# Restart Docker Desktop and try again
```

### "No candidate nodes found" error

This means the scheduler has zero nodes available. Ensure:
- You're passing the correct `--nodes-file`
- The CSV file has valid data with the correct header

### Remote PC connectivity fails

```powershell
# Verify Docker TCP API is enabled on remote PC
docker -H tcp://<REMOTE_IP>:2375 version

# Check Windows Firewall allows port 2375
# Check PCs are on the same subnet (172.16.x.x/23)
```

### Tasks time out (> 600s)

The `TIME_COMPRESSION_FACTOR` in `WorkloadExecutor.java` is set to 10× by default. For very long workflows, consider using the **fast variants** (`Avianflu_fast`, `Epigenomics_fast`) instead.

### Unit tests fail after changes

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\build.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\run-tests.ps1
```

### HDFS NameNode won't start

```powershell
# Remove old containers and retry
docker rm -f hadoop-namenode hadoop-datanode1 hadoop-datanode2
powershell -ExecutionPolicy Bypass -File .\scripts\setup-hadoop.ps1
```

---

## Cluster Configuration

### Node Tiers (Paper Table III)

| Tier | CPU Factor | IO Factor | RAM | Docker CPUs | Docker Memory |
|---|---|---|---|---|---|
| **C1** (High) | 4.0 | 2.5 | 4 GB | 4.0 | 4096 MB |
| **C2** (Mid) | 2.0 | 1.5 | 2 GB | 2.0 | 2048 MB |
| **C3** (Base) | 1.0 | 1.0 | 1 GB | 1.0 | 1024 MB |
| **C4** (Low) | 0.5 | 0.6 | 512 MB | 1.0 | 512 MB |

### Output Files

Results are saved to the `results/` directory:
- `metrics.csv` — Simulation benchmark results
- `real-metrics.csv` — Real execution benchmark results
- `schedules/` — Per-workflow schedule CSVs
- `real-executions/` — Per-run execution detail CSVs
- `bigdata-gene2life-*.csv` — Big data pipeline results
