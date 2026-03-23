# WSH: Workflow Scheduling for Heterogeneous Hadoop Clusters

Implementation of the **WSH (Workflow Scheduling for Heterogeneous computing)** algorithm from the paper *"Scheduling of Big Data Workflows in the Hadoop Framework with Heterogeneous Computing Cluster"* (2025).

This project compares **WSH** against **HEFT** (Heterogeneous Earliest Finish Time) using three bioinformatics workflows (**Gene2life**, **Avian Flu**, **Epigenomics**) across a Docker-based heterogeneous cluster with 4 performance tiers (C1–C4, 28 nodes total) distributed across **4 Windows PCs**.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Architecture Overview](#architecture-overview)
3. [Step 1: Network Setup (All 4 PCs)](#step-1-network-setup-all-4-pcs)
4. [Step 2: Install Docker Desktop (All 4 PCs)](#step-2-install-docker-desktop-all-4-pcs)
5. [Step 3: Enable Docker TCP API (PC2, PC3, PC4)](#step-3-enable-docker-tcp-api-pc2-pc3-pc4)
6. [Step 4: Start Containers](#step-4-start-containers)
7. [Step 5: Configure IPs on Main PC](#step-5-configure-ips-on-main-pc)
8. [Step 6: Verify Connectivity](#step-6-verify-connectivity)
9. [Step 7: Build & Test](#step-7-build--test)
10. [Step 8: Run Benchmarks](#step-8-run-benchmarks)
11. [Big Data Pipeline (HDFS)](#big-data-pipeline-hdfs)
12. [Full Command Reference](#full-command-reference)
13. [Troubleshooting](#troubleshooting)
14. [Cluster Configuration](#cluster-configuration)

---

## Prerequisites

| Requirement | Details |
|---|---|
| **PCs** | 4 Windows 10/11 machines connected via Ethernet |
| **Docker Desktop** | v4.0+ with WSL2 backend (installed on ALL 4 PCs) |
| **JDK** | 21 or newer (on Main PC only) |
| **RAM** | 8 GB+ per PC |
| **CPU** | 4+ cores per PC (Main PC: 8+ cores recommended) |
| **Network** | Same LAN subnet, 1 Gbps Ethernet recommended |

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│  PC1 (Main PC) — Scheduler + C1 tier                        │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ Java Scheduler (WSH/HEFT)                               │ │
│  │ worker-c1-1 ... worker-c1-7  (cpu=4.0, mem=4GB each)   │ │
│  └─────────────────────────────────────────────────────────┘ │
│     ↕ Docker TCP API (port 2375)                             │
├──────────────────────────────────────────────────────────────┤
│  PC2 — C2 tier (mid-range)                                   │
│  worker-c2-1 ... worker-c2-7  (cpu=2.0, mem=2GB each)       │
├──────────────────────────────────────────────────────────────┤
│  PC3 — C3 tier (baseline)                                    │
│  worker-c3-1 ... worker-c3-7  (cpu=1.0, mem=1GB each)       │
├──────────────────────────────────────────────────────────────┤
│  PC4 — C4 tier (low-end)                                     │
│  worker-c4-1 ... worker-c4-7  (cpu=0.5, mem=512MB each)     │
└──────────────────────────────────────────────────────────────┘
```

| PC | Role | Tier | Containers | Docker Compose File |
|---|---|---|---|---|
| **PC1** (Main) | Scheduler + C1 | C1 (high-end) | worker-c1-1 to c1-7 | `docker-compose.yml` |
| **PC2** | Worker | C2 (mid-range) | worker-c2-1 to c2-7 | `docker-compose-c2.yml` |
| **PC3** | Worker | C3 (baseline) | worker-c3-1 to c3-7 | `docker-compose-c3.yml` |
| **PC4** | Worker | C4 (low-end) | worker-c4-1 to c4-7 | `docker-compose-c4.yml` |

---

## Step 1: Network Setup (All 4 PCs)

All 4 PCs must be on the **same LAN subnet** so they can communicate via Docker TCP API.

### Option A: Direct Ethernet (Recommended)
1. Connect all 4 PCs to the **same Ethernet switch/hub**
2. Assign **static IP addresses** on each PC:
   - Open **Settings → Network & Internet → Ethernet → Edit**
   - Set **IP assignment** to **Manual**
   - Enable **IPv4** and configure:

| PC | IP Address | Subnet Mask | Gateway |
|---|---|---|---|
| PC1 (Main) | `172.16.0.1` | `255.255.254.0` | `172.16.0.1` |
| PC2 | `172.16.0.2` | `255.255.254.0` | `172.16.0.1` |
| PC3 | `172.16.0.3` | `255.255.254.0` | `172.16.0.1` |
| PC4 | `172.16.0.4` | `255.255.254.0` | `172.16.0.1` |

3. **Verify connectivity** from PC1:
```powershell
ping 172.16.0.2
ping 172.16.0.3
ping 172.16.0.4
```

### Option B: Existing LAN
If all PCs are already on the same network (e.g., lab network), find each PC's IP with:
```powershell
ipconfig
# Look for the IPv4 Address under Ethernet adapter
```

---

## Step 2: Install Docker Desktop (All 4 PCs)

On **every PC** (PC1, PC2, PC3, PC4):

1. Download Docker Desktop from https://www.docker.com/products/docker-desktop/
2. Install with default settings (enable WSL2 backend)
3. Start Docker Desktop
4. Verify:
```powershell
docker version
```

---

## Step 3: Enable Docker TCP API (PC2, PC3, PC4)

The Main PC (PC1) sends Docker commands to remote PCs over TCP port 2375. This must be enabled on **each remote PC**.

### 3a. Enable in Docker Desktop
On **each remote PC** (PC2, PC3, PC4):
1. Open **Docker Desktop → Settings → General**
2. Check ✅ **"Expose daemon on tcp://localhost:2375 without TLS"**
3. Click **Apply & restart**

### 3b. Set Up Port Proxy & Firewall
Docker Desktop only listens on `localhost:2375` by default. To allow remote access, run `docker-inbound.ps1` **as Administrator** on each remote PC:

1. Copy `scripts\docker-inbound.ps1` to each remote PC
2. Open **PowerShell as Administrator** on each remote PC
3. Run:
```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\docker-inbound.ps1
```

This script:
- Creates a **port proxy** from the PC's LAN IP → `localhost:2375`
- Creates a **Windows Firewall rule** to allow inbound TCP on port 2375
- Displays the PC's IP address and a test command

### 3c. Verify from PC1
From the Main PC (PC1), test each remote PC:
```powershell
docker -H tcp://172.16.0.2:2375 version
docker -H tcp://172.16.0.3:2375 version
docker -H tcp://172.16.0.4:2375 version
```

You should see Docker version info for each. If you get a timeout, check firewall settings.

---

## Step 4: Start Containers

### On PC1 (Main PC)
```powershell
# Start C1 tier (7 containers)
docker compose up -d

# Or use the setup script:
powershell -ExecutionPolicy Bypass -File .\scripts\start-multi-machine.ps1
```

### On PC2
Copy `docker-compose-c2.yml` to PC2, then:
```powershell
docker compose -f docker-compose-c2.yml up -d
```

### On PC3
Copy `docker-compose-c3.yml` to PC3, then:
```powershell
docker compose -f docker-compose-c3.yml up -d
```

### On PC4
Copy `docker-compose-c4.yml` to PC4, then:
```powershell
docker compose -f docker-compose-c4.yml up -d
```

### Verify All Containers
From PC1, check remote containers:
```powershell
docker ps                                      # Local C1 containers
docker -H tcp://172.16.0.2:2375 ps            # PC2's C2 containers
docker -H tcp://172.16.0.3:2375 ps            # PC3's C3 containers
docker -H tcp://172.16.0.4:2375 ps            # PC4's C4 containers
```

Each should show 7 containers running.

---

## Step 5: Configure IPs on Main PC

### 5a. Edit `config/ip.txt`
Open `config\ip.txt` and add the 3 remote PC IPs (one per line, no comments):
```
172.16.0.2
172.16.0.3
172.16.0.4
```

### 5b. Run the Configuration Script
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\configure-cluster.ps1
```

This script:
- Reads IPs from `config/ip.txt`
- Updates `config/cluster/multi-machine-nodes.csv` (replaces `REPLACE_PC2_IP`, `REPLACE_PC3_IP`, `REPLACE_PC4_IP`)
- Tests Docker API connectivity to each remote PC

---

## Step 6: Verify Connectivity

```powershell
# Quick connectivity check
docker -H tcp://172.16.0.2:2375 ps
docker -H tcp://172.16.0.3:2375 ps
docker -H tcp://172.16.0.4:2375 ps

# Full test: execute a command on a remote container
docker -H tcp://172.16.0.2:2375 exec worker-c2-1 echo "Hello from C2"
docker -H tcp://172.16.0.3:2375 exec worker-c3-1 echo "Hello from C3"
docker -H tcp://172.16.0.4:2375 exec worker-c4-1 echo "Hello from C4"
```

---

## Step 7: Build & Test

All commands run on **PC1 (Main PC)** only.

```powershell
# Build the Java project
powershell -ExecutionPolicy Bypass -File .\scripts\build.ps1

# Run unit tests
powershell -ExecutionPolicy Bypass -File .\scripts\run-tests.ps1
# Expected: "All Java scheduler tests passed."

# Quick smoke test (Gene2life on all 28 nodes)
powershell -ExecutionPolicy Bypass -File .\scripts\run-quick-test.ps1
# Takes ~2 minutes. Runs Gene2life with WSH across all 4 PCs.
```

---

## Step 8: Run Benchmarks

### Full HEFT vs WSH Comparison (Recommended)

```powershell
# Fast workflows (Gene2life + Avianflu_fast + Epigenomics_fast) — ~30 min
powershell -ExecutionPolicy Bypass -File .\scripts\run-real-benchmark.ps1

# Original workflows (WARNING: takes many hours)
powershell -ExecutionPolicy Bypass -File .\scripts\run-real-benchmark.ps1 -SlowWorkflows
```

This runs HEFT vs WSH across 8 node counts (4, 7, 10, 13, 16, 20, 24, 28) and produces a comparison CSV.

### Single Workflow Execution

```powershell
# Gene2life with WSH
java -jar build\wsh-scheduler.jar execute --workflow Gene2life --algorithm WSH --output results\gene2life-wsh.csv

# Avianflu (fast variant) with HEFT
java -jar build\wsh-scheduler.jar execute --workflow Avianflu_fast --algorithm HEFT --output results\avianflu-heft.csv

# Epigenomics (fast variant) with WSH
java -jar build\wsh-scheduler.jar execute --workflow Epigenomics_fast --algorithm WSH --output results\epigenomics-wsh.csv
```

### Simulation Only (No Docker Required)

```powershell
java -jar build\wsh-scheduler.jar benchmark --node-counts 4,7,10,13,16,20,24,28 --output results\metrics.csv --schedules-dir results\schedules
```

---

## Big Data Pipeline (HDFS)

Run Gene2life with 30-60 GB of synthetic bioinformatics data on Hadoop HDFS.

### Quick Start

```powershell
# Handles everything: HDFS setup, data generation, upload, and execution
powershell -ExecutionPolicy Bypass -File .\scripts\run-bigdata-gene2life.ps1 -SizeGB 30
```

### Step-by-Step

```powershell
# 1. Setup Hadoop HDFS (NameNode on PC1, DataNodes on remote PCs)
powershell -ExecutionPolicy Bypass -File .\scripts\setup-hadoop.ps1
# Web UI: http://localhost:9870

# 2. Generate synthetic data
powershell -ExecutionPolicy Bypass -File .\scripts\generate-bigdata.ps1 -SizeGB 30

# 3. Upload to HDFS
powershell -ExecutionPolicy Bypass -File .\scripts\upload-to-hdfs.ps1

# 4. Run benchmark
java -jar build\wsh-scheduler.jar execute --workflow Gene2life --algorithm WSH --output results\bigdata-gene2life-wsh.csv
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
| `--nodes-file` | Path to node CSV | `config/cluster/multi-machine-nodes.csv` |
| `--output` | Output CSV path | `results/metrics.csv` |
| `--fast` | Use fast workflow variants *(real-benchmark only)* | On by default in script |

### Available Workflows

| Name | Tasks | Description |
|---|---|---|
| `Gene2life` | 8 | Genomic pipeline (~510s simulated makespan) |
| `Avianflu_small` | 102 | Molecular docking (~180,000s ⚠️) |
| `Avianflu_fast` ★ | 102 | Fast variant, 163× compressed (~1,100s) |
| `Epigenomics` | 100 | Epigenomic pipeline (~400,000s ⚠️) |
| `Epigenomics_fast` ★ | 100 | Fast variant, 160× compressed (~1,100s) |

> ★ **Fast variants** have identical DAG topology but scaled runtimes for feasible testing.

---

## Troubleshooting

### "No such container" error
The scheduler is trying to execute on a container that doesn't exist. Check:
```powershell
# Verify containers on the target PC
docker -H tcp://<REMOTE_IP>:2375 ps
```
Make sure IPs in `config/ip.txt` are correct and `configure-cluster.ps1` has been run.

### Remote PC connectivity fails
```powershell
# 1. Check basic network
ping <REMOTE_IP>

# 2. Check Docker TCP API
docker -H tcp://<REMOTE_IP>:2375 version

# 3. If step 2 fails, on the REMOTE PC run as Administrator:
Set-ExecutionPolicy -Scope Process Bypass
.\docker-inbound.ps1

# 4. Check Windows Firewall allows port 2375
netsh advfirewall firewall show rule name="Docker Remote API 2375"
```

### Docker Desktop won't start
```powershell
# Check WSL2 is installed
wsl --status

# If not installed:
wsl --install
# Restart PC after installation
```

### Tasks time out (>600s)
The `TIME_COMPRESSION_FACTOR` in `WorkloadExecutor.java` is 10×. For very long workflows, use the **fast variants** (`Avianflu_fast`, `Epigenomics_fast`).

### Port proxy not working
```powershell
# Check existing port proxies
netsh interface portproxy show all

# If the proxy exists but doesn't work, delete and re-add:
netsh interface portproxy delete v4tov4 listenaddress=<YOUR_IP> listenport=2375
.\docker-inbound.ps1
```

---

## Cluster Configuration

### Node Tiers (Paper Table III)

| Tier | CPU Factor | IO Factor | RAM | Docker CPUs | Docker Memory |
|---|---|---|---|---|---|
| **C1** (High) | 4.0 | 2.5 | 4 GB | 4.0 | 4096 MB |
| **C2** (Mid) | 2.0 | 1.5 | 2 GB | 2.0 | 2048 MB |
| **C3** (Base) | 1.0 | 1.0 | 1 GB | 1.0 | 1024 MB |
| **C4** (Low) | 0.5 | 0.6 | 512 MB | 0.5 | 512 MB |

### Project Structure

```
bds_project/
├── config/
│   ├── cluster/
│   │   ├── multi-machine-nodes.csv    # 28 nodes across 4 PCs (edit IPs here)
│   │   └── nodes-{4..28}.csv         # Per-count node configs
│   └── ip.txt                         # Remote PC IPs (PC2, PC3, PC4)
├── scripts/
│   ├── build.ps1                      # Compile Java project
│   ├── run-tests.ps1                  # Run unit tests
│   ├── run-quick-test.ps1             # Quick smoke test (Gene2life)
│   ├── run-real-benchmark.ps1         # Full HEFT vs WSH comparison
│   ├── start-multi-machine.ps1        # Start C1 tier on main PC
│   ├── stop-all-containers.ps1        # Stop all containers
│   ├── configure-cluster.ps1          # Auto-configure IPs from ip.txt
│   ├── docker-inbound.ps1             # Enable Docker TCP API (remote PCs)
│   ├── setup-hadoop.ps1               # Set up HDFS cluster
│   ├── generate-bigdata.ps1           # Generate synthetic data
│   ├── upload-to-hdfs.ps1             # Upload data to HDFS
│   └── run-bigdata-gene2life.ps1      # Full big data pipeline
├── src/main/java/org/bds/wsh/         # Java source code
├── src/test/java/org/bds/wsh/tests/   # Unit tests
├── workflows/                          # DAX workflow XML files
├── docker-compose.yml                  # C1 tier (Main PC)
├── docker-compose-c2.yml              # C2 tier (copy to PC2)
├── docker-compose-c3.yml              # C3 tier (copy to PC3)
├── docker-compose-c4.yml              # C4 tier (copy to PC4)
└── README.md
```

### Output Files

Results are saved to the `results/` directory:
- `metrics.csv` — Simulation benchmark results
- `real-metrics.csv` — Real execution benchmark results
- `schedules/` — Per-workflow schedule CSVs
- `real-executions/` — Per-run execution detail CSVs
- `bigdata-gene2life-*.csv` — Big data pipeline results
