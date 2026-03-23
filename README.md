# Gene2Life Java Implementation

This repository implements the `gene2life` portion of the paper _Scheduling of Big Data Workflows in the Hadoop Framework with Heterogeneous Computing Cluster_ in Java. The current scope is the paper's 8-job `gene2life` DAG only:

`blast1, blast2 -> clustalw1, clustalw2 -> dnapars, protpars -> drawgram1, drawgram2`

The implementation keeps the paper's job names and dependencies, but runs real file-processing work on generated genomic-like data rather than using sleeps or a simulation-only model.

## What Is Implemented

- A generated large-data input set with query DNA and two reference database shards.
- Actual Java task executors for `blast`, `clustalw`, `dnapars`, `protpars`, and `drawgram`.
- A `WSH` scheduler following the paper's training-task and cluster-ordering idea.
- A `HEFT` scheduler for comparison.
- Heterogeneous cluster profiles based on the paper's four-cluster layout.
- Run reports with plan, metrics, makespan, speedup, and scheduling length ratio.

## Project Layout

- `src/main/java`: application source.
- `config/clusters-paper.csv`: four-cluster profile matching the paper's relative heterogeneity.
- `config/clusters-server.csv`: a server-oriented default profile for your Ubuntu host.
- `config/clusters-z4-g5.csv`: tuned profile for the reported Ubuntu Z4 G5 workstation.
- `scripts/build.sh`: compile with plain `javac`.
- `scripts/run.sh`: run the CLI.
- `scripts/generate-cluster-config.sh`: generate a cluster CSV from the current server.
- `scripts/server-benchmark.sh`: run a tuned server-side comparison.

## Quick Start

Build:

```bash
./scripts/build.sh
```

Generate data:

```bash
./scripts/run.sh generate-data \
  --workspace work/demo \
  --query-count 128 \
  --reference-records-per-shard 40000 \
  --sequence-length 240
```

Run WSH:

```bash
./scripts/run.sh run \
  --workspace work/demo \
  --data-root work/demo/data \
  --cluster-config config/clusters-server.csv \
  --scheduler wsh
```

Run HEFT:

```bash
./scripts/run.sh run \
  --workspace work/demo \
  --data-root work/demo/data \
  --cluster-config config/clusters-server.csv \
  --scheduler heft
```

Run both:

```bash
./scripts/run.sh compare \
  --workspace work/demo \
  --data-root work/demo/data \
  --cluster-config config/clusters-server.csv
```

## Big-Data Positioning

The paper's published `gene2life` figure shows small edge sizes, which are not enough for a meaningful big-data execution environment. This implementation therefore preserves the DAG and scheduling behavior while scaling the underlying input sizes through configurable data generation. For the Ubuntu server, start with:

```bash
./scripts/run.sh generate-data \
  --workspace work/server \
  --query-count 256 \
  --reference-records-per-shard 300000 \
  --sequence-length 320
```

Increase `reference-records-per-shard` until disk, memory, and runtime match the target experiment budget.

Do not use the Mac Mini for benchmark-sized runs. Use it only for compilation and smoke tests. The Ubuntu server should be the benchmark target.

## Ubuntu 24.04 Server Workflow

1. Install Java 17 or newer.
2. Build the code with `./scripts/build.sh`.
3. Generate data under a workspace on the server's local disk.
4. Run both schedulers against the same generated dataset.
5. Compare the resulting `work/.../wsh/README.md`, `schedule-plan.csv`, and `run-metrics.csv` with the HEFT equivalents.

One-command server benchmark:

```bash
chmod +x ./scripts/server-benchmark.sh
./scripts/server-benchmark.sh
```

Override dataset size through environment variables:

```bash
WORKSPACE=/data/gene2life \
QUERY_COUNT=256 \
REFERENCE_RECORDS_PER_SHARD=500000 \
SEQUENCE_LENGTH=320 \
./scripts/server-benchmark.sh
```

Run with the tuned Z4 G5 profile and explicit JVM settings:

```bash
PROFILE=medium \
CLUSTER_CONFIG=./config/clusters-z4-g5.csv \
GENE2LIFE_JAVA_OPTS="-Xms4g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication" \
./scripts/server-benchmark.sh
```

Auto-generate a cluster config on the server itself:

```bash
chmod +x ./scripts/generate-cluster-config.sh
./scripts/generate-cluster-config.sh ./config/clusters-autogen.csv
```

Detailed server guidance is in `docs/server-tuning.md`.

## Docker

The repository includes a `Dockerfile` for packaging the CLI once the classes are built:

```bash
./scripts/build.sh
docker build -t gene2life-java .
```

Example run:

```bash
docker run --rm -v "$PWD/work:/work" gene2life-java run \
  --workspace /work/demo \
  --data-root /work/demo/data \
  --cluster-config /app/config/clusters-server.csv \
  --scheduler wsh
```

If you want per-node container isolation later, the current scheduler and task boundaries are already separated cleanly enough to move each logical node into its own container process.

## Paper Mapping Notes

- Workflow structure comes directly from Fig. 2 in the paper.
- Communication cost is ignored during ranking and allocation, matching the paper.
- WSH uses training runs on the first node of each cluster before building the plan.
- When job runtimes across clusters are nearly equal, the implementation classifies the job as more IO-intensive and breaks ties toward the lower-powered cluster, matching the paper's stated policy.
- Detailed paper-to-code mapping is documented in `docs/paper-mapping.md`.

## Current Limitations

- This is a standalone Java workflow engine, not a drop-in Hi-WAY/Hadoop replacement.
- The biological tasks are workload-faithful Java implementations, not wrappers around native `blast`, `clustalw`, or PHYLIP binaries.
- The paper's exact runtime numbers depend on its original environment; this code is intended to reproduce the workflow structure and compare WSH versus HEFT on real large-file workloads in your own environment.
