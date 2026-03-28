# Big-Workflow Scheduling Java Implementation

This repository reproduces the paper's `WSH` versus `HEFT` workflow-scheduling study in Java with real task execution, generated workflow-shaped datasets, Docker-backed logical nodes, and a Docker Hadoop cluster for `HDFS` and `YARN` execution.

It implements the three paper workflows:

- `gene2life` with 8 jobs
- `avianflu_small` with 104 jobs
- `epigenomics` with 100 jobs

The target is paper-faithful behavior on one powerful Linux server. It is not a byte-for-byte recreation of the original Hi-WAY deployment.

## Paper Fidelity

What matches the paper:

- the workflow families and DAG sizes
- the `WSH` versus `HEFT` comparison
- heterogeneous cluster profiles
- repeated benchmarking with training-based `WSH`
- a Hadoop-backed execution path using `HDFS` and `YARN`

What intentionally differs:

- this repository contains its own Java workflow engine instead of the original Hi-WAY codebase
- the biological tools are Java workload approximations rather than native `blast`, `clustalw`, `autodock`, `maq`, or PHYLIP binaries
- the Hadoop cluster is Dockerized on one host rather than spread across separate VMs

The authoritative mapping is in [docs/paper-mapping.md](/home/cse-sdpl/bds_project/docs/paper-mapping.md).

## Architecture

The runtime has three execution modes:

- `local`: one JVM executes assigned tasks directly
- `docker`: each logical node is a Docker container; the scheduler uses `docker exec`
- `hadoop`: inputs are mirrored into `HDFS`, each workflow task is submitted as a one-map Hadoop job, intermediates live in `HDFS`, and final run artifacts are copied back into the normal local `work/...` report structure

The main components are:

- `src/main/java/org/gene2life/workflow`
  workflow specs, DAGs, dataset generation, local-path and HDFS-path resolution
- `src/main/java/org/gene2life/task`
  concrete workflow task executors
- `src/main/java/org/gene2life/scheduler`
  `WSH`, `HEFT`, duration modeling, and training benchmarks
- `src/main/java/org/gene2life/execution`
  workflow executor, node runtime, and Docker logical-node pool
- `src/main/java/org/gene2life/hadoop`
  HDFS sync, Hadoop submission, task wrapper job, and host-side Yarn monitoring

## Process Ownership And Cleanup

The project only cleans up runtime that it starts itself:

- Docker logical-node containers labeled `org.gene2life.runtime=docker-node`
- the project Docker Hadoop cluster created by `scripts/hadoop-docker-cluster.sh`
- scheduler executor threads created inside the Java process

It does not stop host Hadoop daemons installed outside this repository.

For benchmark runs:

- `scripts/server-benchmark.sh` now tears down project-owned Docker runtime on exit
- `EXECUTOR=hadoop` also stops the project Docker Hadoop cluster by default
- set `HADOOP_KEEP_CLUSTER=true` if you want to keep that cluster alive for reuse
- `./runner.sh` and `./commands.sh` use the same cleanup default unless you explicitly override `HADOOP_KEEP_CLUSTER=true`

## Cluster Profiles

The important profiles are:

- `config/clusters-z4-g5-paper-sweep.csv`
  default paper-style 12-node heterogeneous profile for this server
- `config/clusters-z4-g5-dense-28.csv`
  dense 28-node host extension for overnight sweeps using the same four-cluster ordering idea
- `config/clusters-z4-g5-paper-sweep-scaled.csv`
  same four-subcluster shape with larger logical nodes for higher host utilization
- `config/clusters-z4-g5.csv`
  host-oriented profile if you want a different non-paper logical layout

Use the paper-style profile when the goal is structural similarity to the paper. Use the dense 28-node profile when the goal is to run the requested `2 4 12 28` logical-node sweep on this host. Use the scaled profile when the goal is to push more CPU and memory through the same workflow mix on this server without changing the 12-node shape.

## Build

Prerequisites:

- Java 17 or newer
- Maven
- Docker with Compose

Build the shaded application jar:

```bash
./scripts/build.sh
```

Build the Docker execution image used by `EXECUTOR=docker`:

```bash
./scripts/build-image.sh gene2life-java:latest
```

Build the Docker Hadoop cluster image:

```bash
./scripts/build-hadoop-cluster-image.sh gene2life-hadoop-cluster:3.4.3
```

## Step-By-Step Hadoop Run

1. Build the application and Hadoop image.

```bash
./scripts/build.sh
./scripts/build-hadoop-cluster-image.sh gene2life-hadoop-cluster:3.4.3
```

2. Start the project Docker Hadoop cluster on the paper-style profile.

```bash
./scripts/hadoop-docker-cluster.sh up \
  ./config/clusters-z4-g5-paper-sweep.csv \
  ./work/hadoop-docker-cluster \
  gene2life-hadoop-cluster:3.4.3
```

`up` waits for `HDFS` to leave safemode, confirms `YARN` node registration, and applies node labels before returning success.

3. Validate `HDFS`, `YARN`, node labels, and a tiny Hadoop task submission.

```bash
./scripts/hadoop-docker-cluster.sh validate \
  ./config/clusters-z4-g5-paper-sweep.csv \
  ./work/hadoop-docker-cluster \
  gene2life-hadoop-cluster:3.4.3
```

Validate Docker-only CPU pinning with the same `cpu_set` semantics used by the logical-node executor:

```bash
./scripts/validate-docker-node-pinning.sh \
  ./config/clusters-z4-g5-paper-sweep.csv \
  gene2life-java:latest
```

4. Generate a workflow dataset.

```bash
./scripts/run.sh generate-data \
  --workflow gene2life \
  --workspace ./work/gene2life-hadoop \
  --data-root ./work/gene2life-hadoop/data \
  --query-count 256 \
  --reference-records-per-shard 300000 \
  --sequence-length 320
```

5. Run one scheduler through Hadoop.

```bash
HOST_IP="$(hostname -I | awk '{print $1}')"

./scripts/run.sh run \
  --workflow gene2life \
  --workspace ./work/gene2life-hadoop \
  --data-root ./work/gene2life-hadoop/data \
  --cluster-config ./config/clusters-z4-g5-paper-sweep.csv \
  --scheduler heft \
  --executor hadoop \
  --hadoop-conf-dir ./work/hadoop-docker-cluster/host-conf \
  --hadoop-fs-default "hdfs://${HOST_IP}:19000" \
  --hadoop-framework-name yarn \
  --hadoop-yarn-rm "${HOST_IP}:18032" \
  --hadoop-enable-node-labels true
```

6. Run the full scheduler comparison through Hadoop.

```bash
EXECUTOR=hadoop \
WORKFLOW=gene2life \
PROFILE=medium \
CLUSTER_CONFIG=./config/clusters-z4-g5-paper-sweep.csv \
./scripts/server-benchmark.sh
```

7. Stop the project Docker Hadoop cluster when you are done.

```bash
./scripts/hadoop-docker-cluster.sh down \
  ./config/clusters-z4-g5-paper-sweep.csv \
  ./work/hadoop-docker-cluster \
  gene2life-hadoop-cluster:3.4.3
```

If you use `scripts/server-benchmark.sh` with `EXECUTOR=hadoop`, step 7 is automatic unless `HADOOP_KEEP_CLUSTER=true`.

## Submission Run

Use `./runner.sh` for the reliable submission-oriented path. It now defaults to a finish-today preset instead of the older multi-round overnight sweep.

Default submission preset:

- `EXECUTOR=hadoop`
- `WORKFLOWS="gene2life avianflu_small"`
- `NODE_COUNTS="2 4 12 28"`
- `PROFILE=small`
- `COMPARE_ROUNDS=1`
- `TRAINING_WARMUP_RUNS=0`
- `TRAINING_MEASURE_RUNS=1`
- cleanup of repo-owned Docker runtime on exit unless `HADOOP_KEEP_CLUSTER=true`

Set `FULL_SWEEP=true` if you want the older slower research-style defaults (`PROFILE=medium`, `COMPARE_ROUNDS=4`, more training samples).

1. Stop any host Hadoop services you do not want running in parallel. The repo does not manage host Hadoop daemons for you.
2. Start the unattended run.

```bash
./runner.sh
```

3. Watch the live console or summary if needed.

```bash
tail -f work/overnight-run-*/runner-console.log
./scripts/submission-status.sh
```

The submission flow runs, in order:

- system and environment capture
- `./scripts/submission-preflight.sh`
- the `2 4 12 28` Hadoop sweep across `gene2life` and `avianflu_small`
- HTML, PDF, text, and summary report generation
- cleanup of repo-owned Docker runtime unless `HADOOP_KEEP_CLUSTER=true`

The unattended session directory is:

- `work/overnight-run-<timestamp>/`

Important outputs inside that directory:

- `runner-console.log`
- `transcript.log`
- `report.html`
- `report.pdf`
- `report.txt`
- `result-summary.txt`
- `run.env`
- `workspaces/` with all workflow and node-count benchmark outputs
- `sweep-logs/` with per-run logs
- `preflight/` with the preflight log and smoke workspace

`./commands.sh` is the direct foreground version of the same flow. It accepts an optional session directory path:

```bash
./commands.sh
./commands.sh /absolute/path/to/session-dir
```

If `wkhtmltopdf` is not installed, the unattended run still succeeds and writes the HTML and text reports.

Use the status helper at any time during a live run:

```bash
./scripts/submission-status.sh
./scripts/submission-status.sh /absolute/path/to/work/overnight-run-<timestamp>
```

It reports the current matrix item, completed comparisons, repo-owned processes, Docker CPU usage, and active YARN applications.

## Benchmark Commands

Default manual benchmark:

```bash
./scripts/server-benchmark.sh
```

Reliable submission-oriented manual benchmark:

```bash
EXECUTOR=hadoop \
PROFILE=small \
COMPARE_ROUNDS=1 \
TRAINING_WARMUP_RUNS=0 \
TRAINING_MEASURE_RUNS=1 \
./scripts/server-benchmark.sh
```

Paper-style Hadoop benchmark:

```bash
EXECUTOR=hadoop \
WORKFLOW=gene2life \
PROFILE=medium \
CLUSTER_CONFIG=./config/clusters-z4-g5-paper-sweep.csv \
./scripts/server-benchmark.sh
```

Higher-utilization Hadoop benchmark on the same server:

```bash
EXECUTOR=hadoop \
WORKFLOW=gene2life \
PROFILE=medium \
CLUSTER_CONFIG=./config/clusters-z4-g5-paper-sweep-scaled.csv \
GENE2LIFE_JAVA_OPTS="-Xms8g -Xmx24g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication" \
./scripts/server-benchmark.sh
```

Paper-style node-count sweep:

```bash
for nodes in 2 4 12; do
  WORKSPACE="./work/gene2life-paper-${nodes}" \
  WORKFLOW=gene2life \
  CLUSTER_CONFIG=./config/clusters-z4-g5-paper-sweep.csv \
  MAX_NODES="$nodes" \
  EXECUTOR=hadoop \
  ./scripts/server-benchmark.sh
done
```

Dense 28-node extension run:

```bash
WORKSPACE="./work/gene2life-dense-28" \
WORKFLOW=gene2life \
CLUSTER_CONFIG=./config/clusters-z4-g5-dense-28.csv \
MAX_NODES=28 \
EXECUTOR=hadoop \
./scripts/server-benchmark.sh
```

Full overnight sweep for all three workflows with automatic `2 4 12 28` config selection:

```bash
FULL_SWEEP=true ./scripts/run-all-workflow-sweeps.sh
```

The sweep wrapper now defaults to:

- `EXECUTOR=hadoop`
- workflows `gene2life avianflu_small` in submission mode
- workflows `gene2life avianflu_small epigenomics` in full-sweep mode
- node counts `2 4 12 28`
- paper profile for `2`, `4`, and `12`
- dense 28-node profile for `28`
- output root `work/hadoop-overnight-sweeps` when you run `./scripts/run-all-workflow-sweeps.sh` directly
- cleanup of the project Docker Hadoop cluster at the end unless `RUN_ALL_KEEP_CLUSTER=true`

When the same sweep runs through `./runner.sh` or `./commands.sh`, the sweep output root is redirected into that session at `work/overnight-run-<timestamp>/workspaces/`.

## Data Generation

The benchmark wrapper is workflow-aware and reuses datasets by default. It stores the generation parameters in `DATA_ROOT/.generation-metadata.env` and regenerates only when those parameters change.

Workflow defaults:

- `gene2life`
  `QUERY_COUNT`, `REFERENCE_RECORDS_PER_SHARD`, `SEQUENCE_LENGTH`
- `avianflu_small`
  `RECEPTOR_FEATURE_COUNT`, `LIGAND_ATOM_COUNT`
- `epigenomics`
  `READS_PER_SPLIT`, `READ_LENGTH`, `REFERENCE_RECORD_COUNT`

## Hadoop Docker Cluster Details

The Docker Hadoop helper manages a single project-owned cluster and publishes non-conflicting host ports so it can coexist with host Hadoop services:

- HDFS RPC: `19000`
- NameNode UI: `19870`
- YARN scheduler: `18030`
- YARN tracker: `18031`
- YARN ResourceManager RPC: `18032`
- YARN admin: `18033`
- YARN UI: `18088`

Useful commands:

```bash
./scripts/hadoop-docker-cluster.sh status
./scripts/hadoop-docker-cluster.sh health
./scripts/hadoop-docker-cluster.sh validate
./scripts/hadoop-docker-cluster.sh down
```

`validate` checks:

- HDFS health and a write/read roundtrip
- YARN node registration
- node-label configuration
- worker CPU and memory pinning against the cluster config
- worker-to-master name resolution and HDFS access from a worker container
- a tiny end-to-end Hadoop workflow submission

`health` also prints the current safemode state before the normal `HDFS` and `YARN` reports.

## Outputs

Every run keeps the same report shape regardless of executor:

- `comparison.md`
- `round-01`, `round-02`, ...
- per-scheduler run directories with `schedule-plan.csv`, `run-metrics.csv`, and task outputs

During Hadoop execution, payload data and intermediates live in `HDFS`; the final reports remain local under the selected workspace.

For unattended runs started with `./runner.sh`, the selected workspace root is the per-session `workspaces/` directory under `work/overnight-run-<timestamp>/`.

`small`, `medium`, and `large` profiles change dataset size only. They do not change workflow structure or job count.

## Metrics

- `makespan`
  actual wall-clock time from first job start to last job finish
- `speedup`
  sequential runtime sum divided by makespan
- `SLR`
  makespan divided by a scheduler-independent critical-path lower bound

For very small smoke datasets, `SLR` can flatten near `1.0` because the lower bound is intentionally conservative. Use benchmark-sized datasets for meaningful scheduler comparison.

## Additional Documentation

- [docs/paper-mapping.md](/home/cse-sdpl/bds_project/docs/paper-mapping.md)
- [docs/server-tuning.md](/home/cse-sdpl/bds_project/docs/server-tuning.md)
