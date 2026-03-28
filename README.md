# Big-Workflow Scheduling Java Implementation

This repository reproduces the paper's `WSH` versus `HEFT` comparison with:

- generated workflow-shaped datasets
- heterogeneous logical nodes
- Docker CPU pinning
- a Docker Hadoop cluster that provides `HDFS`
- real task execution inside pinned Docker worker containers

The primary execution path is now `hdfs-docker`:

- data and intermediates live in `HDFS`
- each scheduled workflow job is executed inside a pinned Docker logical node
- outputs are copied back into the normal local `work/...` tree

This keeps Hadoop/HDFS in the architecture, but avoids the slow "one YARN application per workflow job" path that was making the project unusable on this server.
It is a paper-faithful reproduction path, not a byte-for-byte recreation of the original Hi-WAY runtime stack.

## Paper Mapping

What matches the paper:

- `WSH` versus `HEFT`
- heterogeneous cluster-aware scheduling
- training-driven `WSH`
- the three workflow families
- Hadoop-backed storage through `HDFS`

What differs:

- this is a Java reimplementation, not the original Hi-WAY codebase
- workflow task bodies are Java approximations of the original bioinformatics tools
- execution runs on one strong Linux server with Dockerized Hadoop, not separate physical machines

## Workflow Scope

The repo supports all three workflows:

- `gene2life`
- `avianflu_small`
- `epigenomics`

By default, the workflow scales now match the paper's DAG sizes:

- `gene2life`: `8` jobs
- `avianflu_small`: `104` jobs with `100` `autodock` branches
- `epigenomics`: `100` jobs with `24` split branches

If you need a faster approximation on one server, you can reduce branch counts explicitly:

- `--avianflu-autodock-count <n>`
- `--epigenomics-split-count <n>`

That keeps the stage topology intact while shrinking the parallel fanout.

## Execution Modes

- `local`: single JVM execution
- `docker`: tasks run inside pinned Docker logical nodes
- `hadoop`: legacy path that submits one Hadoop job per workflow task; kept for reference, but slower
- `hdfs-docker`: recommended path; uses Docker Hadoop for `HDFS` and pinned Docker logical nodes for task execution

## Files That Matter

- [pom.xml](/home/cse-sdpl/bds_project/pom.xml)
- [scripts/build.sh](/home/cse-sdpl/bds_project/scripts/build.sh)
- [scripts/build-image.sh](/home/cse-sdpl/bds_project/scripts/build-image.sh)
- [scripts/build-hadoop-cluster-image.sh](/home/cse-sdpl/bds_project/scripts/build-hadoop-cluster-image.sh)
- [scripts/hadoop-docker-cluster.sh](/home/cse-sdpl/bds_project/scripts/hadoop-docker-cluster.sh)
- [scripts/run.sh](/home/cse-sdpl/bds_project/scripts/run.sh)
- [src/main/java/org/gene2life/hadoop/HdfsDockerJobRunner.java](/home/cse-sdpl/bds_project/src/main/java/org/gene2life/hadoop/HdfsDockerJobRunner.java)
- [src/main/java/org/gene2life/scheduler/WshScheduler.java](/home/cse-sdpl/bds_project/src/main/java/org/gene2life/scheduler/WshScheduler.java)

## Prerequisites

- Java 17+
- Maven
- Docker with Compose

Optional but recommended:

- stop any host Hadoop daemons before starting the Docker Hadoop cluster, so there is no confusion about which Hadoop you are talking to

## Build

Build the shaded jar:

```bash
./scripts/build.sh
```

Build the Docker image used for pinned logical-node execution:

```bash
./scripts/build-image.sh gene2life-java:latest
```

Build the Docker Hadoop cluster image:

```bash
./scripts/build-hadoop-cluster-image.sh gene2life-hadoop-cluster:3.4.3
```

## Start Hadoop

Start the Docker Hadoop cluster:

```bash
./scripts/hadoop-docker-cluster.sh up \
  ./config/clusters-z4-g5-paper-sweep.csv \
  ./work/hadoop-docker-cluster \
  gene2life-hadoop-cluster:3.4.3
```

`up` now waits for:

- daemon readiness
- `hdfs dfsadmin -safemode wait`
- `YARN` node registration
- label initialization

Validate the cluster:

```bash
./scripts/hadoop-docker-cluster.sh validate \
  ./config/clusters-z4-g5-paper-sweep.csv \
  ./work/hadoop-docker-cluster \
  gene2life-hadoop-cluster:3.4.3
```

Validate Docker CPU pinning:

```bash
./scripts/validate-docker-node-pinning.sh \
  ./config/clusters-z4-g5-paper-sweep.csv \
  gene2life-java:latest
```

Resolve the host IP used to talk to the Docker Hadoop cluster:

```bash
HOST_IP="$(hostname -I | awk '{print $1}')"
```

## Manual Commands

The following commands are the canonical path. They do not use `runner.sh` or `commands.sh`.
For paper-style comparisons, use node counts `4`, `7`, `10`, and `13`.
In this repo, `13` means the full `12` worker nodes from the paper-style cluster file plus the separate Hadoop master outside the scheduler.

### 1. Gene2Life

Generate data:

```bash
./scripts/run.sh generate-data \
  --workflow gene2life \
  --workspace ./work/final/gene2life \
  --data-root ./work/final/gene2life/data \
  --query-count 128 \
  --reference-records-per-shard 100000 \
  --sequence-length 240
```

Run the comparison:

```bash
./scripts/run.sh compare \
  --workflow gene2life \
  --workspace ./work/final/gene2life \
  --data-root ./work/final/gene2life/data \
  --cluster-config ./config/clusters-z4-g5-paper-sweep.csv \
  --max-nodes 4 \
  --rounds 3 \
  --training-warmup-runs 1 \
  --training-measure-runs 3 \
  --executor hdfs-docker \
  --docker-image gene2life-java:latest \
  --hadoop-conf-dir ./work/hadoop-docker-cluster/host-conf \
  --hadoop-fs-default "hdfs://${HOST_IP}:19000" \
  --hadoop-yarn-rm "${HOST_IP}:18032" \
  --hadoop-framework-name yarn
```

### 2. Avianflu Small

Generate data:

```bash
./scripts/run.sh generate-data \
  --workflow avianflu_small \
  --workspace ./work/final/avianflu_small \
  --data-root ./work/final/avianflu_small/data \
  --receptor-feature-count 128 \
  --ligand-atom-count 24
```

Run the comparison:

```bash
./scripts/run.sh compare \
  --workflow avianflu_small \
  --workspace ./work/final/avianflu_small \
  --data-root ./work/final/avianflu_small/data \
  --cluster-config ./config/clusters-z4-g5-paper-sweep.csv \
  --max-nodes 4 \
  --rounds 3 \
  --training-warmup-runs 1 \
  --training-measure-runs 3 \
  --executor hdfs-docker \
  --docker-image gene2life-java:latest \
  --hadoop-conf-dir ./work/hadoop-docker-cluster/host-conf \
  --hadoop-fs-default "hdfs://${HOST_IP}:19000" \
  --hadoop-yarn-rm "${HOST_IP}:18032" \
  --hadoop-framework-name yarn
```

### 3. Epigenomics

Generate data:

```bash
./scripts/run.sh generate-data \
  --workflow epigenomics \
  --workspace ./work/final/epigenomics \
  --data-root ./work/final/epigenomics/data \
  --reads-per-split 96 \
  --read-length 64 \
  --reference-record-count 256
```

Run the comparison:

```bash
./scripts/run.sh compare \
  --workflow epigenomics \
  --workspace ./work/final/epigenomics \
  --data-root ./work/final/epigenomics/data \
  --cluster-config ./config/clusters-z4-g5-paper-sweep.csv \
  --max-nodes 4 \
  --rounds 3 \
  --training-warmup-runs 1 \
  --training-measure-runs 3 \
  --executor hdfs-docker \
  --docker-image gene2life-java:latest \
  --hadoop-conf-dir ./work/hadoop-docker-cluster/host-conf \
  --hadoop-fs-default "hdfs://${HOST_IP}:19000" \
  --hadoop-yarn-rm "${HOST_IP}:18032" \
  --hadoop-framework-name yarn
```

## Fast Variant Commands

If you need a faster approximation on one server, keep the same commands but add:

- `--avianflu-autodock-count 8` for `avianflu_small`
- `--epigenomics-split-count 4` for `epigenomics`

Those reduced settings are faster, but they are not the paper-scale default anymore.

## Expected Outputs

Each workflow workspace should contain:

- `comparison.md`
- `round-01/wsh/README.md`
- `round-01/wsh/run-metrics.csv`
- `round-01/wsh/schedule-plan.csv`
- `round-01/heft/README.md`
- `round-01/heft/run-metrics.csv`
- `round-01/heft/schedule-plan.csv`

Example:

- [comparison.md](/home/cse-sdpl/bds_project/work/final-verify/gene2life/comparison.md)
- [run-metrics.csv](/home/cse-sdpl/bds_project/work/final-verify/gene2life/round-01/wsh/run-metrics.csv)
- [schedule-plan.csv](/home/cse-sdpl/bds_project/work/final-verify/gene2life/round-01/wsh/schedule-plan.csv)

## Stop Hadoop

When you are done:

```bash
./scripts/hadoop-docker-cluster.sh down \
  ./config/clusters-z4-g5-paper-sweep.csv \
  ./work/hadoop-docker-cluster \
  gene2life-hadoop-cluster:3.4.3
```

## Notes

- `hdfs-docker` is the primary validated path on this server.
- The old unattended wrappers are still in the repo, but they are not the primary run path anymore.
- The paper-scale comparison matrix is `4`, `7`, `10`, `13` nodes.
- In this repo, `13` maps to the full `12` paper-style worker nodes plus the separate Hadoop master container.
