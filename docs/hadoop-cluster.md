# Multi-Worker Hadoop Cluster

This repository can now provision a paper-closer Hadoop cluster on one physical server by running multiple `DataNode` + `NodeManager` worker containers instead of relying on a single host daemon pair.

## What This Improves

- separate Hadoop workers for each modeled node
- per-worker CPU and memory limits derived from the cluster CSV
- YARN node labels matching paper subclusters like `C1`, `C2`, `C3`, `C4`
- host-side client config that points the project at the containerized cluster

This is still one physical server, but it is much closer to the paper than a single-node Hadoop installation.

## Paper Node Counts

The paper varies the total number of virtual machines as `4`, `7`, `10`, and `13`.

In this repository:

- one container is always the Hadoop master
- the remaining containers are workers

So the paper’s `4/7/10/13` total-node experiments correspond to:

- `3` workers
- `6` workers
- `9` workers
- `12` workers

The new orchestration script handles that mapping automatically.

## Build And Start

```bash
chmod +x ./scripts/build-hadoop-cluster-image.sh ./scripts/generate-hadoop-paper-cluster.sh ./scripts/start-hadoop-paper-cluster.sh ./scripts/stop-hadoop-paper-cluster.sh ./scripts/validate-hadoop-paper-cluster.sh ./scripts/run-paper-hadoop-sweeps.sh

MAX_NODES=12 \
CLUSTER_CONFIG=./config/clusters-z4-g5-paper-sweep.csv \
OUTPUT_DIR=/home/cse-sdpl/bds_project/work/hadoop-paper-cluster \
./scripts/start-hadoop-paper-cluster.sh
```

This creates:

- `docker-compose.yml`
- `selected-nodes.csv`
- `client-conf/`
- `cluster.env`

`start-hadoop-paper-cluster.sh` now waits for:

- master HDFS readiness
- all expected HDFS `DataNode`s to register
- all expected YARN `NodeManager`s to register
- HDFS safe mode to turn off

It then creates the project HDFS roots and applies node labels for the modeled subclusters.

Validate the cluster before running the benchmark:

```bash
OUTPUT_DIR=/home/cse-sdpl/bds_project/work/hadoop-paper-cluster \
./scripts/validate-hadoop-paper-cluster.sh
```

This is the fast way to catch the exact failure mode you described: containers exist, but `DataNode` or `NodeManager` processes did not actually join the cluster.

## Run The Project Against The Cluster

```bash
source /home/cse-sdpl/bds_project/work/hadoop-paper-cluster/cluster.env
```

Then run the benchmark from the host:

```bash
WORKSPACE=/home/cse-sdpl/bds_project/work/gene2life-hadoop \
WORKFLOW=gene2life \
PROFILE=medium \
CLUSTER_CONFIG=./config/clusters-z4-g5-paper-sweep.csv \
COMPARE_ROUNDS=4 \
MAX_NODES=12 \
EXECUTOR=hadoop \
HDFS_DATA_ROOT=/gene2life/data/gene2life \
HDFS_BASE_WORK_ROOT=/gene2life/work \
./scripts/server-benchmark.sh
```

`cluster.env` sets:

- `HADOOP_CONF_DIR`
- `HADOOP_FS_DEFAULT`
- `HADOOP_YARN_RM`
- `HADOOP_FRAMEWORK_NAME=yarn`
- `HADOOP_ENABLE_NODE_LABELS=true`
- `GENE2LIFE_HADOOP_CLUSTER_DIR`

## Important Platform Note

Use this path on the HP Ubuntu server, not the Mac Mini. The project can be edited on macOS, but the Hadoop paper-cluster path is designed for Docker-on-Linux so the host can reach HDFS and YARN consistently during benchmark runs.

## Recommended Sweep

Use the orchestrator for the closest paper-style execution on one server:

```bash
PROFILE=medium \
./scripts/run-paper-hadoop-sweeps.sh
```

Useful overrides:

```bash
WORKFLOWS=gene2life \
TOTAL_NODE_COUNTS="4 7 10 13" \
./scripts/run-paper-hadoop-sweeps.sh
```

If you prefer worker counts directly instead of paper total-node counts:

```bash
WORKER_NODE_COUNTS="4 7 10 12" \
./scripts/run-paper-hadoop-sweeps.sh
```

## Manual Node-Count Sweeps

Bring up one cluster size at a time, run the benchmark, then stop it before the next size:

```bash
for workers in 3 6 9 12; do
  OUTPUT_DIR="/home/cse-sdpl/bds_project/work/hadoop-paper-cluster-${workers}" \
  MAX_NODES="$workers" \
  ./scripts/start-hadoop-paper-cluster.sh

  source "/home/cse-sdpl/bds_project/work/hadoop-paper-cluster-${workers}/cluster.env"

  WORKSPACE="/home/cse-sdpl/bds_project/work/gene2life-workers-${workers}" \
  WORKFLOW=gene2life \
  PROFILE=medium \
  CLUSTER_CONFIG=./config/clusters-z4-g5-paper-sweep.csv \
  MAX_NODES="$workers" \
  COMPARE_ROUNDS=4 \
  EXECUTOR=hadoop \
  ./scripts/server-benchmark.sh

  OUTPUT_DIR="/home/cse-sdpl/bds_project/work/hadoop-paper-cluster-${workers}" \
  ./scripts/stop-hadoop-paper-cluster.sh
done
```

Each generated cluster only includes the selected workers, using the same round-robin node-picking strategy used by the scheduler-side cluster limiting. This avoids leaving extra workers available to YARN during a smaller experiment.

## Stop The Cluster

```bash
OUTPUT_DIR=/home/cse-sdpl/bds_project/work/hadoop-paper-cluster \
./scripts/stop-hadoop-paper-cluster.sh
```
