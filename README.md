# Big-Workflow Scheduling Java Implementation

This repository implements the paper's workflow-scheduling comparison in Java with real file-processing tasks, Hadoop/HDFS-backed execution, optional Docker-isolated logical nodes, and workflow-specific data generators. It now supports all three workflows discussed in the paper:

- `gene2life`: 8 jobs
- `avianflu_small`: 104 jobs
- `epigenomics`: 100 jobs

The project is not a simulator-only model. Each task reads and writes real files and performs workload-faithful computation over generated bioinformatics-style data.

## What Is Implemented

- Modular workflow definitions under `src/main/java/org/gene2life/workflow`.
- Real task executors for all supported workflow stages under `src/main/java/org/gene2life/task/WorkflowTaskExecutors.java`.
- `WSH` and `HEFT` schedulers.
- Hadoop/HDFS execution for distributed processing.
- Docker-backed logical nodes for isolated single-host fallback execution.
- Paper-style heterogeneous cluster profiles and host-scaled profiles.
- Run reports with schedule plans, actual run metrics, and scheduler comparison output.

## Workflow Coverage

### `gene2life`

Paper DAG:

`blast1, blast2 -> clustalw1, clustalw2 -> dnapars, protpars -> drawgram1, drawgram2`

Data:

- generated query DNA FASTA
- two reference FASTA shards

### `avianflu_small`

Paper-sized DAG:

- `prepare-receptor`
- `prepare-gpf`
- `prepare-dpf`
- `auto-grid`
- `autodock-001 ... autodock-100`

Data:

- receptor-feature tables
- grid-template tables
- ligand-library metadata
- 100 ligand files

### `epigenomics`

Paper-sized DAG:

- `fastqSplit`
- `filterContams-001 ... 024`
- `sol2sanger-001 ... 024`
- `fastq2bfq-001 ... 024`
- `map-001 ... 024`
- `mapMerge`
- `maqIndex`
- `pileup`

Data:

- FASTQ reads
- reference FASTA
- contaminant motifs

## Modularity

New workflows are added by implementing one `WorkflowSpec`:

- declare the DAG
- declare data generation
- resolve runtime inputs
- resolve training inputs

The scheduler, report writer, Docker executor, and CLI all operate on the generic `WorkflowSpec` interface.

## Project Layout

- `src/main/java/org/gene2life/cli`: CLI entrypoint and argument parsing
- `src/main/java/org/gene2life/workflow`: modular workflow specifications
- `src/main/java/org/gene2life/task`: workflow task executors
- `src/main/java/org/gene2life/scheduler`: `WSH`, `HEFT`, duration model, training benchmarks
- `src/main/java/org/gene2life/execution`: workflow executor, node runtime, Docker node pool
- `src/main/java/org/gene2life/hadoop`: Hadoop/HDFS execution backend
- `config/clusters-paper.csv`: literal paper-style VM capacities
- `config/clusters-z4-g5-paper-sweep.csv`: paper-style 12-node sweep profile pinned onto the Ubuntu host
- `config/clusters-z4-g5-paper-sweep-scaled.csv`: stronger logical-node version of the same four-subcluster pattern
- `scripts/build.sh`: Maven build with Docker fallback when `mvn` is unavailable
- `scripts/run.sh`: CLI launcher
- `scripts/build-image.sh`: Docker image builder
- `scripts/server-benchmark.sh`: workflow-aware benchmark launcher
- `scripts/run-paper-hadoop-sweeps.sh`: paper-close multi-worker Hadoop sweep orchestrator

## Build

```bash
./scripts/build.sh
```

## Prerequisites

- Java 17
- Hadoop 3.x client and cluster access for `EXECUTOR=hadoop`
- `HADOOP_CONF_DIR` pointing at valid `core-site.xml`, `hdfs-site.xml`, `mapred-site.xml`, and `yarn-site.xml` when running against Hadoop
- Docker only if you want the fallback `EXECUTOR=docker` path

## Quick Start

Generate `gene2life` data:

```bash
./scripts/run.sh generate-data \
  --workflow gene2life \
  --workspace work/gene2life-demo \
  --query-count 128 \
  --reference-records-per-shard 40000 \
  --sequence-length 240
```

Generate `avianflu_small` data:

```bash
./scripts/run.sh generate-data \
  --workflow avianflu_small \
  --workspace work/avianflu-demo \
  --receptor-feature-count 256 \
  --ligand-atom-count 48
```

Generate `epigenomics` data:

```bash
./scripts/run.sh generate-data \
  --workflow epigenomics \
  --workspace work/epigenomics-demo \
  --reads-per-split 192 \
  --read-length 80 \
  --reference-record-count 640
```

Run a scheduler:

```bash
./scripts/run.sh run \
  --workflow gene2life \
  --workspace work/gene2life-demo \
  --data-root work/gene2life-demo/data \
  --cluster-config config/clusters-server.csv \
  --scheduler wsh \
  --executor local
```

Run a comparison:

```bash
./scripts/run.sh compare \
  --workflow gene2life \
  --workspace work/gene2life-demo \
  --data-root work/gene2life-demo/data \
  --cluster-config config/clusters-server.csv \
  --rounds 4 \
  --executor local \
  --training-warmup-runs 1 \
  --training-measure-runs 3
```

Run through Hadoop/HDFS:

```bash
./scripts/run.sh compare \
  --workflow gene2life \
  --workspace work/gene2life-hadoop \
  --data-root work/gene2life-hadoop/data \
  --cluster-config config/clusters-z4-g5-paper-sweep.csv \
  --rounds 4 \
  --executor hadoop \
  --hdfs-data-root /gene2life/data/gene2life \
  --hdfs-work-root /gene2life/work \
  --hadoop-conf-dir "$HADOOP_CONF_DIR"
```

## Runtime Positioning

The workflow structures follow the paper, but the datasets are intentionally sized for practical repeated experiments on one server:

- `gene2life` is the heaviest workflow and remains the reference runtime target.
- `avianflu_small` and `epigenomics` use smaller default datasets so they stay in the same general runtime band and do not exceed `gene2life` by default.
- For benchmark runs, keep the dataset fixed while varying node count or scheduler.

## Ubuntu Server Usage

The intended benchmark target is the Ubuntu 24.04 workstation, not the Mac Mini.

For the closest paper-style setup on one physical server, use the multi-worker Hadoop cluster described in `docs/hadoop-cluster.md`. The simpler host-native Hadoop mode remains available, but it is a fallback path rather than the most paper-faithful option.

The recommended path is now:

- start a multi-worker Hadoop cluster on the Linux server, not on the Mac Mini
- let the scheduler see the same heterogeneous subclusters described in the paper
- run each node-count experiment against a cluster that exposes only that many worker nodes

Run the paper-close orchestrated sweep:

```bash
chmod +x ./scripts/run-paper-hadoop-sweeps.sh
PROFILE=medium ./scripts/run-paper-hadoop-sweeps.sh
```

This script:

- provisions a fresh multi-worker Hadoop cluster for each paper node-count target
- reuses one local dataset per workflow
- mirrors data into HDFS for each fresh cluster
- runs `gene2life`, `avianflu_small`, and `epigenomics`
- stops the cluster cleanly before moving to the next node-count target

The paper’s figures vary the total number of virtual machines as `4/7/10/13`, which corresponds to `1 master + 3/6/9/12 workers`. The orchestrator uses those total counts by default. If you prefer to control worker counts directly, set `WORKER_NODE_COUNTS`.

Run only one workflow:

```bash
WORKFLOWS=epigenomics \
PROFILE=medium \
./scripts/run-paper-hadoop-sweeps.sh
```

Run the older host-side benchmark harness against an already-running Hadoop cluster:

```bash
source /home/cse-sdpl/bds_project/work/hadoop-paper-cluster/cluster.env

WORKSPACE=/home/cse-sdpl/bds_project/work/gene2life-hadoop \
  WORKFLOW=gene2life \
  PROFILE=medium \
  CLUSTER_CONFIG=./config/clusters-z4-g5-paper-sweep.csv \
  MAX_NODES=12 \
  COMPARE_ROUNDS=4 \
  TRAINING_WARMUP_RUNS=1 \
  TRAINING_MEASURE_RUNS=3 \
  EXECUTOR=hadoop \
  ./scripts/server-benchmark.sh
```

The benchmark scripts now reuse one dataset per workflow by default. Generation parameters are stored in `DATA_ROOT/.generation-metadata.env`, and regeneration happens only when those parameters change.

## Execution Backends

The benchmark scripts default to `EXECUTOR=hadoop`. That path:

- keeps generated datasets on local disk
- mirrors them into HDFS once per workflow dataset revision
- submits one Hadoop job per workflow stage
- writes stage outputs back to HDFS and mirrors the final files into the local run workspace for reports

`EXECUTOR=docker` and `EXECUTOR=local` remain available for fallback debugging.

## Docker

Build the execution image:

```bash
./scripts/build-image.sh gene2life-java:latest
```

The benchmark launcher uses this image automatically when `EXECUTOR=docker`. In Docker mode, each logical node is a persistent named container for one scheduler run, and jobs execute inside it with `docker exec`.

## Metrics

- `makespan`: actual wall-clock makespan from recorded job start/finish times
- `speedup`: sequential runtime sum divided by makespan
- `SLR`: makespan divided by a scheduler-independent modeled critical-path lower bound

For tiny smoke datasets, `SLR` can clamp to `1.0` because the static lower bound is intentionally conservative. Use benchmark-sized datasets for meaningful `SLR` trends.

## Verification

The current codebase has been compiled and smoke-tested locally for:

- `gene2life`
- `avianflu_small`
- `epigenomics`

Those smoke runs validate:

- data generation
- WSH training
- HEFT planning
- DAG execution
- report generation

Heavy Docker benchmarks should still be run on the Ubuntu server.

## Additional Docs

- `docs/hadoop-cluster.md`: paper-close multi-worker Hadoop setup and execution
- `docs/server-tuning.md`: server-oriented profile, runtime, and workflow sizing guidance
- `docs/paper-mapping.md`: paper-to-code mapping and remaining differences

## Limitations

- This is not a reimplementation of Hi-WAY itself; it is a custom Java workflow engine that now executes stages through Hadoop/HDFS.
- The biological tools are Java workload approximations, not wrappers around native `blast`, `clustalw`, `autodock`, `maq`, or PHYLIP binaries.
- The Hadoop backend relies on your cluster's Hadoop/YARN deployment and configuration.
- In Hadoop mode, logical node assignment drives scheduler modeling and requested container resources, but YARN still controls final physical placement unless you add queue or node-label policies outside this repository.
- Docker-isolated logical nodes on one host are a fallback mode, not the primary Big Data execution path.

## Additional Documentation

- `docs/paper-mapping.md`
- `docs/server-tuning.md`
- `docs/hadoop-cluster.md`
