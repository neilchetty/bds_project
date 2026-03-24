# Big-Workflow Scheduling Java Implementation

This repository implements the paper's workflow-scheduling comparison in Java with real file-processing tasks, Docker-isolated logical nodes, and workflow-specific data generators. It now supports all three workflows discussed in the paper:

- `gene2life`: 8 jobs
- `avianflu_small`: 104 jobs
- `epigenomics`: 100 jobs

The project is not a simulator-only model. Each task reads and writes real files and performs workload-faithful computation over generated bioinformatics-style data.

## What Is Implemented

- Modular workflow definitions under `src/main/java/org/gene2life/workflow`.
- Real task executors for all supported workflow stages under `src/main/java/org/gene2life/task/WorkflowTaskExecutors.java`.
- `WSH` and `HEFT` schedulers.
- Docker-backed logical nodes for isolated execution on one physical host.
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
- `config/clusters-paper.csv`: literal paper-style VM capacities
- `config/clusters-z4-g5-paper-sweep.csv`: paper-style 12-node sweep profile pinned onto the Ubuntu host
- `config/clusters-z4-g5-paper-sweep-scaled.csv`: stronger logical-node version of the same four-subcluster pattern
- `scripts/build.sh`: plain `javac` build
- `scripts/run.sh`: CLI launcher
- `scripts/build-image.sh`: Docker image builder
- `scripts/server-benchmark.sh`: workflow-aware benchmark launcher

## Build

```bash
./scripts/build.sh
```

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
  --scheduler wsh
```

Run a comparison:

```bash
./scripts/run.sh compare \
  --workflow gene2life \
  --workspace work/gene2life-demo \
  --data-root work/gene2life-demo/data \
  --cluster-config config/clusters-server.csv \
  --rounds 4 \
  --training-warmup-runs 1 \
  --training-measure-runs 3
```

## Runtime Positioning

The workflow structures follow the paper, but the datasets are intentionally sized for practical repeated experiments on one server:

- `gene2life` is the heaviest workflow and remains the reference runtime target.
- `avianflu_small` and `epigenomics` use smaller default datasets so they stay in the same general runtime band and do not exceed `gene2life` by default.
- For benchmark runs, keep the dataset fixed while varying node count or scheduler.

## Ubuntu Server Usage

The intended benchmark target is the Ubuntu 24.04 workstation, not the Mac Mini.

Build and run a default benchmark:

```bash
chmod +x ./scripts/server-benchmark.sh
./scripts/server-benchmark.sh
```

Run another workflow through the same benchmark harness:

```bash
WORKFLOW=epigenomics \
PROFILE=medium \
CLUSTER_CONFIG=./config/clusters-z4-g5-paper-sweep.csv \
COMPARE_ROUNDS=4 \
EXECUTOR=docker \
./scripts/server-benchmark.sh
```

Run the paper-style node-count sweep:

```bash
for nodes in 4 7 10 12; do
  WORKSPACE="/data/gene2life-nodes-${nodes}" \
  WORKFLOW=gene2life \
  CLUSTER_CONFIG=./config/clusters-z4-g5-paper-sweep.csv \
  MAX_NODES="$nodes" \
  COMPARE_ROUNDS=4 \
  TRAINING_WARMUP_RUNS=1 \
  TRAINING_MEASURE_RUNS=3 \
  EXECUTOR=docker \
  ./scripts/server-benchmark.sh
done
```

The benchmark scripts now reuse one dataset per workflow by default. Generation parameters are stored in `DATA_ROOT/.generation-metadata.env`, and regeneration happens only when those parameters change.

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

## Limitations

- This is a standalone Java workflow engine, not a full Hi-WAY/Hadoop deployment.
- The biological tools are Java workload approximations, not wrappers around native `blast`, `clustalw`, `autodock`, `maq`, or PHYLIP binaries.
- Docker-isolated logical nodes on one host are closer to the paper than one-JVM execution, but still not identical to separate VMs or a distributed Hadoop cluster.

## Additional Documentation

- `docs/paper-mapping.md`
- `docs/server-tuning.md`
