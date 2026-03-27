# Server Tuning

## Target Host

This guide targets the Linux server used for the project:

- 36 hardware threads
- about 62 GiB RAM
- local disk workspace
- Docker and Docker Compose available
- host Hadoop may already be installed separately

The repository-managed Docker Hadoop cluster is designed to coexist with host Hadoop by using its own non-conflicting published ports.

## Default Versus High-Utilization Modes

There are two recommended operating modes:

- paper-style default
  use `config/clusters-z4-g5-paper-sweep.csv` when the goal is to stay closest to the paper's heterogeneous 12-node structure
- dense overnight extension
  use `config/clusters-z4-g5-dense-28.csv` when the goal is to run the requested 28 logical-node sweep on this host
- high-utilization variant
  use `config/clusters-z4-g5-paper-sweep-scaled.csv` when the goal is to push more of this server's CPU and memory while keeping the same four-subcluster shape

The benchmark wrapper now defaults to the paper-style profile.

## Recommended Baseline Settings

Start here for repeatable scheduler studies:

- `WORKFLOW=gene2life`
- `PROFILE=medium`
- `COMPARE_ROUNDS=4`
- `TRAINING_WARMUP_RUNS=1`
- `TRAINING_MEASURE_RUNS=3`
- `CLUSTER_CONFIG=./config/clusters-z4-g5-paper-sweep.csv`
- `GENE2LIFE_JAVA_OPTS="-Xms4g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication"`

For the higher-utilization preset, start with:

- `CLUSTER_CONFIG=./config/clusters-z4-g5-paper-sweep-scaled.csv`
- `GENE2LIFE_JAVA_OPTS="-Xms8g -Xmx24g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication"`

Increase dataset size only after the JVM and Docker runtime remain stable at the chosen logical-node profile.

## Executor Guidance

Use `EXECUTOR=docker` when you want scheduler studies without HDFS/YARN overhead.

Use `EXECUTOR=hadoop` when the project requirement is to execute through Hadoop, HDFS, and YARN. In this mode:

- data is mirrored into HDFS
- each workflow task runs as a Hadoop job
- outputs are copied back into the normal local report layout
- the benchmark wrapper starts the project Docker Hadoop cluster automatically
- the wrapper tears it down automatically unless `HADOOP_KEEP_CLUSTER=true`

## Workflow Dataset Defaults

The benchmark harness keeps the three workflows in a similar practical runtime band on this host.

- `gene2life`
  `QUERY_COUNT`, `REFERENCE_RECORDS_PER_SHARD`, `SEQUENCE_LENGTH`
- `avianflu_small`
  `RECEPTOR_FEATURE_COUNT`, `LIGAND_ATOM_COUNT`
- `epigenomics`
  `READS_PER_SPLIT`, `READ_LENGTH`, `REFERENCE_RECORD_COUNT`

Datasets are reused by default:

- `REUSE_DATA=true`
- generation metadata is written to `DATA_ROOT/.generation-metadata.env`
- reruns stay on the same input unless the generation parameters change

This is the preferred setup for fair paper-style node-count sweeps.

## Commands

Default paper-style Hadoop comparison:

```bash
EXECUTOR=hadoop \
WORKFLOW=gene2life \
PROFILE=medium \
CLUSTER_CONFIG=./config/clusters-z4-g5-paper-sweep.csv \
./scripts/server-benchmark.sh
```

Higher-utilization Hadoop comparison:

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

Dense 28-node extension:

```bash
WORKSPACE="./work/gene2life-dense-28" \
WORKFLOW=gene2life \
CLUSTER_CONFIG=./config/clusters-z4-g5-dense-28.csv \
MAX_NODES=28 \
EXECUTOR=hadoop \
./scripts/server-benchmark.sh
```

Full overnight sweep:

```bash
./scripts/run-all-workflow-sweeps.sh
```

Validate the project Docker Hadoop cluster before a long run:

```bash
./scripts/hadoop-docker-cluster.sh validate \
  ./config/clusters-z4-g5-paper-sweep.csv \
  ./work/hadoop-docker-cluster \
  gene2life-hadoop-cluster:3.4.3
```

Validate Docker-only CPU pinning:

```bash
./scripts/validate-docker-node-pinning.sh \
  ./config/clusters-z4-g5-paper-sweep.csv \
  gene2life-java:latest
```

## Practical Notes

- Do not compare schedulers from a single back-to-back run; use `compare`.
- `compare` alternates scheduler order to reduce warm-cache and JVM bias.
- Keep the dataset fixed while changing only scheduler or node count.
- If the host starts paging, reduce dataset size before increasing heap.
- The project cleanup scripts remove only repo-owned Docker runtime, not host Hadoop daemons.
- If you want to reuse the Hadoop cluster across multiple benchmarks, set `HADOOP_KEEP_CLUSTER=true`.

## Interpretation

Results from this server are meaningful for relative `WSH` versus `HEFT` behavior under the repo's reproduction model. They are closest to the paper when using the paper-style profile and Hadoop execution, but they are still produced on one physical host rather than a multi-VM Hi-WAY deployment.
