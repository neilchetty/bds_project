# Server Tuning

## Target Host

This guide targets the reported Ubuntu 24.04 workstation:

- 36 hardware threads
- 62.44 GiB RAM
- local ext4 storage
- Docker available

## General Defaults

- Run heavy benchmarks on the Ubuntu server, not the Mac Mini.
- Keep the workspace on local disk.
- Use Docker execution for scheduler studies:
  - `EXECUTOR=docker`
- Start with:
  - `PROFILE=medium`
  - `COMPARE_ROUNDS=4`
  - `TRAINING_WARMUP_RUNS=1`
  - `TRAINING_MEASURE_RUNS=3`
  - `GENE2LIFE_JAVA_OPTS="-Xms4g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication"`

## Cluster Profiles

Use one of these profiles depending on the experiment goal:

- `config/clusters-z4-g5.csv`
  tuned host-usage profile for the Ubuntu workstation
- `config/clusters-z4-g5-paper-sweep.csv`
  literal paper-style 12-node heterogeneous sweep on the same host
- `config/clusters-z4-g5-paper-sweep-scaled.csv`
  same four-subcluster structure with stronger logical nodes

## Workflow-Specific Dataset Defaults

The benchmark launcher is now workflow-aware:

- `gene2life`
  - `QUERY_COUNT`
  - `REFERENCE_RECORDS_PER_SHARD`
  - `SEQUENCE_LENGTH`
- `avianflu_small`
  - `RECEPTOR_FEATURE_COUNT`
  - `LIGAND_ATOM_COUNT`
- `epigenomics`
  - `READS_PER_SPLIT`
  - `READ_LENGTH`
  - `REFERENCE_RECORD_COUNT`

The defaults intentionally keep `avianflu_small` and `epigenomics` in the same rough runtime class as `gene2life`, not heavier.

## Commands

Generate a cluster profile from the host itself:

```bash
chmod +x ./scripts/generate-cluster-config.sh
./scripts/generate-cluster-config.sh ./config/clusters-autogen.csv
```

Run the default `gene2life` benchmark:

```bash
chmod +x ./scripts/server-benchmark.sh
PROFILE=medium \
CLUSTER_CONFIG=./config/clusters-z4-g5.csv \
EXECUTOR=docker \
./scripts/server-benchmark.sh
```

Run `avianflu_small` through the same harness:

```bash
WORKFLOW=avianflu_small \
PROFILE=medium \
CLUSTER_CONFIG=./config/clusters-z4-g5-paper-sweep.csv \
COMPARE_ROUNDS=4 \
EXECUTOR=docker \
./scripts/server-benchmark.sh
```

Run `epigenomics`:

```bash
WORKFLOW=epigenomics \
PROFILE=medium \
CLUSTER_CONFIG=./config/clusters-z4-g5-paper-sweep.csv \
COMPARE_ROUNDS=4 \
EXECUTOR=docker \
./scripts/server-benchmark.sh
```

Run the paper-style node-count sweep for `gene2life`:

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

## Practical Notes

- For fair scheduler comparison, do not rely on a single back-to-back run.
- `compare` alternates scheduler order across rounds to reduce warm-cache and JVM bias.
- Use the same generated dataset while changing only scheduler or node count.
- If memory pressure appears, reduce workflow-specific dataset size before increasing JVM heap.
- If Docker runs fail unexpectedly, rebuild the image with `./scripts/build-image.sh`.
- Persistent logical node containers are created per scheduler run and cleaned up afterwards.

## Interpretation Notes

- Results on the Ubuntu host are meaningful for relative scheduler behavior on your server.
- They are not identical to a real multi-VM Hi-WAY/Hadoop deployment.
- The paper-style sweep profile is the closest configuration in this repository when the goal is structural similarity to the paper.
