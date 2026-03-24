# Server Tuning

## Target Host

This guide targets the reported Ubuntu 24.04 workstation:

- 36 hardware threads
- 62.44 GiB RAM
- local ext4 storage
- Docker available
- Hadoop/HDFS/YARN expected for the primary execution mode

## Recommended Execution Mode

For the closest paper-style execution on one physical server:

- run the multi-worker Hadoop cluster in `docs/hadoop-cluster.md`
- expose one worker container per selected logical node
- enable `HADOOP_ENABLE_NODE_LABELS=true` so scheduler-assigned subclusters map to YARN node labels
- prefer `scripts/run-paper-hadoop-sweeps.sh` over manually reusing one large cluster across all node counts

The host-native single-node Hadoop installation is still useful for debugging, but it is less paper-faithful than the multi-worker containerized cluster.

## General Defaults

- Run heavy benchmarks on the Ubuntu server, not the Mac Mini.
- Keep the workspace on local disk.
- Use Hadoop execution for scheduler studies:
  - `EXECUTOR=hadoop`
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

The benchmark scripts also reuse datasets by default:

- `DATA_ROOT` can point to a shared per-workflow dataset directory
- `REUSE_DATA=true` skips regeneration when the requested generation parameters match `DATA_ROOT/.generation-metadata.env`
- this is the preferred mode for paper-style node-count sweeps because all node counts then run against the same workflow input

## Commands

Generate a cluster profile from the host itself:

```bash
chmod +x ./scripts/generate-cluster-config.sh
./scripts/generate-cluster-config.sh ./config/clusters-autogen.csv
```

Run the paper-close multi-worker sweep:

```bash
chmod +x ./scripts/run-paper-hadoop-sweeps.sh
PROFILE=medium \
./scripts/run-paper-hadoop-sweeps.sh
```

Run only `avianflu_small`:

```bash
WORKFLOWS=avianflu_small \
PROFILE=medium \
./scripts/run-paper-hadoop-sweeps.sh
```

Run only `epigenomics`:

```bash
WORKFLOWS=epigenomics \
PROFILE=medium \
./scripts/run-paper-hadoop-sweeps.sh
```

Run only `gene2life` with the paper’s total-node counts:

```bash
WORKFLOWS=gene2life \
TOTAL_NODE_COUNTS="4 7 10 13" \
./scripts/run-paper-hadoop-sweeps.sh
```

## Practical Notes

- For fair scheduler comparison, do not rely on a single back-to-back run.
- `compare` alternates scheduler order across rounds to reduce warm-cache and JVM bias.
- Use the same generated dataset while changing only scheduler or node count.
- For the closest paper mapping, change cluster size by recreating the worker cluster for each target rather than leaving extra workers online.
- If memory pressure appears, reduce workflow-specific dataset size before increasing JVM heap.
- If Hadoop submission fails unexpectedly, verify `HADOOP_CONF_DIR`, HDFS reachability, and YARN resource availability first.
- Docker remains available as a secondary backend for local fallback and debugging.
- If you need strict physical-node affinity, add YARN queue or node-label rules outside this repository; the current code requests per-job resources but does not hard-pin containers to hosts by itself.

## Interpretation Notes

- Results on the Ubuntu host are meaningful for relative scheduler behavior on your server.
- This is still not Hi-WAY itself, but the primary execution path now goes through Hadoop/HDFS rather than a custom-only local runtime.
- The paper-style sweep profile is the closest configuration in this repository when the goal is structural similarity to the paper.
