# Server Tuning

## Target Host

This tuning guide targets the reported Ubuntu 24.04 workstation:

- 36 hardware threads
- 62.44 GiB RAM
- local ext4 storage

## Recommended Defaults

- Use [clusters-z4-g5.csv](/Users/neilchetty/Downloads/linux_bds/config/clusters-z4-g5.csv) on this host.
- Use [clusters-z4-g5-paper-sweep.csv](/Users/neilchetty/Downloads/linux_bds/config/clusters-z4-g5-paper-sweep.csv) when you want 12 logical nodes and paper-style node-count sweeps.
- Run benchmark workloads on local disk, not over a network mount.
- Start with:
  - `PROFILE=medium`
  - `GENE2LIFE_JAVA_OPTS="-Xms4g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication"`
  - `EXECUTOR=docker`

## Why This Cluster Layout

The paper assumes heterogeneous subclusters. On a single powerful machine, we reproduce that by partitioning the host into logical nodes with different CPU, IO-buffer, and memory characteristics.

The tuned profile uses:

- `C1`: 2 nodes x 6 threads x 10 GiB
- `C2`: 2 nodes x 5 threads x 8 GiB
- `C3`: 2 nodes x 4 threads x 6 GiB
- `C4`: 2 nodes x 3 threads x 4 GiB

That maps cleanly onto 36 threads and 56 GiB of modeled memory, leaving headroom for the OS, filesystem cache, and the JVM itself.

## Execution Strategy

- Mac Mini: compile and smoke test only.
- Ubuntu server: dataset generation, WSH/HEFT comparison, and any scaling study.

## Commands

Generate an auto-tuned cluster file from the server itself:

```bash
chmod +x ./scripts/generate-cluster-config.sh
./scripts/generate-cluster-config.sh ./config/clusters-autogen.csv
```

Run the tuned benchmark:

```bash
chmod +x ./scripts/server-benchmark.sh
PROFILE=medium \
CLUSTER_CONFIG=./config/clusters-z4-g5.csv \
COMPARE_ROUNDS=4 \
EXECUTOR=docker \
./scripts/server-benchmark.sh
```

Run a larger experiment:

```bash
PROFILE=large \
GENE2LIFE_JAVA_OPTS="-Xms6g -Xmx24g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication" \
WORKSPACE=/data/gene2life-large \
EXECUTOR=docker \
./scripts/server-benchmark.sh
```

Sweep node counts on the same server:

```bash
for nodes in 4 7 10 12; do
  WORKSPACE="/data/gene2life-nodes-${nodes}" \
  CLUSTER_CONFIG=./config/clusters-z4-g5-paper-sweep.csv \
  MAX_NODES="$nodes" \
  COMPARE_ROUNDS=4 \
  EXECUTOR=docker \
  ./scripts/server-benchmark.sh
done
```

## Practical Notes

- If the server starts swapping, reduce `REFERENCE_RECORDS_PER_SHARD` or the JVM heap.
- If CPU is underutilized, increase dataset size first before increasing heap.
- If BLAST stages dominate wall time, keep the larger thread counts in `C1` and `C2`.
- If IO becomes the bottleneck, keep the workspace on the fastest local volume and keep the larger `io_buffer_kb` values for `C1` and `C2`.
- On one server, never trust a single `WSH` then `HEFT` back-to-back run. Alternate order across rounds and compare averages across rounds.
- Build the container image directly with `./scripts/build-image.sh` if you want to validate the Docker environment before running a full benchmark.
- In Docker mode, each logical node is a persistent named container for one scheduler run, and jobs execute inside it with `docker exec`.
