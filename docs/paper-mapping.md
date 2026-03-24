# Paper To Code Mapping

## Source Paper

`Main-Paper.pdf` evaluates `WSH` against `HEFT` on three workflows:

- `gene2life`
- `avianflu_small`
- `epigenomics`

This repository now implements all three workflow structures in Java.

## Workflow Mapping

### `gene2life`

Paper structure:

`blast1, blast2 -> clustalw1, clustalw2 -> dnapars, protpars -> drawgram1, drawgram2`

Code:

- workflow spec: `src/main/java/org/gene2life/workflow/Gene2LifeWorkflowSpec.java`
- tasks: `BLAST`, `CLUSTAL`, `DNAPARS`, `PROTPARS`, `DRAWGRAM`

### `avianflu_small`

Paper structure:

- several preprocessing stages
- one `AutoGrid` stage
- a large `Autodock` fanout

Code mapping used here:

- `prepare-receptor`
- `prepare-gpf`
- `prepare-dpf`
- `auto-grid`
- `autodock-001 ... autodock-100`

This preserves the paper's 104-job scale while keeping the data products small.

### `epigenomics`

Paper structure:

- one split stage
- repeated chunk-processing stages
- merge/index/final summary stages

Code mapping used here:

- `fastqSplit`
- `filterContams-001 ... 024`
- `sol2sanger-001 ... 024`
- `fastq2bfq-001 ... 024`
- `map-001 ... 024`
- `mapMerge`
- `maqIndex`
- `pileup`

This preserves the paper's 100-job scale.

## Scheduling Mapping

### `HEFT`

- upward rank is computed from a static resource-aware duration model
- tasks are assigned to the node with minimum earliest finish time
- communication cost is ignored, matching the paper's assumption

### `WSH`

- training tasks run on the first node of each cluster
- cluster ordering is derived from measured training runtimes
- near-equal runtimes are classified as more IO-oriented
- node expansion follows the paper's cluster-by-cluster idea

The implementation stabilizes training with warmup runs plus repeated measured runs and uses the median observed duration per cluster/job profile.

## Cluster Mapping

- `config/clusters-paper.csv` uses the literal paper-style per-node capacities
- `config/clusters-z4-g5-paper-sweep.csv` maps that same heterogeneous pattern onto the Ubuntu server with CPU pinning
- `config/clusters-z4-g5-paper-sweep-scaled.csv` keeps the same four-subcluster pattern but increases logical-node capacity
- `docs/hadoop-cluster.md` provisions those logical nodes as separate Hadoop worker containers, applies YARN node labels per homogeneous subcluster, and recreates the worker cluster per sweep target so smaller experiments do not accidentally run against extra online workers

The paper reports total virtual-machine counts of `4/7/10/13`, which correspond to `1 master + 3/6/9/12 workers`. The new orchestration script follows that mapping directly.

## Data Mapping

This project uses generated but domain-shaped datasets instead of no-op simulation:

- `gene2life`: FASTA query and reference files
- `avianflu_small`: receptor/grid/ligand tabular files
- `epigenomics`: FASTQ reads, reference FASTA, contaminant motifs

These datasets are intended to preserve workflow pressure and file-processing behavior, not biological exactness.

## Metric Mapping

- `makespan`
  actual wall-clock difference between first job start and last job finish
- `speedup`
  sequential runtime sum divided by makespan
- `SLR`
  makespan divided by a scheduler-independent modeled critical-path lower bound derived from the fastest logical node available for each workflow task

## What Still Differs From The Paper

- execution now uses Hadoop/HDFS for stage submission and data movement, and the paper-closer path provisions separate Hadoop worker containers with YARN node labels, but it is still not native Hi-WAY
- Docker-isolated logical nodes remain an optional fallback and share one host kernel
- Java tasks approximate the original bioinformatics tools instead of invoking the original native binaries

So the project now matches the workflow structures and the Big Data execution layer much more closely, but it is still a paper-inspired reproduction rather than a byte-for-byte recreation of the original software stack.
