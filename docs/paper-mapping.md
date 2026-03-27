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

## Execution Mapping

- `local`: one host JVM executes the assigned task directly
- `docker`: each logical node runs as a Docker container and jobs execute with `docker exec`
- `hadoop`: workflow inputs are mirrored into HDFS, each workflow task is submitted as a one-task MapReduce job on the Docker Hadoop cluster, and outputs are copied back into the existing local run-report layout

## Cluster Mapping

- `config/clusters-paper.csv` uses the literal paper-style per-node capacities
- `config/clusters-z4-g5-paper-sweep.csv` maps that same heterogeneous pattern onto the Ubuntu server with CPU pinning
- `config/clusters-z4-g5-paper-sweep-scaled.csv` keeps the same four-subcluster pattern but increases logical-node capacity

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

- scheduling and workflow orchestration are implemented in this repository rather than reusing the original Hi-WAY codebase
- Hadoop mode uses a Dockerized single-host cluster, not separate physical VMs
- Docker-isolated logical nodes share one host kernel
- Java tasks approximate the original bioinformatics tools instead of invoking the original native binaries

So the project now matches the paper's workflow structures, scheduler comparison, and Hadoop/HDFS/YARN-style execution path much more closely, but it is still a paper-inspired reproduction rather than a byte-for-byte recreation of the original software stack.
