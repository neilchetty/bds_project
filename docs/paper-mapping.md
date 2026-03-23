# Paper To Code Mapping

## Source Paper

`Main-Paper.pdf` describes a workflow scheduler named `WSH` and evaluates it against `HEFT` on three workflows. This repository currently implements only the `gene2life` workflow.

## Gene2Life DAG

The paper's Fig. 2 defines the `gene2life` structure as:

`blast1, blast2 -> clustalw1, clustalw2 -> dnapars, protpars -> drawgram1, drawgram2`

This repository preserves those exact jobs and dependencies.

## Job Mapping

- `blast1` and `blast2`
  Reads the generated query DNA plus one reference shard and emits top-hit tables.
- `clustalw1` and `clustalw2`
  Builds consensus alignments from blast hits.
- `dnapars`
  Builds a DNA-oriented phylogenetic tree from `clustalw1`.
- `protpars`
  Translates consensus DNA into proteins and builds a protein-oriented phylogenetic tree from `clustalw2`.
- `drawgram1` and `drawgram2`
  Materializes tree artifacts in text and DOT formats.

## Scheduling Mapping

- `HEFT`
  Uses upward ranking based on average training-task duration across clusters and assigns each job to the node with minimum earliest finish time.
- `WSH`
  Runs a training task on the first node of every cluster, sorts clusters by observed finish time per job, classifies near-equal jobs as more IO-intensive, and expands candidate nodes cluster by cluster.

## Big-Data Mapping

The figure in the paper shows small communication payloads. That is not enough for a meaningful big-data execution environment, so this implementation keeps the workflow structure and scheduler behavior while scaling the actual input files through configurable data generation.

## What Is Not Yet Implemented

- Avianflu_small
- Epigenomics
- Native Hadoop or Hi-WAY integration
- Per-node Docker orchestration
