package org.gene2life.execution;

import org.gene2life.model.JobId;

import java.nio.file.Path;

public final class JobOutputs {
    private JobOutputs() {
    }

    public static Path outputPath(JobId jobId, Path outputDirectory) {
        return switch (jobId) {
            case BLAST1, BLAST2 -> outputDirectory.resolve("hits.tsv");
            case CLUSTALW1, CLUSTALW2 -> outputDirectory.resolve("alignment.tsv");
            case DNAPARS -> outputDirectory.resolve("dna-tree.newick");
            case PROTPARS -> outputDirectory.resolve("protein-tree.newick");
            case DRAWGRAM1, DRAWGRAM2 -> outputDirectory.resolve("tree.txt");
        };
    }

    public static String outputDescription(JobId jobId) {
        return switch (jobId) {
            case BLAST1, BLAST2 -> "blast hits";
            case CLUSTALW1, CLUSTALW2 -> "consensus DNA alignments";
            case DNAPARS -> "DNA phylogenetic tree";
            case PROTPARS -> "protein phylogenetic tree";
            case DRAWGRAM1, DRAWGRAM2 -> "drawgram text tree";
        };
    }
}
