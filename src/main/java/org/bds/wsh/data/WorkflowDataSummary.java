package org.bds.wsh.data;

import java.util.Locale;

import org.bds.wsh.model.Task;
import org.bds.wsh.model.Workflow;

/**
 * Computes and displays the big-data statistics of a workflow.
 *
 * <p>This is where the "big data" lives: every workflow task processes and
 * transfers data at a scale typical of scientific computing pipelines
 * (hundreds of megabytes to terabytes per run).  This class makes those
 * volumes explicit so that users can see exactly how much data flows through
 * each pipeline stage.
 */
public final class WorkflowDataSummary {

    private static final double MB = 1024.0 * 1024.0;
    private static final double GB = 1024.0 * MB;
    private static final double TB = 1024.0 * GB;

    /** Threshold above which a workflow is classified as "Big Data" (1 GB). */
    private static final double BIG_DATA_THRESHOLD_BYTES = GB;

    private final Workflow workflow;

    public WorkflowDataSummary(Workflow workflow) {
        this.workflow = workflow;
    }

    /** Total number of tasks in the workflow. */
    public int taskCount() {
        return workflow.tasks().size();
    }

    /** Total compute time summed across all tasks (seconds). */
    public double totalWorkloadSeconds() {
        return workflow.tasks().values().stream()
                .mapToDouble(Task::workloadSeconds)
                .sum();
    }

    /**
     * Total data transferred across all edges in the workflow (bytes).
     * This is the sum of every inter-task communication volume and is the
     * primary "big data" metric of the pipeline.
     */
    public double totalEdgeDataBytes() {
        return workflow.tasks().values().stream()
                .flatMap(t -> t.edgeDataBytes().values().stream())
                .mapToDouble(Double::doubleValue)
                .sum();
    }

    /** Largest single edge transfer (bytes). */
    public double maxEdgeDataBytes() {
        return workflow.tasks().values().stream()
                .flatMap(t -> t.edgeDataBytes().values().stream())
                .mapToDouble(Double::doubleValue)
                .max()
                .orElse(0.0);
    }

    /** Average edge transfer size (bytes), or 0 if there are no edges. */
    public double avgEdgeDataBytes() {
        long edgeCount = workflow.tasks().values().stream()
                .mapToLong(t -> t.edgeDataBytes().size())
                .sum();
        return edgeCount == 0 ? 0.0 : totalEdgeDataBytes() / edgeCount;
    }

    /** Number of dependency edges in the workflow DAG. */
    public long edgeCount() {
        return workflow.tasks().values().stream()
                .mapToLong(t -> t.edgeDataBytes().size())
                .sum();
    }

    /**
     * Returns {@code true} when the total edge data exceeds the 1-GB threshold
     * used to classify the pipeline as a "Big Data" workflow.
     */
    public boolean isBigData() {
        return totalEdgeDataBytes() >= BIG_DATA_THRESHOLD_BYTES;
    }

    /**
     * Formats the data volume as a human-readable string
     * (e.g. "1.23 TB", "456.78 GB", "789.01 MB").
     */
    public static String formatBytes(double bytes) {
        if (bytes >= TB) {
            return String.format(Locale.US, "%.2f TB", bytes / TB);
        } else if (bytes >= GB) {
            return String.format(Locale.US, "%.2f GB", bytes / GB);
        } else {
            return String.format(Locale.US, "%.2f MB", bytes / MB);
        }
    }

    /**
     * Returns a multi-line human-readable summary of the workflow's big-data
     * characteristics, suitable for printing to the console.
     */
    public String toDisplayString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("  Workflow       : %s%n", workflow.name()));
        sb.append(String.format("  Tasks          : %d%n", taskCount()));
        sb.append(String.format("  DAG edges      : %d%n", edgeCount()));
        sb.append(String.format("  Total CPU work : %.0f seconds (simulated)%n", totalWorkloadSeconds()));
        sb.append(String.format("  Total data     : %s%n", formatBytes(totalEdgeDataBytes())));
        sb.append(String.format("  Max edge data  : %s%n", formatBytes(maxEdgeDataBytes())));
        sb.append(String.format("  Avg edge data  : %s%n", formatBytes(avgEdgeDataBytes())));
        sb.append(String.format("  Big Data?      : %s%n", isBigData() ? "YES ✓" : "no (< 1 GB)"));
        return sb.toString();
    }
}
