package org.bds.wsh.execution;

/**
 * Records the actual wall-clock execution result of a single workflow task
 * run on a real Docker container.
 */
public record TaskExecutionResult(
        String taskId,
        String nodeId,
        String containerName,
        long actualStartEpochMs,
        long actualFinishEpochMs,
        double actualDurationSeconds
) {
    public static TaskExecutionResult of(String taskId, String nodeId, String containerName,
                                         long startMs, long finishMs) {
        return new TaskExecutionResult(taskId, nodeId, containerName, startMs, finishMs,
                (finishMs - startMs) / 1000.0);
    }
}
