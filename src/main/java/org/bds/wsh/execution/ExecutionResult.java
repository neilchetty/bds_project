package org.bds.wsh.execution;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Aggregates real execution results for an entire workflow run.
 * Provides actual wall-clock makespan and per-task timing.
 */
public final class ExecutionResult {
    private final String algorithm;
    private final String workflowName;
    private final int nodeCount;
    private final Map<String, TaskExecutionResult> taskResults;
    private final double realMakespanSeconds;
    private final long executionStartEpochMs;
    private final long executionFinishEpochMs;

    public ExecutionResult(String algorithm, String workflowName, int nodeCount,
                           Map<String, TaskExecutionResult> taskResults,
                           long executionStartEpochMs, long executionFinishEpochMs) {
        this.algorithm = algorithm;
        this.workflowName = workflowName;
        this.nodeCount = nodeCount;
        this.taskResults = Collections.unmodifiableMap(new LinkedHashMap<>(taskResults));
        this.executionStartEpochMs = executionStartEpochMs;
        this.executionFinishEpochMs = executionFinishEpochMs;
        this.realMakespanSeconds = (executionFinishEpochMs - executionStartEpochMs) / 1000.0;
    }

    public String algorithm() { return algorithm; }
    public String workflowName() { return workflowName; }
    public int nodeCount() { return nodeCount; }
    public Map<String, TaskExecutionResult> taskResults() { return taskResults; }
    public double realMakespanSeconds() { return realMakespanSeconds; }
    public long executionStartEpochMs() { return executionStartEpochMs; }
    public long executionFinishEpochMs() { return executionFinishEpochMs; }
}
