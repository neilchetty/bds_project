package org.bds.wsh.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class ScheduleResult {
    private final String algorithm;
    private final String workflowName;
    private final Map<String, ScheduledTask> scheduledTasks;
    private final double makespanSeconds;

    public ScheduleResult(String algorithm, String workflowName, Map<String, ScheduledTask> scheduledTasks) {
        this.algorithm = algorithm;
        this.workflowName = workflowName;
        this.scheduledTasks = Collections.unmodifiableMap(new LinkedHashMap<>(scheduledTasks));
        this.makespanSeconds = scheduledTasks.values().stream()
                .mapToDouble(ScheduledTask::finishSeconds)
                .max()
                .orElse(0.0);
    }

    public String algorithm() { return algorithm; }
    public String workflowName() { return workflowName; }
    public Map<String, ScheduledTask> scheduledTasks() { return scheduledTasks; }
    public double makespanSeconds() { return makespanSeconds; }
}
