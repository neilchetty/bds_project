package org.bds.wsh.model;

public record ScheduledTask(
        String taskId,
        String nodeId,
        String clusterId,
        double startSeconds,
        double finishSeconds
) {
}
