package org.bds.wsh.scheduler;

public record ClusterTrainingProfile(
        String clusterId,
        double cpuTrainingSeconds,
        double ioTrainingSeconds,
        String containerName
) {
    public ClusterTrainingProfile {
        if (clusterId == null || clusterId.isBlank()) {
            throw new IllegalArgumentException("clusterId must not be blank.");
        }
        if (cpuTrainingSeconds <= 0.0 || ioTrainingSeconds <= 0.0) {
            throw new IllegalArgumentException("Training measurements must be positive.");
        }
        containerName = containerName == null ? clusterId : containerName;
    }
}
