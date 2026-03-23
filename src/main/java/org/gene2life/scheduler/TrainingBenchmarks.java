package org.gene2life.scheduler;

import org.gene2life.model.ClusterProfile;
import org.gene2life.model.JobId;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public final class TrainingBenchmarks {
    private final Map<JobId, Map<String, Long>> durationsByJobAndCluster;
    private final Map<JobId, String> classifications;

    public TrainingBenchmarks(Map<JobId, Map<String, Long>> durationsByJobAndCluster, Map<JobId, String> classifications) {
        this.durationsByJobAndCluster = durationsByJobAndCluster;
        this.classifications = classifications;
    }

    public long duration(JobId jobId, String clusterId) {
        return durationsByJobAndCluster.get(jobId).get(clusterId);
    }

    public double averageDuration(JobId jobId) {
        return durationsByJobAndCluster.get(jobId).values().stream().mapToLong(Long::longValue).average().orElse(0);
    }

    public String classification(JobId jobId) {
        return classifications.getOrDefault(jobId, "compute");
    }

    public boolean hasMeasurements(JobId jobId) {
        return durationsByJobAndCluster.containsKey(jobId) && !durationsByJobAndCluster.get(jobId).isEmpty();
    }

    public List<String> sortedClusters(JobId jobId, List<ClusterProfile> clusters) {
        if (!hasMeasurements(jobId)) {
            return clusters.stream()
                    .sorted((left, right) -> Integer.compare(right.maxCpuThreads(), left.maxCpuThreads()))
                    .map(ClusterProfile::clusterId)
                    .toList();
        }
        return clusters.stream()
                .sorted((left, right) -> {
                    long leftDuration = duration(jobId, left.clusterId());
                    long rightDuration = duration(jobId, right.clusterId());
                    int compare = Long.compare(leftDuration, rightDuration);
                    if (compare != 0) {
                        return compare;
                    }
                    if ("io".equals(classification(jobId))) {
                        return Integer.compare(left.maxCpuThreads(), right.maxCpuThreads());
                    }
                    return Integer.compare(right.maxCpuThreads(), left.maxCpuThreads());
                })
                .map(ClusterProfile::clusterId)
                .toList();
    }

    public static TrainingBenchmarks empty() {
        return new TrainingBenchmarks(new EnumMap<>(JobId.class), new EnumMap<>(JobId.class));
    }
}
