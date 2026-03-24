package org.gene2life.scheduler;

import org.gene2life.model.ClusterProfile;
import org.gene2life.model.JobDefinition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TrainingBenchmarks {
    private final Map<String, Map<String, Long>> durationsByProfileAndCluster;
    private final Map<String, String> classifications;
    private final int warmupRuns;
    private final int measurementRuns;

    public TrainingBenchmarks(
            Map<String, Map<String, Long>> durationsByProfileAndCluster,
            Map<String, String> classifications,
            int warmupRuns,
            int measurementRuns) {
        this.durationsByProfileAndCluster = durationsByProfileAndCluster;
        this.classifications = classifications;
        this.warmupRuns = warmupRuns;
        this.measurementRuns = measurementRuns;
    }

    public long duration(JobDefinition job, String clusterId) {
        return durationsByProfileAndCluster.get(job.trainingProfileKey()).get(clusterId);
    }

    public double averageDuration(JobDefinition job) {
        return durationsByProfileAndCluster.get(job.trainingProfileKey()).values().stream().mapToLong(Long::longValue).average().orElse(0);
    }

    public String classification(JobDefinition job) {
        return classifications.getOrDefault(job.trainingProfileKey(), job.taskType().defaultClassification());
    }

    public boolean hasMeasurements(JobDefinition job) {
        return durationsByProfileAndCluster.containsKey(job.trainingProfileKey()) && !durationsByProfileAndCluster.get(job.trainingProfileKey()).isEmpty();
    }

    public int warmupRuns() {
        return warmupRuns;
    }

    public int measurementRuns() {
        return measurementRuns;
    }

    public List<String> sortedClusters(JobDefinition job, List<ClusterProfile> clusters) {
        if (!hasMeasurements(job)) {
            return staticClusterOrder(job, clusters);
        }
        String classification = classification(job);
        return clusters.stream()
                .sorted((left, right) -> {
                    long leftDuration = duration(job, left.clusterId());
                    long rightDuration = duration(job, right.clusterId());
                    if (effectivelyEqual(leftDuration, rightDuration, classification)) {
                        if ("io".equals(classification)) {
                            return Integer.compare(left.maxCpuThreads(), right.maxCpuThreads());
                        }
                        return Integer.compare(right.maxCpuThreads(), left.maxCpuThreads());
                    }
                    int compare = Long.compare(leftDuration, rightDuration);
                    if (compare != 0) {
                        return compare;
                    }
                    return Integer.compare(left.clusterId().hashCode(), right.clusterId().hashCode());
                })
                .map(ClusterProfile::clusterId)
                .toList();
    }

    private List<String> staticClusterOrder(JobDefinition job, List<ClusterProfile> clusters) {
        return clusters.stream()
                .sorted((left, right) -> {
                    if ("io".equals(job.taskType().defaultClassification())) {
                        return Integer.compare(left.maxCpuThreads(), right.maxCpuThreads());
                    }
                    return Integer.compare(right.maxCpuThreads(), left.maxCpuThreads());
                })
                .map(ClusterProfile::clusterId)
                .toList();
    }

    private boolean effectivelyEqual(long leftDuration, long rightDuration, String classification) {
        long minimum = Math.max(1L, Math.min(leftDuration, rightDuration));
        double threshold = "io".equals(classification) ? 0.25 : 0.15;
        return Math.abs(leftDuration - rightDuration) <= Math.max(25L, Math.round(minimum * threshold));
    }

    public static TrainingBenchmarks empty() {
        return new TrainingBenchmarks(new HashMap<>(), new HashMap<>(), 0, 0);
    }
}
