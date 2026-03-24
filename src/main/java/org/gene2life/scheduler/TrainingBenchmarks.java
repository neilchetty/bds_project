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
            return clusters.stream()
                    .sorted((left, right) -> Integer.compare(right.maxCpuThreads(), left.maxCpuThreads()))
                    .map(ClusterProfile::clusterId)
                    .toList();
        }
        return clusters.stream()
                .sorted((left, right) -> {
                    long leftDuration = duration(job, left.clusterId());
                    long rightDuration = duration(job, right.clusterId());
                    int compare = Long.compare(leftDuration, rightDuration);
                    if (compare != 0) {
                        return compare;
                    }
                    if ("io".equals(classification(job))) {
                        return Integer.compare(left.maxCpuThreads(), right.maxCpuThreads());
                    }
                    return Integer.compare(right.maxCpuThreads(), left.maxCpuThreads());
                })
                .map(ClusterProfile::clusterId)
                .toList();
    }

    public static TrainingBenchmarks empty() {
        return new TrainingBenchmarks(new HashMap<>(), new HashMap<>(), 0, 0);
    }
}
