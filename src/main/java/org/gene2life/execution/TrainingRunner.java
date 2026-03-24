package org.gene2life.execution;

import org.gene2life.model.ClusterProfile;
import org.gene2life.model.JobDefinition;
import org.gene2life.model.NodeProfile;
import org.gene2life.scheduler.DurationModel;
import org.gene2life.scheduler.TrainingBenchmarks;
import org.gene2life.task.TaskExecutor;
import org.gene2life.task.TaskInputs;
import org.gene2life.task.TaskResult;
import org.gene2life.workflow.WorkflowSpec;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TrainingRunner {
    private static final long MEASUREMENT_NOISE_FLOOR_MS = 2_000L;

    private final WorkflowSpec workflowSpec;
    private final Map<String, TaskExecutor> executors;
    private final ExecutionMode executionMode;
    private final DockerNodePool dockerNodePool;
    private final int warmupRuns;
    private final int measurementRuns;

    public TrainingRunner(
            WorkflowSpec workflowSpec,
            Map<String, TaskExecutor> executors,
            ExecutionMode executionMode,
            DockerNodePool dockerNodePool,
            int warmupRuns,
            int measurementRuns) {
        this.workflowSpec = workflowSpec;
        this.executors = executors;
        this.executionMode = executionMode;
        this.dockerNodePool = dockerNodePool;
        this.warmupRuns = Math.max(0, warmupRuns);
        this.measurementRuns = Math.max(1, measurementRuns);
    }

    public TrainingBenchmarks benchmark(Path dataRoot, List<ClusterProfile> clusters) throws Exception {
        workflowSpec.ensureTrainingParents(dataRoot);
        Map<String, Map<String, Long>> durations = new HashMap<>();
        Map<String, String> classifications = new HashMap<>();
        for (ClusterProfile cluster : clusters) {
            NodeProfile node = cluster.firstNode();
            for (JobDefinition job : workflowSpec.definition().trainingRepresentativeJobs()) {
                TaskInputs inputs = workflowSpec.resolveTrainingInputs(job.id(), dataRoot);
                for (int warmup = 0; warmup < warmupRuns; warmup++) {
                    executeTrainingSample(job.id(), node, inputs);
                }
                List<Long> measuredDurations = new ArrayList<>();
                for (int sample = 0; sample < measurementRuns; sample++) {
                    long start = System.currentTimeMillis();
                    executeTrainingSample(job.id(), node, inputs);
                    long finish = System.currentTimeMillis();
                    measuredDurations.add(Math.max(1L, finish - start));
                }
                durations.computeIfAbsent(job.trainingProfileKey(), ignored -> new HashMap<>()).put(cluster.clusterId(), median(measuredDurations));
            }
        }
        for (JobDefinition job : workflowSpec.definition().trainingRepresentativeJobs()) {
            Map<String, Long> corrected = correctedDurations(job, clusters, durations.get(job.trainingProfileKey()));
            durations.put(job.trainingProfileKey(), corrected);
            classifications.put(job.trainingProfileKey(), job.taskType().defaultClassification());
        }
        return new TrainingBenchmarks(durations, classifications, warmupRuns, measurementRuns);
    }

    private TaskResult executeTrainingSample(String jobId, NodeProfile node, TaskInputs inputs) throws Exception {
        return executionMode == ExecutionMode.DOCKER
                ? dockerNodePool.execute(workflowSpec, node, jobId, inputs)
                : executors.get(jobId).execute(inputs, node);
    }

    private long median(List<Long> values) {
        List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int middle = sorted.size() / 2;
        if (sorted.size() % 2 == 1) {
            return sorted.get(middle);
        }
        return Math.round((sorted.get(middle - 1) + sorted.get(middle)) / 2.0);
    }

    private Map<String, Long> correctedDurations(
            JobDefinition job,
            List<ClusterProfile> clusters,
            Map<String, Long> measuredDurations) {
        long maxMeasured = measuredDurations.values().stream().mapToLong(Long::longValue).max().orElse(0L);
        if (maxMeasured > MEASUREMENT_NOISE_FLOOR_MS) {
            return measuredDurations;
        }
        Map<String, Long> fallback = new HashMap<>();
        for (ClusterProfile cluster : clusters) {
            fallback.put(cluster.clusterId(), DurationModel.estimateDuration(job, cluster.firstNode()));
        }
        return fallback;
    }
}
