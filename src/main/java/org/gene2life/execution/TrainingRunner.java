package org.gene2life.execution;

import org.gene2life.model.ClusterProfile;
import org.gene2life.model.JobId;
import org.gene2life.model.NodeProfile;
import org.gene2life.scheduler.TrainingBenchmarks;
import org.gene2life.task.TaskExecutor;
import org.gene2life.task.TaskInputs;
import org.gene2life.task.TaskResult;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TrainingRunner {
    private final Map<JobId, TaskExecutor> executors;
    private final ExecutionMode executionMode;
    private final DockerNodePool dockerNodePool;
    private final int warmupRuns;
    private final int measurementRuns;

    public TrainingRunner(
            Map<JobId, TaskExecutor> executors,
            ExecutionMode executionMode,
            DockerNodePool dockerNodePool,
            int warmupRuns,
            int measurementRuns) {
        this.executors = executors;
        this.executionMode = executionMode;
        this.dockerNodePool = dockerNodePool;
        this.warmupRuns = Math.max(0, warmupRuns);
        this.measurementRuns = Math.max(1, measurementRuns);
    }

    public TrainingBenchmarks benchmark(Path dataRoot, List<ClusterProfile> clusters) throws Exception {
        WorkflowExecutor.ensureTrainingParents(dataRoot);
        Map<JobId, Map<String, Long>> durations = new EnumMap<>(JobId.class);
        Map<JobId, String> classifications = new EnumMap<>(JobId.class);
        for (ClusterProfile cluster : clusters) {
            NodeProfile node = cluster.firstNode();
            for (JobId jobId : JobId.values()) {
                Path outputDir = dataRoot.resolve("training/generated").resolve(jobId.cliName());
                Files.createDirectories(outputDir);
                TaskInputs inputs = new TaskInputs(
                        WorkflowExecutor.trainingPrimaryInput(dataRoot, jobId),
                        WorkflowExecutor.trainingSecondaryInput(dataRoot, jobId),
                        outputDir);
                for (int warmup = 0; warmup < warmupRuns; warmup++) {
                    executeTrainingSample(jobId, node, inputs);
                }
                List<Long> measuredDurations = new ArrayList<>();
                TaskResult result = null;
                for (int sample = 0; sample < measurementRuns; sample++) {
                    long start = System.currentTimeMillis();
                    result = executeTrainingSample(jobId, node, inputs);
                    long finish = System.currentTimeMillis();
                    measuredDurations.add(Math.max(1L, finish - start));
                }
                durations.computeIfAbsent(jobId, ignored -> new HashMap<>()).put(cluster.clusterId(), median(measuredDurations));
                mirrorTrainingOutput(jobId, result.outputPath(), dataRoot);
            }
        }
        for (JobId jobId : JobId.values()) {
            List<Long> values = clusters.stream().map(cluster -> durations.get(jobId).get(cluster.clusterId())).sorted().toList();
            long min = values.get(0);
            long max = values.get(values.size() - 1);
            classifications.put(jobId, max <= Math.max(1L, (long) (min * 1.12)) ? "io" : "compute");
        }
        return new TrainingBenchmarks(durations, classifications, warmupRuns, measurementRuns);
    }

    private TaskResult executeTrainingSample(JobId jobId, NodeProfile node, TaskInputs inputs) throws Exception {
        return executionMode == ExecutionMode.DOCKER
                ? dockerNodePool.execute(node, jobId, inputs)
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

    private void mirrorTrainingOutput(JobId jobId, Path source, Path dataRoot) throws Exception {
        Path target = switch (jobId) {
            case BLAST1 -> dataRoot.resolve("training/generated/blast1/hits.tsv");
            case BLAST2 -> dataRoot.resolve("training/generated/blast2/hits.tsv");
            case CLUSTALW1 -> dataRoot.resolve("training/generated/clustalw1/alignment.tsv");
            case CLUSTALW2 -> dataRoot.resolve("training/generated/clustalw2/alignment.tsv");
            case DNAPARS -> dataRoot.resolve("training/generated/dnapars/dna-tree.newick");
            case PROTPARS -> dataRoot.resolve("training/generated/protpars/protein-tree.newick");
            case DRAWGRAM1 -> dataRoot.resolve("training/generated/drawgram1/tree.txt");
            case DRAWGRAM2 -> dataRoot.resolve("training/generated/drawgram2/tree.txt");
        };
        Files.copy(source, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }
}
