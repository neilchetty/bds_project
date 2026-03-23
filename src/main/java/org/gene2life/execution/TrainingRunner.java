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
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TrainingRunner {
    private final Map<JobId, TaskExecutor> executors;
    private final ExecutionMode executionMode;
    private final DockerNodePool dockerNodePool;

    public TrainingRunner(Map<JobId, TaskExecutor> executors, ExecutionMode executionMode, DockerNodePool dockerNodePool) {
        this.executors = executors;
        this.executionMode = executionMode;
        this.dockerNodePool = dockerNodePool;
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
                long start = System.currentTimeMillis();
                TaskResult result = executionMode == ExecutionMode.DOCKER
                        ? dockerNodePool.execute(node, jobId, inputs)
                        : executors.get(jobId).execute(inputs, node);
                long finish = System.currentTimeMillis();
                durations.computeIfAbsent(jobId, ignored -> new HashMap<>()).put(cluster.clusterId(), Math.max(1L, finish - start));
                mirrorTrainingOutput(jobId, result.outputPath(), dataRoot);
            }
        }
        for (JobId jobId : JobId.values()) {
            List<Long> values = clusters.stream().map(cluster -> durations.get(jobId).get(cluster.clusterId())).sorted().toList();
            long min = values.get(0);
            long max = values.get(values.size() - 1);
            classifications.put(jobId, max <= Math.max(1L, (long) (min * 1.12)) ? "io" : "compute");
        }
        return new TrainingBenchmarks(durations, classifications);
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
