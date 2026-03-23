package org.gene2life.execution;

import org.gene2life.model.ClusterProfile;
import org.gene2life.model.JobDefinition;
import org.gene2life.model.JobId;
import org.gene2life.model.JobRun;
import org.gene2life.model.NodeProfile;
import org.gene2life.model.PlanAssignment;
import org.gene2life.model.WorkflowDefinition;
import org.gene2life.task.TaskExecutor;
import org.gene2life.task.TaskInputs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public final class WorkflowExecutor {
    private final WorkflowDefinition workflow;
    private final Map<JobId, TaskExecutor> executors;
    private final Map<String, NodeRuntime> runtimes;

    public WorkflowExecutor(WorkflowDefinition workflow, Map<JobId, TaskExecutor> executors, List<ClusterProfile> clusters) {
        this.workflow = workflow;
        this.executors = executors;
        this.runtimes = new HashMap<>();
        for (ClusterProfile cluster : clusters) {
            for (NodeProfile node : cluster.nodes()) {
                runtimes.put(node.nodeId(), new NodeRuntime(node));
            }
        }
    }

    public List<JobRun> execute(Path dataRoot, Path runRoot, List<PlanAssignment> plan) throws Exception {
        Files.createDirectories(runRoot.resolve("jobs"));
        Map<JobId, Future<JobRun>> futures = new HashMap<>();
        List<PlanAssignment> orderedPlan = new ArrayList<>(plan);
        orderedPlan.sort(Comparator.comparingInt(assignment -> assignment.jobId().ordinal()));
        for (PlanAssignment assignment : orderedPlan) {
            waitForDependencies(assignment.jobId(), futures);
            TaskInputs inputs = resolveInputs(assignment.jobId(), dataRoot, runRoot, futures);
            NodeRuntime runtime = runtimes.get(assignment.nodeId());
            futures.put(assignment.jobId(), runtime.submit(assignment, executors.get(assignment.jobId()), inputs));
        }
        List<JobRun> result = new ArrayList<>();
        for (JobDefinition job : workflow.jobs()) {
            result.add(futures.get(job.id()).get());
        }
        result.sort(Comparator.comparingLong(JobRun::actualStartMillis));
        return result;
    }

    public void close() {
        for (NodeRuntime runtime : runtimes.values()) {
            runtime.close();
        }
    }

    private void waitForDependencies(JobId jobId, Map<JobId, Future<JobRun>> futures) throws Exception {
        for (JobId dependency : workflow.job(jobId).dependencies()) {
            futures.get(dependency).get();
        }
    }

    private TaskInputs resolveInputs(
            JobId jobId,
            Path dataRoot,
            Path runRoot,
            Map<JobId, Future<JobRun>> futures) throws Exception {
        Path outputDirectory = runRoot.resolve("jobs").resolve(jobId.cliName());
        Files.createDirectories(outputDirectory);
        return switch (jobId) {
            case BLAST1 -> new TaskInputs(dataRoot.resolve("query.fasta"), dataRoot.resolve("reference-a.fasta"), outputDirectory);
            case BLAST2 -> new TaskInputs(dataRoot.resolve("query.fasta"), dataRoot.resolve("reference-b.fasta"), outputDirectory);
            case CLUSTALW1 -> new TaskInputs(futures.get(JobId.BLAST1).get().outputPath(), null, outputDirectory);
            case CLUSTALW2 -> new TaskInputs(futures.get(JobId.BLAST2).get().outputPath(), null, outputDirectory);
            case DNAPARS -> new TaskInputs(futures.get(JobId.CLUSTALW1).get().outputPath(), null, outputDirectory);
            case PROTPARS -> new TaskInputs(futures.get(JobId.CLUSTALW2).get().outputPath(), null, outputDirectory);
            case DRAWGRAM1 -> new TaskInputs(futures.get(JobId.DNAPARS).get().outputPath(), null, outputDirectory);
            case DRAWGRAM2 -> new TaskInputs(futures.get(JobId.PROTPARS).get().outputPath(), null, outputDirectory);
        };
    }

    public static Path trainingPrimaryInput(Path dataRoot, JobId jobId) {
        return switch (jobId) {
            case BLAST1, BLAST2 -> dataRoot.resolve("training/query-sample.fasta");
            case CLUSTALW1 -> dataRoot.resolve("training/generated/blast1/hits.tsv");
            case CLUSTALW2 -> dataRoot.resolve("training/generated/blast2/hits.tsv");
            case DNAPARS -> dataRoot.resolve("training/generated/clustalw1/alignment.tsv");
            case PROTPARS -> dataRoot.resolve("training/generated/clustalw2/alignment.tsv");
            case DRAWGRAM1 -> dataRoot.resolve("training/generated/dnapars/dna-tree.newick");
            case DRAWGRAM2 -> dataRoot.resolve("training/generated/protpars/protein-tree.newick");
        };
    }

    public static Path trainingSecondaryInput(Path dataRoot, JobId jobId) {
        return switch (jobId) {
            case BLAST1 -> dataRoot.resolve("training/reference-a-sample.fasta");
            case BLAST2 -> dataRoot.resolve("training/reference-b-sample.fasta");
            default -> null;
        };
    }

    public static void ensureTrainingParents(Path dataRoot) throws IOException {
        Files.createDirectories(dataRoot.resolve("training/generated"));
        for (JobId jobId : JobId.values()) {
            Files.createDirectories(dataRoot.resolve("training/generated").resolve(jobId.cliName()));
        }
    }
}
