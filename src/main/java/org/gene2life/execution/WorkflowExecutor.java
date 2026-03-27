package org.gene2life.execution;

import org.gene2life.hadoop.HadoopJobRunner;
import org.gene2life.model.ClusterProfile;
import org.gene2life.model.JobDefinition;
import org.gene2life.model.JobRun;
import org.gene2life.model.NodeProfile;
import org.gene2life.model.PlanAssignment;
import org.gene2life.model.WorkflowDefinition;
import org.gene2life.task.TaskExecutor;
import org.gene2life.task.TaskResult;
import org.gene2life.task.TaskInputs;
import org.gene2life.workflow.WorkflowSpec;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public final class WorkflowExecutor {
    private final WorkflowSpec workflowSpec;
    private final WorkflowDefinition workflow;
    private final Map<String, TaskExecutor> executors;
    private final Map<String, NodeRuntime> runtimes;
    private final ExecutionMode executionMode;
    private final DockerNodePool dockerNodePool;
    private final HadoopJobRunner hadoopJobRunner;
    private final String hdfsRunRoot;

    public WorkflowExecutor(
            WorkflowSpec workflowSpec,
            Map<String, TaskExecutor> executors,
            List<ClusterProfile> clusters,
            ExecutionMode executionMode,
            DockerNodePool dockerNodePool,
            HadoopJobRunner hadoopJobRunner,
            String hdfsRunRoot) {
        this.workflowSpec = workflowSpec;
        this.workflow = workflowSpec.definition();
        this.executors = executors;
        this.executionMode = executionMode;
        this.dockerNodePool = dockerNodePool;
        this.hadoopJobRunner = hadoopJobRunner;
        this.hdfsRunRoot = hdfsRunRoot;
        this.runtimes = new HashMap<>();
        for (ClusterProfile cluster : clusters) {
            for (NodeProfile node : cluster.nodes()) {
                runtimes.put(node.nodeId(), new NodeRuntime(node));
            }
        }
    }

    public List<JobRun> execute(Path dataRoot, Path runRoot, List<PlanAssignment> plan) throws Exception {
        Files.createDirectories(runRoot.resolve("jobs"));
        Map<String, Future<JobRun>> futures = new HashMap<>();
        List<PlanAssignment> orderedPlan = new ArrayList<>(plan);
        orderedPlan.sort(Comparator.comparingInt(assignment -> workflow.orderOf(assignment.jobId())));
        for (PlanAssignment assignment : orderedPlan) {
            waitForDependencies(assignment.jobId(), futures);
            NodeRuntime runtime = runtimes.get(assignment.nodeId());
            futures.put(assignment.jobId(), runtime.submit(
                    assignment,
                    () -> executeAssignment(assignment, dataRoot, runRoot, futures, runtime.nodeProfile())));
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

    private void waitForDependencies(String jobId, Map<String, Future<JobRun>> futures) throws Exception {
        for (String dependency : workflow.job(jobId).dependencies()) {
            futures.get(dependency).get();
        }
    }

    private TaskResult executeAssignment(
            PlanAssignment assignment,
            Path dataRoot,
            Path runRoot,
            Map<String, Future<JobRun>> futures,
            NodeProfile nodeProfile) throws Exception {
        return switch (executionMode) {
            case LOCAL -> {
                TaskInputs inputs = workflowSpec.resolveInputs(assignment.jobId(), dataRoot, runRoot, futures);
                yield executors.get(assignment.jobId()).execute(inputs, nodeProfile);
            }
            case DOCKER -> {
                TaskInputs inputs = workflowSpec.resolveInputs(assignment.jobId(), dataRoot, runRoot, futures);
                yield dockerNodePool.execute(workflowSpec, nodeProfile, assignment.jobId(), inputs);
            }
            case HADOOP -> {
                if (hadoopJobRunner == null) {
                    throw new IllegalStateException("Hadoop executor selected without Hadoop job runner");
                }
                yield hadoopJobRunner.executeWorkflowJob(
                        workflowSpec,
                        assignment.jobId(),
                        nodeProfile,
                        runRoot,
                        hdfsRunRoot);
            }
        };
    }
}
