package org.gene2life.execution;

import org.gene2life.model.ClusterProfile;
import org.gene2life.model.JobDefinition;
import org.gene2life.model.JobRun;
import org.gene2life.model.NodeProfile;
import org.gene2life.model.PlanAssignment;
import org.gene2life.model.WorkflowDefinition;
import org.gene2life.task.TaskExecutor;
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

    public WorkflowExecutor(
            WorkflowSpec workflowSpec,
            Map<String, TaskExecutor> executors,
            List<ClusterProfile> clusters,
            ExecutionMode executionMode,
            DockerNodePool dockerNodePool) {
        this.workflowSpec = workflowSpec;
        this.workflow = workflowSpec.definition();
        this.executors = executors;
        this.runtimes = new HashMap<>();
        for (ClusterProfile cluster : clusters) {
            for (NodeProfile node : cluster.nodes()) {
                runtimes.put(node.nodeId(), new NodeRuntime(node, executionMode, dockerNodePool, workflowSpec));
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
            TaskInputs inputs = workflowSpec.resolveInputs(assignment.jobId(), dataRoot, runRoot, futures);
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

    private void waitForDependencies(String jobId, Map<String, Future<JobRun>> futures) throws Exception {
        for (String dependency : workflow.job(jobId).dependencies()) {
            futures.get(dependency).get();
        }
    }
}
