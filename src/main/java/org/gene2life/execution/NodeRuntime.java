package org.gene2life.execution;

import org.gene2life.model.NodeProfile;
import org.gene2life.model.JobRun;
import org.gene2life.model.PlanAssignment;
import org.gene2life.task.TaskExecutor;
import org.gene2life.task.TaskInputs;
import org.gene2life.task.TaskResult;
import org.gene2life.workflow.WorkflowSpec;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class NodeRuntime implements AutoCloseable {
    private final NodeProfile nodeProfile;
    private final ExecutorService executorService;
    private final ExecutionMode executionMode;
    private final DockerNodePool dockerNodePool;
    private final WorkflowSpec workflowSpec;

    public NodeRuntime(NodeProfile nodeProfile, ExecutionMode executionMode, DockerNodePool dockerNodePool, WorkflowSpec workflowSpec) {
        this.nodeProfile = nodeProfile;
        this.executionMode = executionMode;
        this.dockerNodePool = dockerNodePool;
        this.workflowSpec = workflowSpec;
        this.executorService = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("node-" + nodeProfile.nodeId());
            return thread;
        });
    }

    public Future<JobRun> submit(PlanAssignment assignment, TaskExecutor executor, TaskInputs inputs) {
        Callable<JobRun> callable = () -> {
            long start = System.currentTimeMillis();
            TaskResult result = executionMode == ExecutionMode.DOCKER
                    ? dockerNodePool.execute(workflowSpec, nodeProfile, assignment.jobId(), inputs)
                    : executor.execute(inputs, nodeProfile);
            long finish = System.currentTimeMillis();
            long duration = Math.max(1L, finish - start);
            return new JobRun(
                    assignment.jobId(),
                    assignment.clusterId(),
                    assignment.nodeId(),
                    assignment.schedulerName(),
                    assignment.predictedStartMillis(),
                    assignment.predictedFinishMillis(),
                    start,
                    finish,
                    duration,
                    result.outputPath(),
                    result.description());
        };
        return executorService.submit(callable);
    }

    @Override
    public void close() {
        executorService.shutdown();
    }
}
