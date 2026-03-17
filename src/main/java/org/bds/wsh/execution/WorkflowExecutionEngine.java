package org.bds.wsh.execution;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.bds.wsh.model.Node;
import org.bds.wsh.model.ScheduleResult;
import org.bds.wsh.model.ScheduledTask;
import org.bds.wsh.model.Task;
import org.bds.wsh.model.Workflow;

/**
 * Orchestrates real execution of an entire workflow on Docker containers.
 *
 * <p>Given a schedule from HEFT or WSH (task → node mapping), this engine:
 * <ol>
 *   <li>Walks tasks in topological (dependency) order</li>
 *   <li>Runs independent tasks concurrently via a thread pool</li>
 *   <li>For each task: transfers predecessor data, then executes real workload</li>
 *   <li>Records actual wall-clock timestamps</li>
 * </ol>
 */
public final class WorkflowExecutionEngine {

    private final WorkloadExecutor workloadExecutor;
    private final DataTransferManager dataTransferManager;

    public WorkflowExecutionEngine() {
        this.workloadExecutor = new WorkloadExecutor();
        this.dataTransferManager = new DataTransferManager();
    }

    /**
     * Executes a workflow according to the given schedule on real Docker containers.
     */
    public ExecutionResult execute(Workflow workflow, ScheduleResult schedule, List<Node> nodes) {
        Map<String, Node> nodeMap = new LinkedHashMap<>();
        for (Node node : nodes) {
            nodeMap.put(node.id(), node);
        }

        System.out.println("=== Real Execution: " + workflow.name() + " with " + schedule.algorithm() + " ===");
        System.out.println("Nodes: " + nodes.size());

        cleanContainers(nodes);

        // Build dependency tracking.
        Map<String, List<String>> successors = workflow.successors();
        Map<String, Integer> remainingDeps = new LinkedHashMap<>();
        for (Task task : workflow.tasks().values()) {
            remainingDeps.put(task.id(), task.predecessors().size());
        }

        ConcurrentHashMap<String, TaskExecutionResult> taskResults = new ConcurrentHashMap<>();
        Set<String> submitted = new HashSet<>();
        Set<String> processed = new HashSet<>(); // tracks tasks whose successors have been unlocked

        int maxParallel = Math.min(nodes.size(), Runtime.getRuntime().availableProcessors());
        ExecutorService executor = Executors.newFixedThreadPool(maxParallel);

        long executionStartMs = System.currentTimeMillis();

        try {
            // Find initially ready tasks (no predecessors).
            List<String> readyTasks = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : remainingDeps.entrySet()) {
                if (entry.getValue() == 0) {
                    readyTasks.add(entry.getKey());
                }
            }

            while (!readyTasks.isEmpty()) {
                List<CompletableFuture<Void>> waveFutures = new ArrayList<>();
                List<String> waveTaskIds = new ArrayList<>(readyTasks);

                for (String taskId : waveTaskIds) {
                    submitted.add(taskId);
                    Task task = workflow.tasks().get(taskId);
                    ScheduledTask scheduledTask = schedule.scheduledTasks().get(taskId);
                    Node targetNode = nodeMap.get(scheduledTask.nodeId());

                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        // Transfer predecessor data.
                        dataTransferManager.transferPredecessorData(
                                task, targetNode, schedule.scheduledTasks(), nodeMap);
                        // Execute real workload.
                        TaskExecutionResult result = workloadExecutor.execute(task, targetNode);
                        taskResults.put(taskId, result);
                    }, executor);

                    waveFutures.add(future);
                }

                // Wait for all tasks in this wave.
                CompletableFuture.allOf(waveFutures.toArray(new CompletableFuture[0])).join();

                // Find next wave: only process NEWLY completed tasks (not previously processed).
                readyTasks = new ArrayList<>();
                for (String completedId : taskResults.keySet()) {
                    if (processed.contains(completedId)) continue;
                    processed.add(completedId);
                    for (String succId : successors.getOrDefault(completedId, List.of())) {
                        if (submitted.contains(succId)) continue;
                        int remaining = remainingDeps.get(succId) - 1;
                        remainingDeps.put(succId, remaining);
                        if (remaining == 0) {
                            readyTasks.add(succId);
                        }
                    }
                }
            }
        } finally {
            executor.shutdown();
            try { executor.awaitTermination(10, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        }

        long executionFinishMs = System.currentTimeMillis();

        System.out.printf("=== Completed %s/%s in %.2fs (real wall-clock) ===%n",
                workflow.name(), schedule.algorithm(),
                (executionFinishMs - executionStartMs) / 1000.0);

        cleanContainers(nodes);

        return new ExecutionResult(
                schedule.algorithm(),
                workflow.name(),
                nodes.size(),
                new LinkedHashMap<>(taskResults),
                executionStartMs,
                executionFinishMs
        );
    }

    /** Removes leftover temp files from all containers. */
    private void cleanContainers(List<Node> nodes) {
        for (Node node : nodes) {
            try {
                List<String> cmd = WorkloadExecutor.buildDockerCommand(node,
                        "rm -f /tmp/wsh-task-*.bin /tmp/wsh-xfer-*.bin 2>/dev/null || true");
                ProcessBuilder pb = new ProcessBuilder(cmd);
                pb.redirectErrorStream(true);
                Process p = pb.start();
                p.getInputStream().readAllBytes();
                p.waitFor(10, TimeUnit.SECONDS);
            } catch (Exception ignored) { }
        }
    }
}
