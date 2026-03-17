package org.bds.wsh.scheduler;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bds.wsh.model.Node;
import org.bds.wsh.model.ScheduleResult;
import org.bds.wsh.model.ScheduledTask;
import org.bds.wsh.model.Task;
import org.bds.wsh.model.Workflow;

/**
 * Base class for list-scheduling algorithms (HEFT, WSH).
 * Computes upward ranks, then schedules tasks in rank order
 * using a CandidateProvider to select candidate nodes.
 *
 * Paper-faithful note:
 * The referenced WSH paper ignores communication costs when computing ranks and
 * earliest start times (EST). Therefore EFT here is:
 * EFT(ti, nj) = max(readyTime(ti), availTime(nj)) + w(ti, nj)
 * where readyTime(ti) = max(AFT of predecessors), with no extra comm term.
 */
abstract class AbstractScheduler implements Scheduler {
    protected final RuntimeModel runtimeModel;

    protected AbstractScheduler(RuntimeModel runtimeModel) {
        this.runtimeModel = runtimeModel;
    }

    protected record Candidate(Node node, double startSeconds, double finishSeconds) {
    }

    protected List<String> prioritizedTasks(Workflow workflow, List<Node> nodes) {
        Map<String, Double> ranks = RankCalculator.upwardRanks(workflow, nodes, runtimeModel);
        return workflow.tasks().keySet().stream()
                .sorted(Comparator.comparing((String taskId) -> ranks.get(taskId)).reversed().thenComparing(taskId -> taskId))
                .toList();
    }

    /**
     * Selects the candidate node with the earliest finish time (EFT).
     * EFT(ti, nj) = EST(ti, nj) + w(ti, nj)
     * EST is max of predecessor finish times and node availability.
     */
    protected Candidate chooseBestNode(
            Task task,
            List<Node> candidates,
            Map<String, Double> nodeAvailable,
            Map<String, ScheduledTask> scheduledTasks,
            Map<String, Node> nodeMap
    ) {
        Candidate best = null;
        for (Node node : candidates) {
            double duration = runtimeModel.estimateSeconds(task, node);
            double readyTime = 0.0;
            for (String predId : task.predecessors()) {
                ScheduledTask pred = scheduledTasks.get(predId);
                readyTime = Math.max(readyTime, pred.finishSeconds());
            }

            double start = earliestInsertionStartSeconds(node.id(), readyTime, duration, scheduledTasks);
            double finish = start + duration;

            if (best == null || finish < best.finishSeconds() - 1e-9 ||
                    (Math.abs(finish - best.finishSeconds()) < 1e-9 && node.id().compareTo(best.node().id()) < 0)) {
                best = new Candidate(node, start, finish);
            }
        }
        if (best == null) {
            throw new IllegalStateException("No candidate nodes found for task " + task.id());
        }
        return best;
    }

    /**
     * Finds the earliest start time >= readyTime that can fit {@code durationSeconds}
     * into the node timeline (idle-time insertion policy).
     */
    private static double earliestInsertionStartSeconds(
            String nodeId,
            double readyTime,
            double durationSeconds,
            Map<String, ScheduledTask> scheduledTasks
    ) {
        List<ScheduledTask> onNode = scheduledTasks.values().stream()
                .filter(st -> st.nodeId().equals(nodeId))
                .sorted(Comparator.comparingDouble(ScheduledTask::startSeconds).thenComparing(ScheduledTask::taskId))
                .toList();

        if (onNode.isEmpty()) {
            return readyTime;
        }

        // Before first task
        ScheduledTask first = onNode.get(0);
        if (readyTime + durationSeconds <= first.startSeconds()) {
            return readyTime;
        }

        // Between tasks
        for (int i = 0; i < onNode.size() - 1; i++) {
            ScheduledTask current = onNode.get(i);
            ScheduledTask next = onNode.get(i + 1);
            double candidateStart = Math.max(readyTime, current.finishSeconds());
            if (candidateStart + durationSeconds <= next.startSeconds()) {
                return candidateStart;
            }
        }

        // After last task
        ScheduledTask last = onNode.get(onNode.size() - 1);
        return Math.max(readyTime, last.finishSeconds());
    }

    protected ScheduleResult scheduleWithCandidates(Workflow workflow, List<Node> nodes, CandidateProvider provider) {
        List<String> orderedTasks = prioritizedTasks(workflow, nodes);
        Map<String, Node> nodeMap = new LinkedHashMap<>();
        for (Node node : nodes) {
            nodeMap.put(node.id(), node);
        }
        Map<String, Double> nodeAvailable = new LinkedHashMap<>();
        for (Node node : nodes) {
            nodeAvailable.put(node.id(), 0.0);
        }
        Map<String, ScheduledTask> scheduled = new LinkedHashMap<>();
        for (String taskId : orderedTasks) {
            Task task = workflow.tasks().get(taskId);
            List<Node> candidates = new ArrayList<>(provider.candidates(task, workflow, nodes, scheduled, nodeAvailable));
            Candidate best = chooseBestNode(task, candidates, nodeAvailable, scheduled, nodeMap);
            scheduled.put(task.id(), new ScheduledTask(task.id(), best.node().id(), best.node().clusterId(), best.startSeconds(), best.finishSeconds()));
            nodeAvailable.put(best.node().id(), best.finishSeconds());
            provider.onScheduled(task, workflow, nodes, best.node(), scheduled);
        }
        return new ScheduleResult(name(), workflow.name(), scheduled);
    }

    @FunctionalInterface
    protected interface CandidateProvider {
        List<Node> candidates(Task task, Workflow workflow, List<Node> nodes, Map<String, ScheduledTask> scheduledTasks, Map<String, Double> nodeAvailable);

        default void onScheduled(Task task, Workflow workflow, List<Node> nodes, Node selectedNode, Map<String, ScheduledTask> scheduledTasks) {
        }
    }
}
