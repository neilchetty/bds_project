package org.bds.wsh.scheduler;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bds.wsh.model.Node;
import org.bds.wsh.model.Task;
import org.bds.wsh.model.Workflow;

/**
 * Computes HEFT-style upward ranks.
 *
 * Paper-faithful note:
 * The WSH paper explicitly ignores communication costs for simplicity, so this
 * implementation computes:
 *
 * rank_u(t_i) = w_avg(t_i) + max over successors t_j of rank_u(t_j)
 */
public final class RankCalculator {
    private RankCalculator() {
    }

    public static Map<String, Double> upwardRanks(Workflow workflow, List<Node> nodes, RuntimeModel runtimeModel) {
        Map<String, List<String>> successors = workflow.successors();
        Map<String, Double> memo = new LinkedHashMap<>();
        for (String taskId : workflow.tasks().keySet()) {
            rank(taskId, workflow, nodes, successors, memo, runtimeModel);
        }
        return memo;
    }

    private static double rank(
            String taskId,
            Workflow workflow,
            List<Node> nodes,
            Map<String, List<String>> successors,
            Map<String, Double> memo,
            RuntimeModel runtimeModel
    ) {
        if (memo.containsKey(taskId)) {
            return memo.get(taskId);
        }
        Task task = workflow.tasks().get(taskId);
        double averageCost = nodes.stream()
                .mapToDouble(node -> runtimeModel.estimateSeconds(task, node))
                .average()
                .orElseThrow();
        double bestChild = 0.0;
        for (String childId : successors.get(taskId)) {
            bestChild = Math.max(bestChild, rank(childId, workflow, nodes, successors, memo, runtimeModel));
        }
        double r = averageCost + bestChild;
        memo.put(taskId, r);
        return r;
    }
}
