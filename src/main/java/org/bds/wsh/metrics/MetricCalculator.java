package org.bds.wsh.metrics;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bds.wsh.model.Node;
import org.bds.wsh.model.ScheduleResult;
import org.bds.wsh.model.Task;
import org.bds.wsh.model.Workflow;
import org.bds.wsh.scheduler.RuntimeModel;
import org.bds.wsh.scheduler.StaticRuntimeModel;

public final class MetricCalculator {
    public MetricSet collect(ScheduleResult result, Workflow workflow, List<Node> nodes, int nodeCount) {
        return collect(result, workflow, nodes, nodeCount, new StaticRuntimeModel());
    }

    public MetricSet collect(ScheduleResult result, Workflow workflow, List<Node> nodes, int nodeCount, RuntimeModel runtimeModel) {
        double criticalPath = criticalPathSeconds(workflow, nodes, runtimeModel);
        double sequentialRuntime = workflow.tasks().values().stream()
                .mapToDouble(task -> runtimeModel.estimateSeconds(task, nodes.get(0)))
                .sum();
        double makespan = result.makespanSeconds();
        double slr = criticalPath == 0.0 ? 0.0 : makespan / criticalPath;
        double speedup = makespan == 0.0 ? 0.0 : sequentialRuntime / makespan;
        return new MetricSet(workflow.name(), result.algorithm(), nodeCount, makespan, slr, speedup);
    }

    private double criticalPathSeconds(Workflow workflow, List<Node> nodes, RuntimeModel runtimeModel) {
        Map<String, Double> longest = new LinkedHashMap<>();
        for (String taskId : workflow.topologicalOrder()) {
            Task task = workflow.tasks().get(taskId);
            double shortestRuntime = nodes.stream()
                    .mapToDouble(node -> runtimeModel.estimateSeconds(task, node))
                    .min()
                    .orElseThrow();
            double predecessorReady = 0.0;
            for (String predecessor : task.predecessors()) {
                predecessorReady = Math.max(predecessorReady, longest.get(predecessor));
            }
            longest.put(taskId, predecessorReady + shortestRuntime);
        }
        return longest.values().stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
    }
}
