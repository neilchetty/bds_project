package org.bds.wsh.cli;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.bds.wsh.config.ClusterFactory;
import org.bds.wsh.io.CsvWriter;
import org.bds.wsh.metrics.MetricCalculator;
import org.bds.wsh.metrics.MetricSet;
import org.bds.wsh.model.Node;
import org.bds.wsh.model.ScheduleResult;
import org.bds.wsh.model.Workflow;
import org.bds.wsh.scheduler.HeftScheduler;
import org.bds.wsh.scheduler.RuntimeModel;
import org.bds.wsh.scheduler.Scheduler;
import org.bds.wsh.scheduler.StaticRuntimeModel;
import org.bds.wsh.scheduler.WshScheduler;
import org.bds.wsh.workflow.WorkflowLibrary;

public final class BenchmarkRunner {
    private final CsvWriter csvWriter = new CsvWriter();
    private final MetricCalculator metricCalculator = new MetricCalculator();

    public List<MetricSet> run(int[] nodeCounts, Path metricsOutput, Path schedulesDirectory) throws IOException {
        return run(WorkflowLibrary.defaultWorkflows(), nodeCounts, metricsOutput, schedulesDirectory, null, new StaticRuntimeModel());
    }

    public List<MetricSet> run(
            List<Workflow> workflows,
            int[] nodeCounts,
            Path metricsOutput,
            Path schedulesDirectory,
            List<Node> fixedNodes,
            RuntimeModel runtimeModel
    ) throws IOException {
        List<MetricSet> metrics = new ArrayList<>();
        List<Scheduler> schedulers = List.of(new HeftScheduler(runtimeModel), new WshScheduler(runtimeModel));
        int[] effectiveNodeCounts = fixedNodes == null ? nodeCounts : new int[]{fixedNodes.size()};

        for (Workflow workflow : workflows) {
            for (int nodeCount : effectiveNodeCounts) {
                List<Node> nodes = fixedNodes == null ? ClusterFactory.buildPaperCluster(nodeCount) : fixedNodes;
                int effectiveNodeCount = nodes.size();
                for (Scheduler scheduler : schedulers) {
                    ScheduleResult result = scheduler.schedule(workflow, nodes);
                    metrics.add(metricCalculator.collect(result, workflow, nodes, effectiveNodeCount, runtimeModel));
                    Path scheduleFile = schedulesDirectory.resolve(workflow.name() + "_" + scheduler.name() + "_" + effectiveNodeCount + ".csv");
                    csvWriter.writeSchedule(scheduleFile, result);
                }
            }
        }
        csvWriter.writeMetrics(metricsOutput, metrics);
        return metrics;
    }
}
