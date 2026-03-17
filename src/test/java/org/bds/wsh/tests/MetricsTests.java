package org.bds.wsh.tests;

import java.nio.file.Files;
import java.nio.file.Path;

import org.bds.wsh.cli.BenchmarkRunner;
import org.bds.wsh.cli.MetricsVerifier;
import org.bds.wsh.config.ClusterFactory;
import org.bds.wsh.metrics.MetricCalculator;
import org.bds.wsh.scheduler.StaticRuntimeModel;
import org.bds.wsh.model.Workflow;
import org.bds.wsh.scheduler.WshScheduler;
import org.bds.wsh.workflow.WorkflowLibrary;

public final class MetricsTests {
    public void run() throws Exception {
        Workflow workflow = WorkflowLibrary.gene2life();
        var nodes = ClusterFactory.buildPaperCluster(4);
        var result = new WshScheduler(new StaticRuntimeModel()).schedule(workflow, nodes);
        var metrics = new MetricCalculator().collect(result, workflow, nodes, 4, new StaticRuntimeModel());
        TestSupport.assertTrue(metrics.makespanSeconds() > 0.0, "Makespan must be positive.");
        TestSupport.assertTrue(metrics.slr() > 0.0, "SLR must be positive.");
        TestSupport.assertTrue(metrics.speedup() > 0.0, "Speedup must be positive.");

        Path metricsFile = Path.of("results", "test-metrics.csv");
        Path schedulesDir = Path.of("results", "test-schedules");
        new BenchmarkRunner().run(new int[]{4, 7, 10, 13}, metricsFile, schedulesDir);
        new MetricsVerifier().verify(metricsFile);
        TestSupport.assertTrue(Files.exists(metricsFile), "Metrics file should exist.");
    }
}
