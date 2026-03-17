package org.bds.wsh.execution;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.bds.wsh.model.Node;
import org.bds.wsh.model.ScheduleResult;
import org.bds.wsh.model.Workflow;
import org.bds.wsh.scheduler.HeftScheduler;
import org.bds.wsh.scheduler.RuntimeModel;
import org.bds.wsh.scheduler.Scheduler;
import org.bds.wsh.scheduler.WshScheduler;

/**
 * End-to-end real benchmark runner.
 *
 * <p>For each workflow and node configuration:
 * <ol>
 *   <li>Generates schedules using both HEFT and WSH</li>
 *   <li>Executes each schedule on real Docker containers</li>
 *   <li>Records actual wall-clock makespans</li>
 *   <li>Writes comparison CSV with simulated vs real makespans</li>
 * </ol>
 */
public final class RealBenchmarkRunner {

    private final WorkflowExecutionEngine engine;

    public RealBenchmarkRunner() {
        this.engine = new WorkflowExecutionEngine();
    }

    /**
     * Runs full real-world benchmark comparison.
     *
     * @param workflows workflows to benchmark
     * @param nodeConfigs list of node configurations (each entry = one set of nodes to test)
     * @param runtimeModel runtime model for scheduling phase
     * @param metricsOutput path to write the comparison CSV
     * @param executionDetailsDir directory for per-run execution details
     * @return list of comparison results
     */
    public List<ComparisonResult> run(
            List<Workflow> workflows,
            List<List<Node>> nodeConfigs,
            RuntimeModel runtimeModel,
            Path metricsOutput,
            Path executionDetailsDir
    ) throws IOException {
        List<Scheduler> schedulers = List.of(
                new HeftScheduler(runtimeModel),
                new WshScheduler(runtimeModel)
        );

        List<ComparisonResult> results = new ArrayList<>();

        for (Workflow workflow : workflows) {
            for (List<Node> nodes : nodeConfigs) {
                int nodeCount = nodes.size();
                System.out.println("\n" + "=".repeat(70));
                System.out.printf("Workflow: %s | Nodes: %d%n", workflow.name(), nodeCount);
                System.out.println("=".repeat(70));

                Map<String, Double> simulatedMakespans = new LinkedHashMap<>();
                Map<String, Double> realMakespans = new LinkedHashMap<>();

                for (Scheduler scheduler : schedulers) {
                    // Phase 1: Generate schedule.
                    ScheduleResult schedule = scheduler.schedule(workflow, nodes);
                    double simulatedMakespan = schedule.makespanSeconds();
                    simulatedMakespans.put(scheduler.name(), simulatedMakespan);

                    System.out.printf("%n--- %s: Simulated makespan = %.2fs ---%n",
                            scheduler.name(), simulatedMakespan);

                    // Phase 2: Execute schedule on real containers.
                    ExecutionResult execResult = engine.execute(workflow, schedule, nodes);
                    double realMakespan = execResult.realMakespanSeconds();
                    realMakespans.put(scheduler.name(), realMakespan);

                    System.out.printf("--- %s: Real makespan = %.2fs ---%n",
                            scheduler.name(), realMakespan);

                    // Write per-run execution details.
                    if (executionDetailsDir != null) {
                        writeExecutionDetails(executionDetailsDir, workflow.name(),
                                scheduler.name(), nodeCount, execResult);
                    }
                }

                // Compute improvements.
                double heftReal = realMakespans.getOrDefault("HEFT", 0.0);
                double wshReal = realMakespans.getOrDefault("WSH", 0.0);
                double improvementPercent = heftReal > 0 && wshReal > 0
                        ? ((heftReal - wshReal) / heftReal) * 100.0 : 0.0;

                System.out.printf("%n>>> WSH improvement over HEFT (real): %.2f%%%n", improvementPercent);

                for (Scheduler scheduler : schedulers) {
                    results.add(new ComparisonResult(
                            workflow.name(),
                            scheduler.name(),
                            nodeCount,
                            simulatedMakespans.get(scheduler.name()),
                            realMakespans.get(scheduler.name()),
                            scheduler.name().equals("WSH") ? improvementPercent : 0.0
                    ));
                }
            }
        }

        // Write comparison CSV.
        writeComparisonCsv(metricsOutput, results);
        return results;
    }

    private void writeComparisonCsv(Path path, List<ComparisonResult> results) throws IOException {
        Files.createDirectories(path.getParent());
        StringBuilder sb = new StringBuilder();
        sb.append("workflow,algorithm,node_count,simulated_makespan_seconds,real_makespan_seconds,wsh_improvement_percent\n");
        for (ComparisonResult r : results) {
            sb.append(String.format(Locale.US, "%s,%s,%d,%.5f,%.5f,%.2f%n",
                    r.workflow, r.algorithm, r.nodeCount,
                    r.simulatedMakespan, r.realMakespan, r.improvementPercent));
        }
        Files.writeString(path, sb.toString(), StandardCharsets.UTF_8);
        System.out.println("\nSaved real-benchmark comparison to " + path);
    }

    private void writeExecutionDetails(Path dir, String workflow, String algorithm,
                                        int nodeCount, ExecutionResult execResult) throws IOException {
        Files.createDirectories(dir);
        Path file = dir.resolve(String.format("%s_%s_%d_execution.csv", workflow, algorithm, nodeCount));
        StringBuilder sb = new StringBuilder();
        sb.append("task_id,node_id,container_name,start_epoch_ms,finish_epoch_ms,duration_seconds\n");
        for (TaskExecutionResult tr : execResult.taskResults().values()) {
            sb.append(String.format(Locale.US, "%s,%s,%s,%d,%d,%.3f%n",
                    tr.taskId(), tr.nodeId(), tr.containerName(),
                    tr.actualStartEpochMs(), tr.actualFinishEpochMs(), tr.actualDurationSeconds()));
        }
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
    }

    public record ComparisonResult(
            String workflow,
            String algorithm,
            int nodeCount,
            double simulatedMakespan,
            double realMakespan,
            double improvementPercent
    ) {}
}
