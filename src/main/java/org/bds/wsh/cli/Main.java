package org.bds.wsh.cli;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.bds.wsh.config.ClusterFactory;
import org.bds.wsh.execution.ExecutionResult;
import org.bds.wsh.execution.RealBenchmarkRunner;
import org.bds.wsh.execution.WorkflowExecutionEngine;
import org.bds.wsh.io.CsvWriter;
import org.bds.wsh.io.DaxWorkflowLoader;
import org.bds.wsh.io.TrainingProfileCsv;
import org.bds.wsh.model.Node;
import org.bds.wsh.model.ScheduleResult;
import org.bds.wsh.model.Workflow;
import org.bds.wsh.scheduler.ClusterTrainingProfile;
import org.bds.wsh.scheduler.HeftScheduler;
import org.bds.wsh.scheduler.RuntimeModel;
import org.bds.wsh.scheduler.Scheduler;
import org.bds.wsh.scheduler.StaticRuntimeModel;
import org.bds.wsh.scheduler.TrainingAwareRuntimeModel;
import org.bds.wsh.scheduler.WshScheduler;
import org.bds.wsh.workflow.WorkflowLibrary;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }
        String command = args[0].toLowerCase(Locale.ROOT);
        switch (command) {
            case "benchmark" -> runBenchmark(Arrays.copyOfRange(args, 1, args.length));
            case "schedule" -> runSchedule(Arrays.copyOfRange(args, 1, args.length));
            case "verify" -> runVerify(Arrays.copyOfRange(args, 1, args.length));
            case "execute" -> runExecute(Arrays.copyOfRange(args, 1, args.length));
            case "real-benchmark" -> runRealBenchmark(Arrays.copyOfRange(args, 1, args.length));
            default -> throw new IllegalArgumentException("Unknown command: " + args[0]);
        }
    }

    private static void runBenchmark(String[] args) throws Exception {
        int[] nodeCounts = parseNodeCounts(optionValue(args, "--node-counts", "4,7,10,13,16,20,24,28"));
        Path output = Path.of(optionValue(args, "--output", "results/metrics.csv"));
        Path schedulesDir = Path.of(optionValue(args, "--schedules-dir", "results/schedules"));
        Path trainingFile = optionPath(args, "--training-file");
        Path nodesFile = optionPath(args, "--nodes-file");
        String workflowFiles = optionValue(args, "--workflow-files", null);

        RuntimeModel runtimeModel = runtimeModel(trainingFile);
        List<Node> fixedNodes = nodesFile == null ? null : ClusterFactory.loadFromCsv(nodesFile);
        List<Workflow> workflows = workflows(workflowFiles);

        BenchmarkRunner runner = new BenchmarkRunner();
        var metrics = runner.run(workflows, nodeCounts, output, schedulesDir, fixedNodes, runtimeModel);
        System.out.println("workflow,algorithm,node_count,makespan_seconds,slr,speedup");
        metrics.forEach(metric -> System.out.printf(Locale.US, "%s,%s,%d,%.5f,%.5f,%.5f%n",
                metric.workflow(), metric.algorithm(), metric.nodeCount(), metric.makespanSeconds(), metric.slr(), metric.speedup()));
        System.out.println();
        System.out.println("Saved metrics to " + output);
        System.out.println("Saved schedules to " + schedulesDir);
    }

    private static void runSchedule(String[] args) throws Exception {
        String workflowName = optionValue(args, "--workflow", "Gene2life");
        Path workflowFile = optionPath(args, "--workflow-file");
        String algorithmName = optionValue(args, "--algorithm", "WSH");
        int nodeCount = Integer.parseInt(optionValue(args, "--node-count", "13"));
        Path output = Path.of(optionValue(args, "--output", "results/single-schedule.csv"));
        Path trainingFile = optionPath(args, "--training-file");
        Path nodesFile = optionPath(args, "--nodes-file");

        Workflow workflow = workflowFile == null ? workflowByName(workflowName) : new DaxWorkflowLoader().load(workflowFile);
        Scheduler scheduler = schedulerByName(algorithmName, runtimeModel(trainingFile));
        List<Node> nodes = nodesFile == null ? ClusterFactory.buildPaperCluster(nodeCount) : ClusterFactory.loadFromCsv(nodesFile);
        ScheduleResult result = scheduler.schedule(workflow, nodes);
        new CsvWriter().writeSchedule(output, result);

        System.out.println("task_id,node_id,cluster_id,start_seconds,finish_seconds");
        result.scheduledTasks().values().forEach(task -> System.out.printf(Locale.US, "%s,%s,%s,%.5f,%.5f%n",
                task.taskId(), task.nodeId(), task.clusterId(), task.startSeconds(), task.finishSeconds()));
        System.out.printf(Locale.US, "%nMakespan: %.5f seconds%n", result.makespanSeconds());
        System.out.println("Saved schedule to " + output);
    }

    /**
     * Executes a single workflow on real Docker containers using the specified algorithm.
     */
    private static void runExecute(String[] args) throws Exception {
        String workflowName = optionValue(args, "--workflow", "Gene2life");
        Path workflowFile = optionPath(args, "--workflow-file");
        String algorithmName = optionValue(args, "--algorithm", "WSH");
        Path output = Path.of(optionValue(args, "--output", "results/real-execution.csv"));
        Path trainingFile = optionPath(args, "--training-file");
        Path nodesFile = optionPath(args, "--nodes-file");

        if (nodesFile == null) {
            nodesFile = Path.of("config/cluster/local-docker-nodes.csv");
        }

        Workflow workflow = workflowFile == null ? workflowByName(workflowName) : new DaxWorkflowLoader().load(workflowFile);
        RuntimeModel rm = runtimeModel(trainingFile);
        Scheduler scheduler = schedulerByName(algorithmName, rm);
        List<Node> nodes = ClusterFactory.loadFromCsv(nodesFile);

        System.out.println("Scheduling " + workflow.name() + " with " + algorithmName + " (" + nodes.size() + " nodes)...");
        ScheduleResult schedule = scheduler.schedule(workflow, nodes);
        System.out.printf(Locale.US, "Simulated makespan: %.5f seconds%n", schedule.makespanSeconds());

        System.out.println("\nExecuting on real Docker containers...\n");
        WorkflowExecutionEngine engine = new WorkflowExecutionEngine();
        ExecutionResult execResult = engine.execute(workflow, schedule, nodes);

        // Write execution results.
        java.nio.file.Files.createDirectories(output.getParent());
        StringBuilder sb = new StringBuilder();
        sb.append("task_id,node_id,container_name,start_epoch_ms,finish_epoch_ms,duration_seconds\n");
        for (var tr : execResult.taskResults().values()) {
            sb.append(String.format(Locale.US, "%s,%s,%s,%d,%d,%.3f%n",
                    tr.taskId(), tr.nodeId(), tr.containerName(),
                    tr.actualStartEpochMs(), tr.actualFinishEpochMs(), tr.actualDurationSeconds()));
        }
        java.nio.file.Files.writeString(output, sb.toString());

        System.out.printf(Locale.US, "%nReal makespan: %.2f seconds%n", execResult.realMakespanSeconds());
        System.out.println("Saved execution results to " + output);
    }

    /**
     * Runs full real-world benchmark: schedules and executes workflows with both
     * HEFT and WSH across multiple node configurations on real Docker containers.
     */
    private static void runRealBenchmark(String[] args) throws Exception {
        Path output = Path.of(optionValue(args, "--output", "results/real-metrics.csv"));
        Path detailsDir = Path.of(optionValue(args, "--details-dir", "results/real-executions"));
        Path trainingFile = optionPath(args, "--training-file");
        String workflowFiles = optionValue(args, "--workflow-files", null);
        String nodeCountsStr = optionValue(args, "--node-counts", "4,7,10,13,16,20,24,28");

        RuntimeModel rm = runtimeModel(trainingFile);
        List<Workflow> wfs = workflows(workflowFiles);

        // Build node configurations for each requested count.
        int[] nodeCounts = parseNodeCounts(nodeCountsStr);
        List<List<Node>> nodeConfigs = new ArrayList<>();
        for (int count : nodeCounts) {
            Path nodesFile = Path.of("config/cluster/nodes-" + count + ".csv");
            if (java.nio.file.Files.exists(nodesFile)) {
                nodeConfigs.add(ClusterFactory.loadFromCsv(nodesFile));
            } else {
                // Fall back to paper cluster model which distributes nodes
                // round-robin across C1-C4 tiers for proper heterogeneity.
                System.out.println("Note: config/cluster/nodes-" + count + ".csv not found, using paper cluster model.");
                nodeConfigs.add(ClusterFactory.buildPaperCluster(count));
            }
        }

        System.out.println("╔══════════════════════════════════════════════════════════╗");
        System.out.println("║      WSH Real-World Benchmark: HEFT vs WSH              ║");
        System.out.println("║      Executing real workloads on Docker containers       ║");
        System.out.println("╚══════════════════════════════════════════════════════════╝");
        System.out.println();

        RealBenchmarkRunner runner = new RealBenchmarkRunner();
        var results = runner.run(wfs, nodeConfigs, rm, output, detailsDir);

        // Print summary table.
        System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                          RESULTS SUMMARY                                   ║");
        System.out.println("╠═══════════════════╦═══════════╦═══════╦═══════════════╦═════════════════════╣");
        System.out.println("║ Workflow          ║ Algorithm ║ Nodes ║ Real Makespan ║ WSH Improvement     ║");
        System.out.println("╠═══════════════════╬═══════════╬═══════╬═══════════════╬═════════════════════╣");
        for (var r : results) {
            System.out.printf("║ %-17s ║ %-9s ║ %5d ║ %11.2fs ║ %17.2f%% ║%n",
                    r.workflow(), r.algorithm(), r.nodeCount(), r.realMakespan(), r.improvementPercent());
        }
        System.out.println("╚═══════════════════╩═══════════╩═══════╩═══════════════╩═════════════════════╝");
        System.out.println("\nSaved comparison results to " + output);
        System.out.println("Saved per-run details to " + detailsDir);
    }

    private static void runVerify(String[] args) throws Exception {
        Path input = Path.of(optionValue(args, "--input", "results/metrics.csv"));
        new MetricsVerifier().verify(input);
        System.out.println("Verification passed for " + input);
    }

    private static RuntimeModel runtimeModel(Path trainingFile) throws Exception {
        RuntimeModel staticModel = new StaticRuntimeModel();
        if (trainingFile == null) {
            return staticModel;
        }
        Map<String, ClusterTrainingProfile> profiles = new TrainingProfileCsv().load(trainingFile);
        return new TrainingAwareRuntimeModel(staticModel, profiles);
    }

    private static List<Workflow> workflows(String workflowFiles) throws Exception {
        if (workflowFiles == null || workflowFiles.isBlank()) {
            return WorkflowLibrary.defaultWorkflows();
        }
        DaxWorkflowLoader loader = new DaxWorkflowLoader();
        List<Workflow> workflows = new ArrayList<>();
        for (String token : workflowFiles.split(",")) {
            workflows.add(loader.load(Path.of(token.trim())));
        }
        return workflows;
    }

    private static String optionValue(String[] args, String name, String defaultValue) {
        for (int index = 0; index < args.length; index++) {
            if (args[index].equalsIgnoreCase(name)) {
                if (index + 1 >= args.length) {
                    throw new IllegalArgumentException("Missing value for option " + name);
                }
                return args[index + 1];
            }
        }
        return defaultValue;
    }

    private static Path optionPath(String[] args, String name) {
        String value = optionValue(args, name, null);
        return value == null ? null : Path.of(value);
    }

    private static int[] parseNodeCounts(String raw) {
        String[] tokens = raw.split(",");
        int[] result = new int[tokens.length];
        for (int index = 0; index < tokens.length; index++) {
            result[index] = Integer.parseInt(tokens[index].trim());
        }
        return result;
    }

    private static Workflow workflowByName(String name) {
        return switch (name.toLowerCase(Locale.ROOT)) {
            case "gene2life", "gen2life" -> WorkflowLibrary.gene2life();
            case "avianflu_small", "avianflu" -> WorkflowLibrary.avianfluSmall();
            case "avianflu_fast" -> WorkflowLibrary.avianfluSmallFast();
            case "epigenomics" -> WorkflowLibrary.epigenomics();
            case "epigenomics_fast" -> WorkflowLibrary.epigenomicsFast();
            default -> throw new IllegalArgumentException("Unknown workflow: " + name
                    + ". Available: Gene2life, Avianflu_small, Avianflu_fast, Epigenomics, Epigenomics_fast");
        };
    }

    private static Scheduler schedulerByName(String name, RuntimeModel runtimeModel) {
        return switch (name.toUpperCase(Locale.ROOT)) {
            case "WSH" -> new WshScheduler(runtimeModel);
            case "HEFT" -> new HeftScheduler(runtimeModel);
            default -> throw new IllegalArgumentException("Unknown algorithm: " + name);
        };
    }

    private static void printUsage() {
        List<String> lines = new ArrayList<>();
        lines.add("Usage:");
        lines.add("  Simulation commands (existing):");
        lines.add("    java -jar build/wsh-scheduler.jar benchmark --node-counts 4,7,10,13,16,20,24,28 --output results/metrics.csv --schedules-dir results/schedules");
        lines.add("    java -jar build/wsh-scheduler.jar schedule --workflow Gene2life --algorithm WSH --node-count 13 --output results/single-schedule.csv");
        lines.add("    java -jar build/wsh-scheduler.jar verify --input results/metrics.csv");
        lines.add("");
        lines.add("  Real execution commands (NEW):");
        lines.add("    java -jar build/wsh-scheduler.jar execute --workflow Gene2life --algorithm WSH --nodes-file config/cluster/local-docker-nodes.csv --output results/real-execution.csv");
        lines.add("    java -jar build/wsh-scheduler.jar real-benchmark --node-counts 4,7,10,13,16,20,24,28 --output results/real-metrics.csv --details-dir results/real-executions");
        lines.forEach(System.out::println);
    }
}
