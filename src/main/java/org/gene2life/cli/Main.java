package org.gene2life.cli;

import org.gene2life.config.ClusterConfigLoader;
import org.gene2life.config.ClusterProfiles;
import org.gene2life.data.DataGenerator;
import org.gene2life.execution.DockerNodePool;
import org.gene2life.execution.ExecutionMode;
import org.gene2life.execution.JobOutputs;
import org.gene2life.execution.TrainingRunner;
import org.gene2life.execution.WorkflowExecutor;
import org.gene2life.model.ClusterProfile;
import org.gene2life.model.JobId;
import org.gene2life.model.JobRun;
import org.gene2life.model.NodeProfile;
import org.gene2life.model.PlanAssignment;
import org.gene2life.model.WorkflowDefinition;
import org.gene2life.report.ReportWriter;
import org.gene2life.scheduler.HeftScheduler;
import org.gene2life.scheduler.Scheduler;
import org.gene2life.scheduler.TrainingBenchmarks;
import org.gene2life.scheduler.WshScheduler;
import org.gene2life.task.TaskExecutor;
import org.gene2life.task.Gene2LifeTaskExecutors;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        CliArguments cli = CliArguments.parse(args);
        switch (cli.command()) {
            case "generate-data" -> generateData(cli);
            case "run" -> runScheduler(cli);
            case "compare" -> compare(cli);
            case "run-job" -> runJob(cli);
            default -> throw new IllegalArgumentException("Unknown command: " + cli.command());
        }
    }

    private static void generateData(CliArguments cli) throws Exception {
        Path workspace = Path.of(cli.option("workspace", "work/demo"));
        Path dataRoot = workspace.resolve("data");
        Files.createDirectories(workspace);
        DataGenerator generator = new DataGenerator(Long.parseLong(cli.option("seed", "42")));
        generator.generate(
                dataRoot,
                cli.optionInt("query-count", 128),
                cli.optionInt("reference-records-per-shard", 40000),
                cli.optionInt("sequence-length", 240),
                cli.optionInt("training-fraction-percent", 2));
        System.out.println("Generated data under " + dataRoot.toAbsolutePath());
    }

    private static void runScheduler(CliArguments cli) throws Exception {
        runSchedulerInternal(
                Path.of(cli.option("workspace", "work/run")),
                Path.of(cli.option("data-root", Path.of(cli.option("workspace", "work/run")).resolve("data").toString())),
                Path.of(cli.option("cluster-config", "config/clusters-server.csv")),
                cli.option("scheduler", "wsh"),
                cli.optionInt("max-nodes", 0),
                ExecutionMode.fromCliValue(cli.option("executor", "local")),
                cli.option("docker-image", "gene2life-java:latest"));
    }

    private static RunOutcome runSchedulerInternal(
            Path workspace,
            Path dataRoot,
            Path clusterConfig,
            String schedulerName,
            int maxNodes,
            ExecutionMode executionMode,
            String dockerImage) throws Exception {
        WorkflowDefinition workflow = WorkflowDefinition.gene2life();
        Map<org.gene2life.model.JobId, TaskExecutor> executors = Gene2LifeTaskExecutors.executors();
        List<ClusterProfile> clusters = ClusterProfiles.limitRoundRobin(ClusterConfigLoader.load(clusterConfig), maxNodes);
        DockerNodePool dockerNodePool = executionMode == ExecutionMode.DOCKER
                ? new DockerNodePool(dockerImage, commonAncestor(workspace.toAbsolutePath(), dataRoot.toAbsolutePath()),
                schedulerName + "-" + workspace.getFileName(), clusters.stream().flatMap(cluster -> cluster.nodes().stream()).toList())
                : null;
        Scheduler scheduler = scheduler(schedulerName);
        TrainingBenchmarks benchmarks;
        try {
            benchmarks = schedulerName.equalsIgnoreCase("wsh")
                    ? new TrainingRunner(executors, executionMode, dockerNodePool).benchmark(dataRoot, clusters)
                    : TrainingBenchmarks.empty();
        } catch (Exception exception) {
            if (dockerNodePool != null) {
                dockerNodePool.close();
            }
            throw exception;
        }
        List<PlanAssignment> plan = scheduler.buildPlan(workflow, clusters, benchmarks);
        Path runRoot = workspace.resolve(scheduler.name().toLowerCase());
        WorkflowExecutor executor = new WorkflowExecutor(workflow, executors, clusters, executionMode, dockerNodePool);
        try {
            List<JobRun> runs = executor.execute(dataRoot, runRoot, plan);
            new ReportWriter().writeRunReport(runRoot, workflow, clusters, scheduler.name(), benchmarks, plan, runs);
            System.out.println("Completed " + scheduler.name() + " run under " + runRoot.toAbsolutePath());
            return new RunOutcome(scheduler.name(), runRoot, runs);
        } finally {
            executor.close();
            if (dockerNodePool != null) {
                dockerNodePool.close();
            }
        }
    }

    private static void compare(CliArguments cli) throws Exception {
        Path workspace = Path.of(cli.option("workspace", "work/compare"));
        Path dataRoot = Path.of(cli.option("data-root", workspace.resolve("data").toString()));
        Path clusterConfig = Path.of(cli.option("cluster-config", "config/clusters-server.csv"));
        int rounds = cli.optionInt("rounds", 2);
        int maxNodes = cli.optionInt("max-nodes", 0);
        ExecutionMode executionMode = ExecutionMode.fromCliValue(cli.option("executor", "local"));
        String dockerImage = cli.option("docker-image", "gene2life-java:latest");
        List<RoundOutcome> roundOutcomes = new ArrayList<>();
        for (int round = 1; round <= rounds; round++) {
            boolean wshFirst = round % 2 == 1;
            Path roundWorkspace = workspace.resolve(String.format("round-%02d", round));
            if (wshFirst) {
                RunOutcome wsh = runSchedulerInternal(roundWorkspace, dataRoot, clusterConfig, "wsh", maxNodes, executionMode, dockerImage);
                RunOutcome heft = runSchedulerInternal(roundWorkspace, dataRoot, clusterConfig, "heft", maxNodes, executionMode, dockerImage);
                roundOutcomes.add(new RoundOutcome(round, "WSH->HEFT", wsh, heft));
            } else {
                RunOutcome heft = runSchedulerInternal(roundWorkspace, dataRoot, clusterConfig, "heft", maxNodes, executionMode, dockerImage);
                RunOutcome wsh = runSchedulerInternal(roundWorkspace, dataRoot, clusterConfig, "wsh", maxNodes, executionMode, dockerImage);
                roundOutcomes.add(new RoundOutcome(round, "HEFT->WSH", wsh, heft));
            }
        }
        writeComparisonReport(workspace.resolve("comparison.md"), WorkflowDefinition.gene2life(), roundOutcomes);
        System.out.println("Comparison runs completed under " + workspace.toAbsolutePath());
    }

    private static void runJob(CliArguments cli) throws Exception {
        JobId jobId = JobId.fromCliName(cli.option("job", ""));
        Path primaryInput = Path.of(cli.option("primary-input", ""));
        String secondary = cli.option("secondary-input", "");
        Path secondaryInput = secondary.isBlank() ? null : Path.of(secondary);
        Path outputDir = Path.of(cli.option("output-dir", ""));
        Files.createDirectories(outputDir);
        NodeProfile nodeProfile = new NodeProfile(
                cli.option("cluster-id", "docker-cluster"),
                cli.option("node-id", "docker-node"),
                cli.optionInt("cpu-threads", 1),
                cli.optionInt("io-buffer-kb", 256),
                cli.optionInt("memory-mb", 1024));
        TaskExecutor executor = Gene2LifeTaskExecutors.executors().get(jobId);
        executor.execute(new org.gene2life.task.TaskInputs(primaryInput, secondaryInput, outputDir), nodeProfile);
        System.out.println("Completed job " + jobId.cliName() + " -> " + JobOutputs.outputPath(jobId, outputDir));
    }

    private static Scheduler scheduler(String name) {
        return switch (name.toLowerCase()) {
            case "heft" -> new HeftScheduler();
            case "wsh" -> new WshScheduler();
            default -> throw new IllegalArgumentException("Unsupported scheduler: " + name);
        };
    }

    private static void writeComparisonReport(Path output, WorkflowDefinition workflow, List<RoundOutcome> rounds) throws Exception {
        List<Metrics> wshMetrics = rounds.stream().map(round -> metrics(workflow, round.wsh.runs)).toList();
        List<Metrics> heftMetrics = rounds.stream().map(round -> metrics(workflow, round.heft.runs)).toList();
        Metrics wshAverage = averageMetrics(wshMetrics);
        Metrics heftAverage = averageMetrics(heftMetrics);
        double makespanImprovement = percentageImprovement(heftAverage.makespanMillis, wshAverage.makespanMillis);
        StringBuilder body = new StringBuilder();
        body.append("# Scheduler Comparison\n\n");
        body.append("## Aggregate\n\n");
        body.append("- Rounds: ").append(rounds.size()).append('\n');
        body.append("- WSH average makespan: ").append(wshAverage.makespanMillis).append(" ms\n");
        body.append("- HEFT average makespan: ").append(heftAverage.makespanMillis).append(" ms\n");
        body.append("- WSH average speedup: ").append(String.format("%.4f", wshAverage.speedup)).append('\n');
        body.append("- HEFT average speedup: ").append(String.format("%.4f", heftAverage.speedup)).append('\n');
        body.append("- WSH average scheduling length ratio: ").append(String.format("%.4f", wshAverage.slr)).append('\n');
        body.append("- HEFT average scheduling length ratio: ").append(String.format("%.4f", heftAverage.slr)).append('\n');
        body.append("- WSH makespan improvement over HEFT: ").append(String.format("%.2f%%", makespanImprovement)).append("\n\n");
        body.append("## Rounds\n\n");
        for (RoundOutcome round : rounds) {
            Metrics wsh = metrics(workflow, round.wsh.runs);
            Metrics heft = metrics(workflow, round.heft.runs);
            body.append("### Round ").append(round.roundNumber).append(" (").append(round.order).append(")\n\n");
            body.append("- WSH makespan: ").append(wsh.makespanMillis).append(" ms\n");
            body.append("- WSH run directory: ").append(round.wsh.runRoot.toAbsolutePath()).append('\n');
            body.append("- HEFT makespan: ").append(heft.makespanMillis).append(" ms\n");
            body.append("- HEFT run directory: ").append(round.heft.runRoot.toAbsolutePath()).append("\n\n");
        }
        Files.writeString(output, body, StandardCharsets.UTF_8);
    }

    private static Metrics metrics(WorkflowDefinition workflow, List<JobRun> runs) {
        long makespan = runs.stream().mapToLong(JobRun::actualFinishMillis).max().orElse(0L)
                - runs.stream().mapToLong(JobRun::actualStartMillis).min().orElse(0L);
        long sequential = runs.stream().mapToLong(JobRun::durationMillis).sum();
        Map<JobId, Long> durations = new java.util.EnumMap<>(JobId.class);
        for (JobRun run : runs) {
            durations.put(run.jobId(), run.durationMillis());
        }
        long criticalPath = criticalPath(workflow, durations);
        double speedup = makespan == 0 ? 0.0 : (double) sequential / makespan;
        double slr = criticalPath == 0 ? 0.0 : Math.max(1.0, (double) makespan / criticalPath);
        return new Metrics(makespan, speedup, slr);
    }

    private static long criticalPath(WorkflowDefinition workflow, Map<JobId, Long> durations) {
        Map<JobId, Long> cache = new java.util.EnumMap<>(JobId.class);
        long max = 0L;
        for (var job : workflow.jobs()) {
            max = Math.max(max, criticalPath(job.id(), workflow, durations, cache));
        }
        return max;
    }

    private static long criticalPath(
            JobId jobId,
            WorkflowDefinition workflow,
            Map<JobId, Long> durations,
            Map<JobId, Long> cache) {
        if (cache.containsKey(jobId)) {
            return cache.get(jobId);
        }
        long own = durations.getOrDefault(jobId, 0L);
        long successor = workflow.successors(jobId).stream()
                .mapToLong(job -> criticalPath(job.id(), workflow, durations, cache))
                .max()
                .orElse(0L);
        long value = own + successor;
        cache.put(jobId, value);
        return value;
    }

    private static double percentageImprovement(long baseline, long candidate) {
        if (baseline <= 0) {
            return 0.0;
        }
        return ((double) baseline - candidate) * 100.0 / baseline;
    }

    private static Path commonAncestor(Path left, Path right) {
        Path normalizedLeft = left.normalize();
        Path normalizedRight = right.normalize();
        Path candidate = normalizedLeft;
        while (candidate != null && !normalizedRight.startsWith(candidate)) {
            candidate = candidate.getParent();
        }
        if (candidate == null) {
            throw new IllegalArgumentException("Unable to determine common ancestor for " + left + " and " + right);
        }
        return candidate;
    }

    private static Metrics averageMetrics(List<Metrics> values) {
        double makespan = values.stream().mapToLong(Metrics::makespanMillis).average().orElse(0.0);
        double speedup = values.stream().mapToDouble(Metrics::speedup).average().orElse(0.0);
        double slr = values.stream().mapToDouble(Metrics::slr).average().orElse(0.0);
        return new Metrics(Math.round(makespan), speedup, slr);
    }

    private record RunOutcome(String schedulerName, Path runRoot, List<JobRun> runs) {
    }

    private record RoundOutcome(int roundNumber, String order, RunOutcome wsh, RunOutcome heft) {
    }

    private record Metrics(long makespanMillis, double speedup, double slr) {
    }
}
