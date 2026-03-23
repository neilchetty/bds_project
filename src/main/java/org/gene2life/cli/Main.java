package org.gene2life.cli;

import org.gene2life.config.ClusterConfigLoader;
import org.gene2life.data.DataGenerator;
import org.gene2life.execution.TrainingRunner;
import org.gene2life.execution.WorkflowExecutor;
import org.gene2life.model.ClusterProfile;
import org.gene2life.model.JobId;
import org.gene2life.model.JobRun;
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
                cli.option("scheduler", "wsh"));
    }

    private static RunOutcome runSchedulerInternal(Path workspace, Path dataRoot, Path clusterConfig, String schedulerName) throws Exception {
        WorkflowDefinition workflow = WorkflowDefinition.gene2life();
        Map<org.gene2life.model.JobId, TaskExecutor> executors = Gene2LifeTaskExecutors.executors();
        List<ClusterProfile> clusters = ClusterConfigLoader.load(clusterConfig);
        Scheduler scheduler = scheduler(schedulerName);
        TrainingBenchmarks benchmarks = new TrainingRunner(executors).benchmark(dataRoot, clusters);
        List<PlanAssignment> plan = scheduler.buildPlan(workflow, clusters, benchmarks);
        Path runRoot = workspace.resolve(scheduler.name().toLowerCase());
        WorkflowExecutor executor = new WorkflowExecutor(workflow, executors, clusters);
        try {
            List<JobRun> runs = executor.execute(dataRoot, runRoot, plan);
            new ReportWriter().writeRunReport(runRoot, workflow, clusters, scheduler.name(), benchmarks, plan, runs);
            System.out.println("Completed " + scheduler.name() + " run under " + runRoot.toAbsolutePath());
            return new RunOutcome(scheduler.name(), runRoot, runs);
        } finally {
            executor.close();
        }
    }

    private static void compare(CliArguments cli) throws Exception {
        Path workspace = Path.of(cli.option("workspace", "work/compare"));
        Path dataRoot = Path.of(cli.option("data-root", workspace.resolve("data").toString()));
        Path clusterConfig = Path.of(cli.option("cluster-config", "config/clusters-server.csv"));
        RunOutcome wsh = runSchedulerInternal(workspace, dataRoot, clusterConfig, "wsh");
        RunOutcome heft = runSchedulerInternal(workspace, dataRoot, clusterConfig, "heft");
        writeComparisonReport(workspace.resolve("comparison.md"), WorkflowDefinition.gene2life(), wsh, heft);
        System.out.println("Comparison runs completed under " + workspace.toAbsolutePath());
    }

    private static Scheduler scheduler(String name) {
        return switch (name.toLowerCase()) {
            case "heft" -> new HeftScheduler();
            case "wsh" -> new WshScheduler();
            default -> throw new IllegalArgumentException("Unsupported scheduler: " + name);
        };
    }

    private static void writeComparisonReport(Path output, WorkflowDefinition workflow, RunOutcome left, RunOutcome right) throws Exception {
        Metrics leftMetrics = metrics(workflow, left.runs);
        Metrics rightMetrics = metrics(workflow, right.runs);
        double makespanImprovement = percentageImprovement(rightMetrics.makespanMillis, leftMetrics.makespanMillis);
        String body = """
                # Scheduler Comparison

                ## WSH

                - Makespan: %d ms
                - Speedup: %.4f
                - Scheduling length ratio: %.4f
                - Run directory: %s

                ## HEFT

                - Makespan: %d ms
                - Speedup: %.4f
                - Scheduling length ratio: %.4f
                - Run directory: %s

                ## Delta

                - WSH makespan improvement over HEFT: %.2f%%
                """.formatted(
                leftMetrics.makespanMillis,
                leftMetrics.speedup,
                leftMetrics.slr,
                left.runRoot.toAbsolutePath(),
                rightMetrics.makespanMillis,
                rightMetrics.speedup,
                rightMetrics.slr,
                right.runRoot.toAbsolutePath(),
                makespanImprovement);
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

    private record RunOutcome(String schedulerName, Path runRoot, List<JobRun> runs) {
    }

    private record Metrics(long makespanMillis, double speedup, double slr) {
    }
}
