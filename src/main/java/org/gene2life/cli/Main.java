package org.gene2life.cli;

import org.gene2life.config.ClusterConfigLoader;
import org.gene2life.config.ClusterProfiles;
import org.gene2life.execution.DockerNodePool;
import org.gene2life.execution.ExecutionMode;
import org.gene2life.execution.JobOutputs;
import org.gene2life.execution.TrainingRunner;
import org.gene2life.execution.WorkflowExecutor;
import org.gene2life.hadoop.HadoopExecutionConfig;
import org.gene2life.hadoop.HadoopJobRunner;
import org.gene2life.hadoop.HadoopSupport;
import org.gene2life.hadoop.HdfsDockerJobRunner;
import org.gene2life.model.ClusterProfile;
import org.gene2life.model.JobRun;
import org.gene2life.model.NodeProfile;
import org.gene2life.model.PlanAssignment;
import org.gene2life.model.WorkflowDefinition;
import org.gene2life.report.ReportWriter;
import org.gene2life.report.WorkflowMetrics;
import org.gene2life.report.WorkflowMetrics.RunMetrics;
import org.gene2life.scheduler.HeftScheduler;
import org.gene2life.scheduler.Scheduler;
import org.gene2life.scheduler.TrainingBenchmarks;
import org.gene2life.scheduler.WshScheduler;
import org.gene2life.task.TaskExecutor;
import org.gene2life.task.TaskInputs;
import org.gene2life.workflow.WorkflowRegistry;
import org.gene2life.workflow.WorkflowSpec;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
        WorkflowSpec workflowSpec = workflowSpec(cli);
        Path workspace = Path.of(cli.option("workspace", "work/demo"));
        Path dataRoot = Path.of(cli.option("data-root", workspace.resolve("data").toString()));
        Files.createDirectories(dataRoot);
        workflowSpec.generateData(dataRoot, cli);
        System.out.println("Generated " + workflowSpec.workflowId() + " data under " + dataRoot.toAbsolutePath());
    }

    private static void runScheduler(CliArguments cli) throws Exception {
        WorkflowSpec workflowSpec = workflowSpec(cli);
        Path workspace = Path.of(cli.option("workspace", "work/run"));
        runSchedulerInternal(
                workflowSpec,
                workspace,
                Path.of(cli.option("data-root", workspace.resolve("data").toString())),
                Path.of(cli.option("cluster-config", "config/clusters-server.csv")),
                cli.option("scheduler", "wsh"),
                cli.optionInt("max-nodes", 0),
                ExecutionMode.fromCliValue(cli.option("executor", "local")),
                cli.option("docker-image", "gene2life-java:latest"),
                cli.option("hdfs-data-root", ""),
                cli.option("hdfs-work-root", ""),
                cli.option("hadoop-conf-dir", System.getenv().getOrDefault("HADOOP_CONF_DIR", "")),
                cli.option("hadoop-fs-default", System.getenv().getOrDefault("HADOOP_FS_DEFAULT", "")),
                cli.option("hadoop-framework-name", System.getenv().getOrDefault("HADOOP_FRAMEWORK_NAME", "yarn")),
                cli.option("hadoop-yarn-rm", System.getenv().getOrDefault("HADOOP_YARN_RM", "")),
                cli.optionBoolean("hadoop-enable-node-labels",
                        Boolean.parseBoolean(System.getenv().getOrDefault("HADOOP_ENABLE_NODE_LABELS", "false"))),
                cli.optionInt("training-warmup-runs", 1),
                cli.optionInt("training-measure-runs", 3));
    }

    private static RunOutcome runSchedulerInternal(
            WorkflowSpec workflowSpec,
            Path workspace,
            Path dataRoot,
            Path clusterConfig,
            String schedulerName,
            int maxNodes,
            ExecutionMode executionMode,
            String dockerImage,
            String hdfsDataRoot,
            String hdfsWorkRoot,
            String hadoopConfDir,
            String hadoopFsDefault,
            String hadoopFrameworkName,
            String hadoopYarnRm,
            boolean hadoopEnableNodeLabels,
            int trainingWarmupRuns,
            int trainingMeasureRuns) throws Exception {
        WorkflowDefinition workflow = workflowSpec.definition();
        Map<String, TaskExecutor> executors = workflowSpec.executors();
        List<ClusterProfile> clusters = ClusterProfiles.limitRoundRobin(ClusterConfigLoader.load(clusterConfig), maxNodes);
        DockerNodePool dockerNodePool = executionMode == ExecutionMode.DOCKER
                || executionMode == ExecutionMode.HDFS_DOCKER
                ? new DockerNodePool(
                dockerImage,
                commonAncestor(workspace.toAbsolutePath(), dataRoot.toAbsolutePath()),
                workflow.workflowId() + "-" + schedulerName + "-" + workspace.getFileName(),
                clusters.stream().flatMap(cluster -> cluster.nodes().stream()).toList())
                : null;
        Path runRoot = workspace.resolve(schedulerName.toLowerCase());
        HadoopExecutionConfig hadoopExecutionConfig = executionMode == ExecutionMode.HADOOP
                || executionMode == ExecutionMode.HDFS_DOCKER
                ? buildHadoopExecutionConfig(
                workflowSpec,
                workspace,
                hdfsDataRoot,
                hdfsWorkRoot,
                hadoopConfDir,
                hadoopFsDefault,
                hadoopFrameworkName,
                hadoopYarnRm,
                hadoopEnableNodeLabels)
                : null;
        HadoopJobRunner hadoopJobRunner = null;
        HdfsDockerJobRunner hdfsDockerJobRunner = null;
        if (hadoopExecutionConfig != null) {
            System.out.println("Hadoop execution config:"
                    + " confDir=" + hadoopExecutionConfig.hadoopConfDir()
                    + " fsDefault=" + hadoopExecutionConfig.fsDefaultFs()
                    + " yarnRm=" + hadoopExecutionConfig.yarnResourceManagerAddress()
                    + " dataRoot=" + hadoopExecutionConfig.normalizedDataRoot()
                    + " workspaceRoot=" + hadoopExecutionConfig.normalizedWorkspaceRoot());
            HadoopSupport hadoopSupport = new HadoopSupport(hadoopExecutionConfig);
            if (executionMode == ExecutionMode.HADOOP) {
                hadoopJobRunner = new HadoopJobRunner(hadoopSupport);
                hadoopJobRunner.syncDataRoot(dataRoot);
            } else {
                hdfsDockerJobRunner = new HdfsDockerJobRunner(dockerNodePool, hadoopSupport);
                hdfsDockerJobRunner.syncDataRoot(dataRoot);
            }
        }
        try {
            Scheduler scheduler = scheduler(schedulerName);
            TrainingBenchmarks benchmarks = schedulerName.equalsIgnoreCase("wsh")
                    ? new TrainingRunner(
                    workflowSpec,
                    executors,
                    executionMode,
                    dockerNodePool,
                    hadoopJobRunner,
                    hdfsDockerJobRunner,
                    trainingWarmupRuns,
                    trainingMeasureRuns).benchmark(dataRoot, clusters)
                    : TrainingBenchmarks.empty();
            List<PlanAssignment> plan = scheduler.buildPlan(workflow, clusters, benchmarks);
            WorkflowExecutor executor = new WorkflowExecutor(
                    workflowSpec,
                    executors,
                    clusters,
                    executionMode,
                    dockerNodePool,
                    hadoopJobRunner,
                    hdfsDockerJobRunner,
                    hadoopExecutionConfig == null
                            ? ""
                            : hadoopExecutionConfig.normalizedWorkspaceRoot() + "/" + scheduler.name().toLowerCase());
            List<JobRun> runs = executor.execute(dataRoot, runRoot, plan);
            try {
                new ReportWriter().writeRunReport(runRoot, workflow, clusters, scheduler.name(), benchmarks, plan, runs);
                System.out.println("Completed " + scheduler.name() + " " + workflow.workflowId() + " run under "
                        + runRoot.toAbsolutePath());
                return new RunOutcome(scheduler.name(), runRoot, runs);
            } finally {
                executor.close();
            }
        } finally {
            if (dockerNodePool != null) {
                dockerNodePool.close();
            }
        }
    }

    private static void compare(CliArguments cli) throws Exception {
        WorkflowSpec workflowSpec = workflowSpec(cli);
        Path workspace = Path.of(cli.option("workspace", "work/compare"));
        Path dataRoot = Path.of(cli.option("data-root", workspace.resolve("data").toString()));
        Path clusterConfig = Path.of(cli.option("cluster-config", "config/clusters-server.csv"));
        int rounds = cli.optionInt("rounds", 3);
        int maxNodes = cli.optionInt("max-nodes", 0);
        ExecutionMode executionMode = ExecutionMode.fromCliValue(cli.option("executor", "local"));
        String dockerImage = cli.option("docker-image", "gene2life-java:latest");
        String hdfsDataRoot = cli.option("hdfs-data-root", "");
        String hdfsWorkRoot = cli.option("hdfs-work-root", "");
        String hadoopConfDir = cli.option("hadoop-conf-dir", System.getenv().getOrDefault("HADOOP_CONF_DIR", ""));
        String hadoopFsDefault = cli.option("hadoop-fs-default", System.getenv().getOrDefault("HADOOP_FS_DEFAULT", ""));
        String hadoopFrameworkName = cli.option("hadoop-framework-name", System.getenv().getOrDefault("HADOOP_FRAMEWORK_NAME", "yarn"));
        String hadoopYarnRm = cli.option("hadoop-yarn-rm", System.getenv().getOrDefault("HADOOP_YARN_RM", ""));
        boolean hadoopEnableNodeLabels = cli.optionBoolean(
                "hadoop-enable-node-labels",
                Boolean.parseBoolean(System.getenv().getOrDefault("HADOOP_ENABLE_NODE_LABELS", "false")));
        int trainingWarmupRuns = cli.optionInt("training-warmup-runs", 1);
        int trainingMeasureRuns = cli.optionInt("training-measure-runs", 3);
        List<ClusterProfile> clusters = ClusterProfiles.limitRoundRobin(ClusterConfigLoader.load(clusterConfig), maxNodes);
        List<RoundOutcome> roundOutcomes = new ArrayList<>();
        for (int round = 1; round <= rounds; round++) {
            boolean wshFirst = round % 2 == 1;
            Path roundWorkspace = workspace.resolve(String.format("round-%02d", round));
            if (wshFirst) {
                RunOutcome wsh = runSchedulerInternal(
                        workflowSpec,
                        roundWorkspace,
                        dataRoot,
                        clusterConfig,
                        "wsh",
                        maxNodes,
                        executionMode,
                        dockerImage,
                        hdfsDataRoot,
                        hdfsWorkRoot,
                        hadoopConfDir,
                        hadoopFsDefault,
                        hadoopFrameworkName,
                        hadoopYarnRm,
                        hadoopEnableNodeLabels,
                        trainingWarmupRuns,
                        trainingMeasureRuns);
                RunOutcome heft = runSchedulerInternal(
                        workflowSpec,
                        roundWorkspace,
                        dataRoot,
                        clusterConfig,
                        "heft",
                        maxNodes,
                        executionMode,
                        dockerImage,
                        hdfsDataRoot,
                        hdfsWorkRoot,
                        hadoopConfDir,
                        hadoopFsDefault,
                        hadoopFrameworkName,
                        hadoopYarnRm,
                        hadoopEnableNodeLabels,
                        trainingWarmupRuns,
                        trainingMeasureRuns);
                roundOutcomes.add(new RoundOutcome(round, "WSH->HEFT", wsh, heft));
            } else {
                RunOutcome heft = runSchedulerInternal(
                        workflowSpec,
                        roundWorkspace,
                        dataRoot,
                        clusterConfig,
                        "heft",
                        maxNodes,
                        executionMode,
                        dockerImage,
                        hdfsDataRoot,
                        hdfsWorkRoot,
                        hadoopConfDir,
                        hadoopFsDefault,
                        hadoopFrameworkName,
                        hadoopYarnRm,
                        hadoopEnableNodeLabels,
                        trainingWarmupRuns,
                        trainingMeasureRuns);
                RunOutcome wsh = runSchedulerInternal(
                        workflowSpec,
                        roundWorkspace,
                        dataRoot,
                        clusterConfig,
                        "wsh",
                        maxNodes,
                        executionMode,
                        dockerImage,
                        hdfsDataRoot,
                        hdfsWorkRoot,
                        hadoopConfDir,
                        hadoopFsDefault,
                        hadoopFrameworkName,
                        hadoopYarnRm,
                        hadoopEnableNodeLabels,
                        trainingWarmupRuns,
                        trainingMeasureRuns);
                roundOutcomes.add(new RoundOutcome(round, "HEFT->WSH", wsh, heft));
            }
        }
        writeComparisonReport(workspace.resolve("comparison.md"), workflowSpec.definition(), clusters, roundOutcomes);
        System.out.println("Comparison runs completed under " + workspace.toAbsolutePath());
    }

    private static void runJob(CliArguments cli) throws Exception {
        WorkflowSpec workflowSpec = workflowSpec(cli);
        String jobId = cli.option("job", "");
        Path outputDir = Path.of(cli.option("output-dir", ""));
        Files.createDirectories(outputDir);
        TaskExecutor executor = workflowSpec.executors().get(jobId);
        if (executor == null) {
            throw new IllegalArgumentException("Unknown job for workflow " + workflowSpec.workflowId() + ": " + jobId);
        }
        TaskInputs inputs = new TaskInputs(parsePathList(cli.option("inputs", "")), outputDir, parseParams(cli.option("params", "")));
        NodeProfile nodeProfile = new NodeProfile(
                cli.option("cluster-id", "docker-cluster"),
                cli.option("node-id", "docker-node"),
                cli.optionInt("cpu-threads", 1),
                cli.optionInt("io-buffer-kb", 256),
                cli.optionInt("memory-mb", 1024));
        executor.execute(inputs, nodeProfile);
        System.out.println("Completed job " + jobId + " -> " + JobOutputs.outputPath(workflowSpec, jobId, outputDir));
    }

    private static WorkflowSpec workflowSpec(CliArguments cli) {
        return WorkflowRegistry.fromCli(cli);
    }

    private static Scheduler scheduler(String name) {
        return switch (name.toLowerCase()) {
            case "heft" -> new HeftScheduler();
            case "wsh" -> new WshScheduler();
            default -> throw new IllegalArgumentException("Unsupported scheduler: " + name);
        };
    }

    private static void writeComparisonReport(
            Path output,
            WorkflowDefinition workflow,
            List<ClusterProfile> clusters,
            List<RoundOutcome> rounds) throws Exception {
        List<RunMetrics> wshMetrics = rounds.stream().map(round -> WorkflowMetrics.summarize(workflow, clusters, round.wsh.runs)).toList();
        List<RunMetrics> heftMetrics = rounds.stream().map(round -> WorkflowMetrics.summarize(workflow, clusters, round.heft.runs)).toList();
        RunMetrics wshAverage = averageMetrics(wshMetrics);
        RunMetrics heftAverage = averageMetrics(heftMetrics);
        double makespanImprovement = percentageImprovement(heftAverage.makespanMillis(), wshAverage.makespanMillis());
        StringBuilder body = new StringBuilder();
        body.append("# Scheduler Comparison\n\n");
        body.append("## Workflow\n\n");
        body.append("- Workflow: ").append(workflow.displayName()).append(" (`").append(workflow.workflowId()).append("`)\n\n");
        body.append("## Aggregate\n\n");
        body.append("- Rounds: ").append(rounds.size()).append('\n');
        body.append("- WSH average makespan: ").append(wshAverage.makespanMillis()).append(" ms\n");
        body.append("- HEFT average makespan: ").append(heftAverage.makespanMillis()).append(" ms\n");
        body.append("- WSH average speedup: ").append(String.format("%.4f", wshAverage.speedup())).append('\n');
        body.append("- HEFT average speedup: ").append(String.format("%.4f", heftAverage.speedup())).append('\n');
        body.append("- WSH average scheduling length ratio: ").append(String.format("%.4f", wshAverage.slr())).append('\n');
        body.append("- HEFT average scheduling length ratio: ").append(String.format("%.4f", heftAverage.slr())).append('\n');
        body.append("- WSH makespan improvement over HEFT: ").append(String.format("%.2f%%", makespanImprovement)).append("\n\n");
        body.append("## Rounds\n\n");
        for (RoundOutcome round : rounds) {
            RunMetrics wsh = WorkflowMetrics.summarize(workflow, clusters, round.wsh.runs);
            RunMetrics heft = WorkflowMetrics.summarize(workflow, clusters, round.heft.runs);
            body.append("### Round ").append(round.roundNumber).append(" (").append(round.order).append(")\n\n");
            body.append("- WSH makespan: ").append(wsh.makespanMillis()).append(" ms\n");
            body.append("- WSH run directory: ").append(round.wsh.runRoot.toAbsolutePath()).append('\n');
            body.append("- HEFT makespan: ").append(heft.makespanMillis()).append(" ms\n");
            body.append("- HEFT run directory: ").append(round.heft.runRoot.toAbsolutePath()).append("\n\n");
        }
        Files.writeString(output, body, StandardCharsets.UTF_8);
    }

    private static double percentageImprovement(long baseline, long candidate) {
        if (baseline <= 0) {
            return 0.0;
        }
        return ((double) baseline - candidate) * 100.0 / baseline;
    }

    private static HadoopExecutionConfig buildHadoopExecutionConfig(
            WorkflowSpec workflowSpec,
            Path workspace,
            String hdfsDataRoot,
            String hdfsWorkRoot,
            String hadoopConfDir,
            String hadoopFsDefault,
            String hadoopFrameworkName,
            String hadoopYarnRm,
            boolean hadoopEnableNodeLabels) {
        String currentUser = System.getProperty("user.name", "gene2life");
        String defaultRoot = "/user/" + currentUser + "/gene2life";
        String dataRoot = hdfsDataRoot == null || hdfsDataRoot.isBlank()
                ? defaultRoot + "/data/" + workflowSpec.workflowId()
                : workflowSpec.normalizeHdfsPath(hdfsDataRoot);
        String workspaceRoot = hdfsWorkRoot == null || hdfsWorkRoot.isBlank()
                ? defaultRoot + "/work/" + workflowSpec.workflowId() + "/" + sanitizeWorkspace(workspace)
                : workflowSpec.normalizeHdfsPath(hdfsWorkRoot) + "/" + sanitizeWorkspace(workspace);
        return new HadoopExecutionConfig(
                dataRoot,
                workspaceRoot,
                hadoopConfDir,
                hadoopFsDefault,
                hadoopFrameworkName,
                hadoopYarnRm,
                hadoopEnableNodeLabels);
    }

    private static String sanitizeWorkspace(Path workspace) {
        String normalized = workspace.toAbsolutePath().normalize().toString().replace('\\', '/');
        normalized = normalized.replaceAll("[^A-Za-z0-9/_-]", "-");
        normalized = normalized.replaceAll("/+", "/");
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        return normalized.isBlank() ? "default" : normalized;
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

    private static RunMetrics averageMetrics(List<RunMetrics> values) {
        double makespan = values.stream().mapToLong(RunMetrics::makespanMillis).average().orElse(0.0);
        double sequential = values.stream().mapToLong(RunMetrics::sequentialRuntimeMillis).average().orElse(0.0);
        double criticalPath = values.stream().mapToLong(RunMetrics::criticalPathLowerBoundMillis).average().orElse(0.0);
        double speedup = values.stream().mapToDouble(RunMetrics::speedup).average().orElse(0.0);
        double slr = values.stream().mapToDouble(RunMetrics::slr).average().orElse(0.0);
        return new RunMetrics(Math.round(makespan), Math.round(sequential), Math.round(criticalPath), speedup, slr);
    }

    private static List<Path> parsePathList(String raw) {
        if (raw == null || raw.isBlank()) {
            return List.of();
        }
        List<Path> paths = new ArrayList<>();
        for (String value : raw.split(",")) {
            if (!value.isBlank()) {
                paths.add(Path.of(value));
            }
        }
        return List.copyOf(paths);
    }

    private static Map<String, String> parseParams(String raw) {
        if (raw == null || raw.isBlank()) {
            return Map.of();
        }
        Map<String, String> params = new LinkedHashMap<>();
        for (String token : raw.split(",")) {
            if (token.isBlank()) {
                continue;
            }
            int separator = token.indexOf('=');
            if (separator <= 0) {
                throw new IllegalArgumentException("Invalid parameter token: " + token);
            }
            params.put(token.substring(0, separator), token.substring(separator + 1));
        }
        return Map.copyOf(params);
    }

    private record RunOutcome(String schedulerName, Path runRoot, List<JobRun> runs) {
    }

    private record RoundOutcome(int roundNumber, String order, RunOutcome wsh, RunOutcome heft) {
    }
}
