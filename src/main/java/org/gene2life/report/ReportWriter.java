package org.gene2life.report;

import org.gene2life.model.JobDefinition;
import org.gene2life.model.JobId;
import org.gene2life.model.JobRun;
import org.gene2life.model.PlanAssignment;
import org.gene2life.model.ClusterProfile;
import org.gene2life.model.WorkflowDefinition;
import org.gene2life.scheduler.TrainingBenchmarks;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public final class ReportWriter {
    public void writeRunReport(
            Path runRoot,
            WorkflowDefinition workflow,
            List<ClusterProfile> clusters,
            String schedulerName,
            TrainingBenchmarks benchmarks,
            List<PlanAssignment> plan,
            List<JobRun> runs) throws IOException {
        Files.createDirectories(runRoot);
        writePlanCsv(runRoot.resolve("schedule-plan.csv"), plan);
        writeRunsCsv(runRoot.resolve("run-metrics.csv"), runs);
        writeSummaryMarkdown(runRoot.resolve("README.md"), workflow, clusters, schedulerName, benchmarks, plan, runs);
    }

    private void writePlanCsv(Path output, List<PlanAssignment> plan) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("job_id,cluster_id,node_id,predicted_start_ms,predicted_finish_ms,upward_rank,scheduler,classification");
            writer.newLine();
            for (PlanAssignment assignment : plan) {
                writer.write(String.join(",",
                        assignment.jobId().cliName(),
                        assignment.clusterId(),
                        assignment.nodeId(),
                        Long.toString(assignment.predictedStartMillis()),
                        Long.toString(assignment.predictedFinishMillis()),
                        String.format("%.4f", assignment.upwardRank()),
                        assignment.schedulerName(),
                        assignment.classification()));
                writer.newLine();
            }
        }
    }

    private void writeRunsCsv(Path output, List<JobRun> runs) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("job_id,cluster_id,node_id,actual_start_ms,actual_finish_ms,duration_ms,output_path,description");
            writer.newLine();
            for (JobRun run : runs) {
                writer.write(String.join(",",
                        run.jobId().cliName(),
                        run.clusterId(),
                        run.nodeId(),
                        Long.toString(run.actualStartMillis()),
                        Long.toString(run.actualFinishMillis()),
                        Long.toString(run.durationMillis()),
                        run.outputPath().toString(),
                        run.outputDescription()));
                writer.newLine();
            }
        }
    }

    private void writeSummaryMarkdown(
            Path output,
            WorkflowDefinition workflow,
            List<ClusterProfile> clusters,
            String schedulerName,
            TrainingBenchmarks benchmarks,
            List<PlanAssignment> plan,
            List<JobRun> runs) throws IOException {
        long makespan = runs.stream().mapToLong(JobRun::actualFinishMillis).max().orElse(0L)
                - runs.stream().mapToLong(JobRun::actualStartMillis).min().orElse(0L);
        long sequential = runs.stream().mapToLong(JobRun::durationMillis).sum();
        Map<JobId, Long> actualDurations = new EnumMap<>(JobId.class);
        for (JobRun run : runs) {
            actualDurations.put(run.jobId(), run.durationMillis());
        }
        long criticalPath = criticalPath(workflow, actualDurations);
        double speedup = makespan == 0 ? 0.0 : (double) sequential / makespan;
        double slr = criticalPath == 0 ? 0.0 : Math.max(1.0, (double) makespan / criticalPath);
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("# " + schedulerName + " Gene2Life Run");
            writer.newLine();
            writer.newLine();
            writer.write("This run uses the paper's 8-job `gene2life` DAG and executes real Java file-processing tasks over generated genomic-like data.");
            writer.newLine();
            writer.newLine();
            writer.write("## Metrics");
            writer.newLine();
            writer.newLine();
            writer.write("- Makespan: " + makespan + " ms");
            writer.newLine();
            writer.write("- Sequential runtime sum: " + sequential + " ms");
            writer.newLine();
            writer.write("- Critical-path lower bound: " + criticalPath + " ms");
            writer.newLine();
            writer.write("- Speedup: " + String.format("%.4f", speedup));
            writer.newLine();
            writer.write("- Scheduling length ratio: " + String.format("%.4f", slr));
            writer.newLine();
            writer.newLine();
            writer.write("## Training Benchmarks");
            writer.newLine();
            writer.newLine();
            for (JobDefinition job : workflow.jobs()) {
                writer.write("- " + job.id().cliName() + ": ");
                writer.write(plan.stream().filter(item -> item.jobId() == job.id()).findFirst().map(PlanAssignment::classification).orElse("compute"));
                writer.write(benchmarks.hasMeasurements(job.id()) ? " intensive; cluster order = " : " intensive; cluster order (static) = ");
                writer.write(String.join(" > ", benchmarks.sortedClusters(job.id(), clusters)));
                writer.newLine();
            }
            writer.newLine();
            writer.write("## Outputs");
            writer.newLine();
            writer.newLine();
            for (JobRun run : runs) {
                writer.write("- " + run.jobId().cliName() + ": " + run.outputPath());
                writer.newLine();
            }
        }
    }

    private long criticalPath(WorkflowDefinition workflow, Map<JobId, Long> durations) {
        Map<JobId, Long> cache = new EnumMap<>(JobId.class);
        long max = 0L;
        for (JobDefinition job : workflow.jobs()) {
            max = Math.max(max, criticalPath(job.id(), workflow, durations, cache));
        }
        return max;
    }

    private long criticalPath(
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
}
