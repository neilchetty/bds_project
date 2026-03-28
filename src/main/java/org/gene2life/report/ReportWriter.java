package org.gene2life.report;

import org.gene2life.model.JobDefinition;
import org.gene2life.model.JobRun;
import org.gene2life.model.PlanAssignment;
import org.gene2life.model.ClusterProfile;
import org.gene2life.model.WorkflowDefinition;
import org.gene2life.report.WorkflowMetrics.RunMetrics;
import org.gene2life.scheduler.TrainingBenchmarks;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

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
        RunMetrics metrics = WorkflowMetrics.summarize(workflow, clusters, runs);
        writeSummaryMarkdown(runRoot.resolve("README.md"), workflow, clusters, schedulerName, benchmarks, plan, runs, metrics);
    }

    private void writePlanCsv(Path output, List<PlanAssignment> plan) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("job_id,cluster_id,node_id,predicted_start_ms,predicted_finish_ms,upward_rank,scheduler,classification");
            writer.newLine();
            for (PlanAssignment assignment : plan) {
                writer.write(String.join(",",
                        assignment.jobId(),
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
                        run.jobId(),
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
            List<JobRun> runs,
            RunMetrics metrics) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("# " + schedulerName + " " + workflow.displayName() + " Run");
            writer.newLine();
            writer.newLine();
            writer.write("This run executes the paper-mapped `" + workflow.workflowId() + "` workflow over generated bioinformatics-style data.");
            writer.newLine();
            writer.newLine();
            writer.write("## Metrics");
            writer.newLine();
            writer.newLine();
            writer.write("- Makespan: " + metrics.makespanMillis() + " ms");
            writer.newLine();
            writer.write("- Sequential runtime sum: " + metrics.sequentialRuntimeMillis() + " ms");
            writer.newLine();
            writer.write("- Modeled critical-path lower bound: " + metrics.criticalPathLowerBoundMillis() + " ms");
            writer.newLine();
            writer.write("- Speedup: " + String.format("%.4f", metrics.speedup()));
            writer.newLine();
            writer.write("- Scheduling length ratio: " + String.format("%.4f", metrics.slr()));
            writer.newLine();
            writer.newLine();
            writer.write("## Training Benchmarks");
            writer.newLine();
            writer.newLine();
            if (workflow.jobs().stream().anyMatch(benchmarks::hasMeasurements)) {
                writer.write("- Warmup runs per cluster/job: " + benchmarks.warmupRuns());
                writer.newLine();
                writer.write("- Measured runs per cluster/job: " + benchmarks.measurementRuns());
                writer.newLine();
                writer.newLine();
            }
            for (JobDefinition job : workflow.trainingRepresentativeJobs()) {
                writer.write("- " + job.trainingProfileKey() + " (from " + job.id() + "): ");
                writer.write(plan.stream()
                        .filter(item -> item.jobId().equals(job.id()))
                        .findFirst()
                        .map(PlanAssignment::classification)
                        .orElse(job.taskType().defaultClassification()));
                writer.write(benchmarks.hasMeasurements(job) ? " intensive; cluster order = " : " intensive; cluster order (static) = ");
                writer.write(String.join(" > ", benchmarks.sortedClusters(job, clusters)));
                writer.newLine();
            }
            writer.newLine();
            writer.write("## Outputs");
            writer.newLine();
            writer.newLine();
            for (JobRun run : runs) {
                writer.write("- " + run.jobId() + ": " + run.outputPath());
                writer.newLine();
            }
        }
    }
}
