package org.bds.wsh.io;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

import org.bds.wsh.metrics.MetricSet;
import org.bds.wsh.model.ScheduleResult;
import org.bds.wsh.model.ScheduledTask;

public final class CsvWriter {
    public void writeMetrics(Path path, List<MetricSet> metrics) throws IOException {
        Files.createDirectories(path.getParent());
        StringBuilder builder = new StringBuilder();
        builder.append("workflow,algorithm,node_count,makespan_seconds,slr,speedup\n");
        for (MetricSet metric : metrics) {
            builder.append(metric.workflow()).append(',')
                    .append(metric.algorithm()).append(',')
                    .append(metric.nodeCount()).append(',')
                    .append(format(metric.makespanSeconds())).append(',')
                    .append(format(metric.slr())).append(',')
                    .append(format(metric.speedup())).append('\n');
        }
        Files.writeString(path, builder.toString(), StandardCharsets.UTF_8);
    }

    public void writeSchedule(Path path, ScheduleResult result) throws IOException {
        Files.createDirectories(path.getParent());
        StringBuilder builder = new StringBuilder();
        builder.append("task_id,node_id,cluster_id,start_seconds,finish_seconds\n");
        for (ScheduledTask task : result.scheduledTasks().values()) {
            builder.append(task.taskId()).append(',')
                    .append(task.nodeId()).append(',')
                    .append(task.clusterId()).append(',')
                    .append(format(task.startSeconds())).append(',')
                    .append(format(task.finishSeconds())).append('\n');
        }
        Files.writeString(path, builder.toString(), StandardCharsets.UTF_8);
    }

    private String format(double value) {
        return String.format(Locale.US, "%.5f", value);
    }
}
