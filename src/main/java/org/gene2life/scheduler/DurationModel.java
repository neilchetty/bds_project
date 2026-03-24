package org.gene2life.scheduler;

import org.gene2life.model.ClusterProfile;
import org.gene2life.model.JobDefinition;
import org.gene2life.model.JobId;
import org.gene2life.model.NodeProfile;
import org.gene2life.model.WorkflowDefinition;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public final class DurationModel {
    private DurationModel() {
    }

    public static long estimateDuration(JobId jobId, NodeProfile node) {
        double base = switch (jobId) {
            case BLAST1, BLAST2 -> 62_000.0;
            case CLUSTALW1, CLUSTALW2 -> 90_000.0;
            case DNAPARS -> 19_000.0;
            case PROTPARS -> 16_000.0;
            case DRAWGRAM1, DRAWGRAM2 -> 18_000.0;
        };
        double cpuFactor = Math.max(1.0, node.cpuThreads());
        double ioFactor = Math.max(1.0, node.ioBufferKb() / 256.0);
        double estimate = switch (jobId) {
            case BLAST1, BLAST2, CLUSTALW1, CLUSTALW2 -> base / cpuFactor;
            case DNAPARS, PROTPARS -> base / ((cpuFactor * 0.6) + (ioFactor * 0.4));
            case DRAWGRAM1, DRAWGRAM2 -> base / ioFactor;
        };
        return Math.max(1L, Math.round(estimate));
    }

    public static long optimisticCriticalPath(WorkflowDefinition workflow, List<ClusterProfile> clusters) {
        Map<JobId, Long> lowerBounds = new EnumMap<>(JobId.class);
        for (JobDefinition job : workflow.jobs()) {
            long minDuration = clusters.stream()
                    .flatMap(cluster -> cluster.nodes().stream())
                    .mapToLong(node -> estimateDuration(job.id(), node))
                    .min()
                    .orElse(0L);
            lowerBounds.put(job.id(), minDuration);
        }
        return criticalPath(workflow, lowerBounds);
    }

    private static long criticalPath(WorkflowDefinition workflow, Map<JobId, Long> durations) {
        Map<JobId, Long> cache = new EnumMap<>(JobId.class);
        long max = 0L;
        for (JobDefinition job : workflow.jobs()) {
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
}
