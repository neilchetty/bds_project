package org.gene2life.report;

import org.gene2life.model.ClusterProfile;
import org.gene2life.model.JobRun;
import org.gene2life.model.WorkflowDefinition;
import org.gene2life.scheduler.DurationModel;

import java.util.List;

public final class WorkflowMetrics {
    private WorkflowMetrics() {
    }

    public static RunMetrics summarize(
            WorkflowDefinition workflow,
            List<ClusterProfile> clusters,
            List<JobRun> runs) {
        long makespan = runs.stream().mapToLong(JobRun::actualFinishMillis).max().orElse(0L)
                - runs.stream().mapToLong(JobRun::actualStartMillis).min().orElse(0L);
        long sequential = runs.stream().mapToLong(JobRun::durationMillis).sum();
        long criticalPathLowerBound = DurationModel.optimisticCriticalPath(workflow, clusters);
        double speedup = makespan == 0 ? 0.0 : (double) sequential / makespan;
        double slr = criticalPathLowerBound == 0 ? 0.0 : Math.max(1.0, (double) makespan / criticalPathLowerBound);
        return new RunMetrics(makespan, sequential, criticalPathLowerBound, speedup, slr);
    }

    public record RunMetrics(
            long makespanMillis,
            long sequentialRuntimeMillis,
            long criticalPathLowerBoundMillis,
            double speedup,
            double slr) {
    }
}
