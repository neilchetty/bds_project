package org.gene2life.model;

public record PlanAssignment(
        String jobId,
        String clusterId,
        String nodeId,
        long predictedStartMillis,
        long predictedFinishMillis,
        double upwardRank,
        String schedulerName,
        String classification) {
}
