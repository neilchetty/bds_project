package org.gene2life.model;

public record PlanAssignment(
        JobId jobId,
        String clusterId,
        String nodeId,
        long predictedStartMillis,
        long predictedFinishMillis,
        double upwardRank,
        String schedulerName,
        String classification) {
}
