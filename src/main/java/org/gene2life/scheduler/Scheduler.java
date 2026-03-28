package org.gene2life.scheduler;

import org.gene2life.model.ClusterProfile;
import org.gene2life.model.PlanAssignment;
import org.gene2life.model.WorkflowDefinition;

import java.util.List;

public interface Scheduler {
    List<PlanAssignment> buildPlan(WorkflowDefinition workflow, List<ClusterProfile> clusters, TrainingBenchmarks benchmarks);

    String name();
}
