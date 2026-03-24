package org.gene2life.scheduler;

import org.gene2life.model.ClusterProfile;
import org.gene2life.model.JobDefinition;
import org.gene2life.model.JobId;
import org.gene2life.model.NodeProfile;
import org.gene2life.model.PlanAssignment;
import org.gene2life.model.WorkflowDefinition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class HeftScheduler implements Scheduler {
    @Override
    public List<PlanAssignment> buildPlan(WorkflowDefinition workflow, List<ClusterProfile> clusters, TrainingBenchmarks benchmarks) {
        Map<JobId, Double> ranks = computeUpwardRanks(workflow, clusters);
        List<JobDefinition> ordered = workflow.jobs().stream()
                .sorted(Comparator.<JobDefinition>comparingDouble(def -> ranks.get(def.id())).reversed()
                        .thenComparing(def -> def.id().ordinal()))
                .toList();
        Map<String, Long> nodeAvailable = new HashMap<>();
        Map<JobId, Long> jobFinish = new EnumMap<>(JobId.class);
        List<NodeProfile> nodes = clusters.stream().flatMap(cluster -> cluster.nodes().stream()).toList();
        List<PlanAssignment> plan = new ArrayList<>();
        for (JobDefinition job : ordered) {
            Candidate best = null;
            for (NodeProfile node : nodes) {
                long est = Math.max(nodeAvailable.getOrDefault(node.nodeId(), 0L), maxDependencyFinish(job, jobFinish));
                long eft = est + estimatedDuration(job.id(), node);
                Candidate candidate = new Candidate(node, est, eft);
                if (best == null || candidate.eft < best.eft) {
                    best = candidate;
                }
            }
            nodeAvailable.put(best.node.nodeId(), best.eft);
            jobFinish.put(job.id(), best.eft);
            plan.add(new PlanAssignment(
                    job.id(),
                    best.node.clusterId(),
                    best.node.nodeId(),
                    best.est,
                    best.eft,
                    ranks.get(job.id()),
                    name(),
                    staticClassification(job.id())));
        }
        return plan;
    }

    @Override
    public String name() {
        return "HEFT";
    }

    private Map<JobId, Double> computeUpwardRanks(WorkflowDefinition workflow, List<ClusterProfile> clusters) {
        Map<JobId, Double> ranks = new EnumMap<>(JobId.class);
        for (JobDefinition job : workflow.jobs()) {
            computeRank(job.id(), workflow, clusters, ranks);
        }
        return ranks;
    }

    private double computeRank(
            JobId jobId,
            WorkflowDefinition workflow,
            List<ClusterProfile> clusters,
            Map<JobId, Double> ranks) {
        if (ranks.containsKey(jobId)) {
            return ranks.get(jobId);
        }
        double own = clusters.stream()
                .flatMap(cluster -> cluster.nodes().stream())
                .mapToLong(node -> estimatedDuration(jobId, node))
                .average()
                .orElse(0.0);
        double child = workflow.successors(jobId).stream()
                .mapToDouble(successor -> computeRank(successor.id(), workflow, clusters, ranks))
                .max()
                .orElse(0.0);
        double value = own + child;
        ranks.put(jobId, value);
        return value;
    }

    private long maxDependencyFinish(JobDefinition job, Map<JobId, Long> jobFinish) {
        return job.dependencies().stream().mapToLong(dep -> jobFinish.getOrDefault(dep, 0L)).max().orElse(0L);
    }

    private long estimatedDuration(JobId jobId, NodeProfile node) {
        return DurationModel.estimateDuration(jobId, node);
    }

    private String staticClassification(JobId jobId) {
        return switch (jobId) {
            case BLAST1, BLAST2, CLUSTALW1, CLUSTALW2 -> "compute";
            case DNAPARS, PROTPARS, DRAWGRAM1, DRAWGRAM2 -> "io";
        };
    }

    private record Candidate(NodeProfile node, long est, long eft) {
    }
}
