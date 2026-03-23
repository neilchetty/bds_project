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

public final class WshScheduler implements Scheduler {
    @Override
    public List<PlanAssignment> buildPlan(WorkflowDefinition workflow, List<ClusterProfile> clusters, TrainingBenchmarks benchmarks) {
        Map<JobId, Double> ranks = computeUpwardRanks(workflow, benchmarks);
        List<JobDefinition> ordered = workflow.jobs().stream()
                .sorted(Comparator.<JobDefinition>comparingDouble(def -> ranks.get(def.id())).reversed()
                        .thenComparing(def -> def.id().ordinal()))
                .toList();
        Map<String, Long> nodeAvailable = new HashMap<>();
        Map<JobId, Long> jobFinish = new EnumMap<>(JobId.class);
        Map<String, Integer> activatedNodesPerCluster = new HashMap<>();
        List<PlanAssignment> plan = new ArrayList<>();
        for (JobDefinition job : ordered) {
            List<String> sortedClusters = benchmarks.sortedClusters(job.id(), clusters);
            List<NodeProfile> candidates = candidateNodes(sortedClusters, clusters, activatedNodesPerCluster);
            Candidate best = null;
            for (NodeProfile node : candidates) {
                long est = Math.max(nodeAvailable.getOrDefault(node.nodeId(), 0L), maxDependencyFinish(job, jobFinish));
                long eft = est + benchmarks.duration(job.id(), node.clusterId());
                Candidate candidate = new Candidate(node, est, eft);
                if (best == null || candidate.eft < best.eft) {
                    best = candidate;
                }
            }
            nodeAvailable.put(best.node.nodeId(), best.eft);
            jobFinish.put(job.id(), best.eft);
            String bestClusterId = best.node.clusterId();
            ClusterProfile cluster = clusters.stream().filter(item -> item.clusterId().equals(bestClusterId)).findFirst().orElseThrow();
            int activated = activatedNodesPerCluster.getOrDefault(cluster.clusterId(), 0);
            if (activated < cluster.nodes().size() && cluster.nodes().get(activated).nodeId().equals(best.node.nodeId())) {
                activatedNodesPerCluster.put(cluster.clusterId(), activated + 1);
            }
            if (isLastNodeOfLastCluster(best.node, sortedClusters, clusters)) {
                for (ClusterProfile item : clusters) {
                    activatedNodesPerCluster.put(item.clusterId(), item.nodes().size());
                }
            }
            plan.add(new PlanAssignment(
                    job.id(),
                    best.node.clusterId(),
                    best.node.nodeId(),
                    best.est,
                    best.eft,
                    ranks.get(job.id()),
                    name(),
                    benchmarks.classification(job.id())));
        }
        return plan;
    }

    @Override
    public String name() {
        return "WSH";
    }

    private List<NodeProfile> candidateNodes(
            List<String> sortedClusterIds,
            List<ClusterProfile> clusters,
            Map<String, Integer> activatedNodesPerCluster) {
        List<NodeProfile> candidates = new ArrayList<>();
        for (String clusterId : sortedClusterIds) {
            ClusterProfile cluster = clusters.stream().filter(item -> item.clusterId().equals(clusterId)).findFirst().orElseThrow();
            int activated = activatedNodesPerCluster.getOrDefault(clusterId, 0);
            if (activated >= cluster.nodes().size()) {
                candidates.addAll(cluster.nodes());
                continue;
            }
            candidates.addAll(cluster.nodes().subList(0, activated));
            candidates.add(cluster.nodes().get(activated));
            break;
        }
        if (candidates.isEmpty()) {
            return clusters.stream().flatMap(cluster -> cluster.nodes().stream()).toList();
        }
        return candidates;
    }

    private long maxDependencyFinish(JobDefinition job, Map<JobId, Long> jobFinish) {
        return job.dependencies().stream().mapToLong(dep -> jobFinish.getOrDefault(dep, 0L)).max().orElse(0L);
    }

    private boolean isLastNodeOfLastCluster(NodeProfile node, List<String> sortedClusterIds, List<ClusterProfile> clusters) {
        if (sortedClusterIds.isEmpty()) {
            return false;
        }
        String lastClusterId = sortedClusterIds.get(sortedClusterIds.size() - 1);
        if (!lastClusterId.equals(node.clusterId())) {
            return false;
        }
        ClusterProfile cluster = clusters.stream().filter(item -> item.clusterId().equals(lastClusterId)).findFirst().orElseThrow();
        return cluster.nodes().get(cluster.nodes().size() - 1).nodeId().equals(node.nodeId());
    }

    private Map<JobId, Double> computeUpwardRanks(WorkflowDefinition workflow, TrainingBenchmarks benchmarks) {
        Map<JobId, Double> ranks = new EnumMap<>(JobId.class);
        for (JobDefinition job : workflow.jobs()) {
            computeRank(job.id(), workflow, benchmarks, ranks);
        }
        return ranks;
    }

    private double computeRank(
            JobId jobId,
            WorkflowDefinition workflow,
            TrainingBenchmarks benchmarks,
            Map<JobId, Double> ranks) {
        if (ranks.containsKey(jobId)) {
            return ranks.get(jobId);
        }
        double own = benchmarks.averageDuration(jobId);
        double successor = workflow.successors(jobId).stream()
                .mapToDouble(job -> computeRank(job.id(), workflow, benchmarks, ranks))
                .max()
                .orElse(0.0);
        double value = own + successor;
        ranks.put(jobId, value);
        return value;
    }

    private record Candidate(NodeProfile node, long est, long eft) {
    }
}
