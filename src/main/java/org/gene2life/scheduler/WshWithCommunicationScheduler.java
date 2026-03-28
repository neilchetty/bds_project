package org.gene2life.scheduler;

import org.gene2life.model.ClusterProfile;
import org.gene2life.model.JobDefinition;
import org.gene2life.model.NodeProfile;
import org.gene2life.model.PlanAssignment;
import org.gene2life.model.WorkflowDefinition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * WSH (Workflow Scheduling with History) enhanced with Communication Cost awareness.
 * 
 * This combines the paper's WSH algorithm with communication cost modeling,
 * addressing both the original WSH limitations and the general scheduling
 * limitation of ignoring data transfer times.
 */
public final class WshWithCommunicationScheduler implements Scheduler {
    
    private final CommunicationCostModel commModel;
    
    public WshWithCommunicationScheduler() {
        this(new CommunicationCostModel());
    }
    
    public WshWithCommunicationScheduler(CommunicationCostModel commModel) {
        this.commModel = commModel;
    }
    
    @Override
    public List<PlanAssignment> buildPlan(WorkflowDefinition workflow, 
                                          List<ClusterProfile> clusters, 
                                          TrainingBenchmarks benchmarks) {
        // Compute upward ranks including communication costs
        Map<String, Double> ranks = computeUpwardRanksWithComm(workflow, benchmarks);
        
        List<JobDefinition> ordered = workflow.jobs().stream()
                .sorted(Comparator.<JobDefinition>comparingDouble(def -> ranks.get(def.id())).reversed()
                        .thenComparingInt(def -> workflow.orderOf(def.id())))
                .toList();
        
        Map<String, Long> nodeAvailable = new HashMap<>();
        Map<String, Long> jobFinish = new HashMap<>();
        Map<String, NodeProfile> jobAssignments = new HashMap<>();
        Map<String, Integer> activatedNodesPerCluster = new HashMap<>();
        List<PlanAssignment> plan = new ArrayList<>();
        
        for (JobDefinition job : ordered) {
            List<String> sortedClusters = benchmarks.sortedClusters(job, clusters);
            List<NodeProfile> candidates = candidateNodes(sortedClusters, clusters, activatedNodesPerCluster);
            
            Candidate best = null;
            for (NodeProfile node : candidates) {
                // Calculate EST with communication costs
                long baseEst = Math.max(nodeAvailable.getOrDefault(node.nodeId(), 0L), 
                                       maxDependencyFinish(job, jobFinish));
                long est = commModel.calculateAftWithCommunication(
                    job, node, baseEst, workflow, jobAssignments, jobFinish);
                
                long eft = est + benchmarks.duration(job, node.clusterId());
                
                Candidate candidate = new Candidate(node, est, eft);
                if (best == null || candidate.eft < best.eft) {
                    best = candidate;
                }
            }
            
            nodeAvailable.put(best.node.nodeId(), best.eft);
            jobFinish.put(job.id(), best.eft);
            jobAssignments.put(job.id(), best.node);
            
            // Track cluster activation
            String bestClusterId = best.node.clusterId();
            ClusterProfile cluster = clusters.stream()
                .filter(item -> item.clusterId().equals(bestClusterId))
                .findFirst().orElseThrow();
            int activated = activatedNodesPerCluster.getOrDefault(cluster.clusterId(), 0);
            if (activated < cluster.nodes().size() && 
                cluster.nodes().get(activated).nodeId().equals(best.node.nodeId())) {
                activatedNodesPerCluster.put(cluster.clusterId(), activated + 1);
            }
            
            plan.add(new PlanAssignment(
                    job.id(),
                    best.node.clusterId(),
                    best.node.nodeId(),
                    best.est,
                    best.eft,
                    ranks.get(job.id()),
                    name(),
                    benchmarks.classification(job)));
        }
        return plan;
    }
    
    @Override
    public String name() {
        return "WSH-Comm";
    }
    
    private List<NodeProfile> candidateNodes(
            List<String> sortedClusterIds,
            List<ClusterProfile> clusters,
            Map<String, Integer> activatedNodesPerCluster) {
        List<NodeProfile> candidates = new ArrayList<>();
        boolean addedNewNode = false;
        for (String clusterId : sortedClusterIds) {
            ClusterProfile cluster = clusters.stream()
                .filter(item -> item.clusterId().equals(clusterId))
                .findFirst().orElseThrow();
            int activated = activatedNodesPerCluster.getOrDefault(clusterId, 0);
            if (activated >= cluster.nodes().size()) {
                candidates.addAll(cluster.nodes());
            } else if (!addedNewNode) {
                candidates.addAll(cluster.nodes().subList(0, activated));
                candidates.add(cluster.nodes().get(activated));
                addedNewNode = true;
            } else {
                if (activated > 0) {
                    candidates.addAll(cluster.nodes().subList(0, activated));
                }
            }
        }
        if (candidates.isEmpty()) {
            return clusters.stream().flatMap(cluster -> cluster.nodes().stream()).toList();
        }
        return candidates;
    }
    
    private long maxDependencyFinish(JobDefinition job, Map<String, Long> jobFinish) {
        return job.dependencies().stream()
                .mapToLong(dep -> jobFinish.getOrDefault(dep, 0L))
                .max()
                .orElse(0L);
    }
    
    private Map<String, Double> computeUpwardRanksWithComm(WorkflowDefinition workflow,
                                                           TrainingBenchmarks benchmarks) {
        Map<String, Double> ranks = new HashMap<>();
        for (JobDefinition job : workflow.jobs()) {
            computeRankWithComm(job.id(), workflow, benchmarks, ranks);
        }
        return ranks;
    }
    
    private double computeRankWithComm(String jobId, WorkflowDefinition workflow,
                                       TrainingBenchmarks benchmarks, Map<String, Double> ranks) {
        if (ranks.containsKey(jobId)) {
            return ranks.get(jobId);
        }
        
        JobDefinition job = workflow.job(jobId);
        double ownExec = benchmarks.averageDuration(job);
        
        // Add average communication cost to successors
        double avgCommCost = workflow.successors(jobId).stream()
            .mapToDouble(succ -> {
                long dataSize = commModel.estimateOutputSize(job);
                // Average cost considering heterogeneous clusters
                return dataSize / (1024.0 * 1024.0) / 100.0 * 1000; // Simplified model
            })
            .average()
            .orElse(0.0);
        
        double maxChild = workflow.successors(jobId).stream()
            .mapToDouble(succ -> computeRankWithComm(succ.id(), workflow, benchmarks, ranks))
            .max()
            .orElse(0.0);
        
        double value = ownExec + avgCommCost + maxChild;
        ranks.put(jobId, value);
        return value;
    }
    
    private record Candidate(NodeProfile node, long est, long eft) {}
}
