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
 * HEFT with Communication Cost awareness.
 * This addresses the paper limitation: "communication cost is ignored".
 * 
 * Communication costs are added to the rank calculation and EFT computation,
 * making the scheduler aware of data transfer times between dependent tasks.
 */
public final class HeftWithCommunicationScheduler implements Scheduler {
    private final CommunicationCostModel commModel;
    
    public HeftWithCommunicationScheduler() {
        this.commModel = new CommunicationCostModel();
    }
    
    public HeftWithCommunicationScheduler(CommunicationCostModel commModel) {
        this.commModel = commModel;
    }
    
    @Override
    public List<PlanAssignment> buildPlan(WorkflowDefinition workflow, List<ClusterProfile> clusters, TrainingBenchmarks benchmarks) {
        // Compute upward ranks with communication costs
        Map<String, Double> ranks = computeUpwardRanksWithCommunication(workflow, clusters);
        
        List<JobDefinition> ordered = workflow.jobs().stream()
                .sorted(Comparator.<JobDefinition>comparingDouble(def -> ranks.get(def.id())).reversed()
                        .thenComparingInt(def -> workflow.orderOf(def.id())))
                .toList();
        
        Map<String, Long> nodeAvailable = new HashMap<>();
        Map<String, Long> jobFinish = new HashMap<>();
        Map<String, NodeProfile> jobAssignments = new HashMap<>();
        List<NodeProfile> nodes = clusters.stream().flatMap(cluster -> cluster.nodes().stream()).toList();
        List<PlanAssignment> plan = new ArrayList<>();
        
        for (JobDefinition job : ordered) {
            Candidate best = null;
            for (NodeProfile node : nodes) {
                // Basic EST based on node availability
                long baseEst = Math.max(nodeAvailable.getOrDefault(node.nodeId(), 0L), 
                                       maxDependencyFinish(job, jobFinish));
                
                // Adjust EST to include communication costs
                long est = commModel.calculateAftWithCommunication(
                    job, node, baseEst, workflow, jobAssignments, jobFinish);
                
                long execTime = estimatedDuration(job, node);
                long eft = est + execTime;
                
                Candidate candidate = new Candidate(node, est, eft, execTime);
                if (best == null || candidate.eft < best.eft) {
                    best = candidate;
                }
            }
            
            nodeAvailable.put(best.node.nodeId(), best.eft);
            jobFinish.put(job.id(), best.eft);
            jobAssignments.put(job.id(), best.node);
            
            plan.add(new PlanAssignment(
                    job.id(),
                    best.node.clusterId(),
                    best.node.nodeId(),
                    best.est,
                    best.eft,
                    ranks.get(job.id()),
                    name(),
                    staticClassification(job)));
        }
        return plan;
    }
    
    @Override
    public String name() {
        return "HEFT-Comm";
    }
    
    /**
     * Compute upward ranks including communication costs in the successor path.
     */
    private Map<String, Double> computeUpwardRanksWithCommunication(WorkflowDefinition workflow, 
                                                                   List<ClusterProfile> clusters) {
        Map<String, Double> ranks = new HashMap<>();
        for (JobDefinition job : workflow.jobs()) {
            computeRankWithComm(job.id(), workflow, clusters, ranks);
        }
        return ranks;
    }
    
    private double computeRankWithComm(String jobId, WorkflowDefinition workflow, 
                                       List<ClusterProfile> clusters, Map<String, Double> ranks) {
        if (ranks.containsKey(jobId)) {
            return ranks.get(jobId);
        }
        
        JobDefinition job = workflow.job(jobId);
        
        // Own execution time (average across all nodes)
        double ownExec = clusters.stream()
                .flatMap(cluster -> cluster.nodes().stream())
                .mapToLong(node -> estimatedDuration(job, node))
                .average()
                .orElse(0.0);
        
        // Average communication cost to successors
        double avgCommCost = workflow.successors(jobId).stream()
                .mapToDouble(successor -> {
                    long dataSize = commModel.estimateOutputSize(job);
                    // Average communication cost across all cluster pairs
                    double avgTransfer = clusters.stream()
                        .flatMap(c1 -> c1.nodes().stream())
                        .flatMap(n1 -> clusters.stream()
                            .flatMap(c2 -> c2.nodes().stream())
                            .filter(n2 -> !n1.nodeId().equals(n2.nodeId()))
                            .mapToLong(n2 -> commModel.estimateTransferTime(n1, n2, dataSize))
                            .average().orElse(0.0))
                        .filter(v -> v > 0)
                        .findFirst().orElse(0.0);
                    return avgTransfer;
                })
                .average()
                .orElse(0.0);
        
        // Recursive successor rank
        double successorRank = workflow.successors(jobId).stream()
                .mapToDouble(successor -> computeRankWithComm(successor.id(), workflow, clusters, ranks))
                .max()
                .orElse(0.0);
        
        double value = ownExec + avgCommCost + successorRank;
        ranks.put(jobId, value);
        return value;
    }
    
    private long maxDependencyFinish(JobDefinition job, Map<String, Long> jobFinish) {
        return job.dependencies().stream()
                .mapToLong(dep -> jobFinish.getOrDefault(dep, 0L))
                .max()
                .orElse(0L);
    }
    
    private long estimatedDuration(JobDefinition job, NodeProfile node) {
        return DurationModel.estimateDuration(job, node);
    }
    
    private String staticClassification(JobDefinition job) {
        return job.taskType().defaultClassification();
    }
    
    private record Candidate(NodeProfile node, long est, long eft, long execTime) {}
}
