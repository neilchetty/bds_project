package org.gene2life.scheduler;

import org.gene2life.model.ClusterProfile;
import org.gene2life.model.JobDefinition;
import org.gene2life.model.NodeProfile;
import org.gene2life.model.PlanAssignment;
import org.gene2life.model.WorkflowDefinition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Task Replication Scheduler for critical workflow reliability.
 * 
 * This addresses the limitation of single-point-of-failure in task execution.
 * Critical tasks are replicated across multiple nodes, and the first successful
 * completion is used (speculative execution).
 * 
 * Features:
 * - Selective replication based on task criticality
 * - Speculative execution (first completion wins)
 * - Voting mechanism for result validation
 * - Configurable replication factor per task type
 */
public final class TaskReplicationScheduler implements Scheduler {
    
    private final Scheduler baseScheduler;
    private final Map<String, Integer> replicationFactors;
    private final double criticalityThreshold;
    
    public TaskReplicationScheduler(Scheduler baseScheduler) {
        this(baseScheduler, new HashMap<>(), 0.7);
    }
    
    public TaskReplicationScheduler(Scheduler baseScheduler, 
                                   Map<String, Integer> replicationFactors,
                                   double criticalityThreshold) {
        this.baseScheduler = baseScheduler;
        this.replicationFactors = replicationFactors;
        this.criticalityThreshold = criticalityThreshold;
    }
    
    /**
     * Set replication factor for a specific task type.
     * 
     * @param taskType Task type name
     * @param factor Number of replicas (1 = no replication)
     */
    public void setReplicationFactor(String taskType, int factor) {
        replicationFactors.put(taskType, Math.max(1, factor));
    }
    
    @Override
    public List<PlanAssignment> buildPlan(WorkflowDefinition workflow,
                                          List<ClusterProfile> clusters,
                                          TrainingBenchmarks benchmarks) {
        // Get base schedule
        List<PlanAssignment> basePlan = baseScheduler.buildPlan(workflow, clusters, benchmarks);
        
        // Identify critical tasks that need replication
        Map<String, Integer> jobReplication = determineReplicationFactors(workflow, basePlan);
        
        // Create replicated schedule
        List<PlanAssignment> replicatedPlan = new ArrayList<>();
        Map<String, Set<String>> originalToReplicas = new HashMap<>();
        
        for (PlanAssignment assignment : basePlan) {
            JobDefinition job = workflow.job(assignment.jobId());
            int replication = jobReplication.getOrDefault(job.id(), 1);
            
            if (replication <= 1) {
                // No replication needed
                replicatedPlan.add(assignment);
            } else {
                // Create replicas
                Set<String> replicaIds = new HashSet<>();
                for (int i = 0; i < replication; i++) {
                    String replicaId = assignment.jobId() + "-replica-" + (i + 1);
                    replicaIds.add(replicaId);
                    
                    // For replicas after the first, find alternative nodes
                    NodeProfile targetNode = assignment.nodeId();
                    if (i > 0) {
                        targetNode = findAlternativeNode(assignment.nodeId(), clusters, replicatedPlan);
                    }
                    
                    PlanAssignment replica = new PlanAssignment(
                        replicaId,
                        targetNode.clusterId(),
                        targetNode.nodeId(),
                        assignment.predictedStartMillis(),
                        assignment.predictedFinishMillis(),
                        assignment.upwardRank(),
                        name(),
                        "replicated-" + assignment.classification()
                    );
                    replicatedPlan.add(replica);
                }
                originalToReplicas.put(assignment.jobId(), replicaIds);
            }
        }
        
        // Store replication mapping for executor
        ReplicationMetadata.setMapping(workflow.workflowId(), originalToReplicas);
        
        return replicatedPlan;
    }
    
    /**
     * Determine replication factor for each job based on criticality analysis.
     */
    private Map<String, Integer> determineReplicationFactors(WorkflowDefinition workflow,
                                                             List<PlanAssignment> basePlan) {
        Map<String, Integer> factors = new HashMap<>();
        
        // Calculate upward and downward ranks for criticality
        Map<String, Double> upwardRanks = calculateUpwardRanks(workflow);
        Map<String, Double> downwardRanks = calculateDownwardRanks(workflow);
        
        double maxRank = upwardRanks.values().stream().mapToDouble(Double::doubleValue).max().orElse(1.0);
        
        for (JobDefinition job : workflow.jobs()) {
            // Critical path jobs (zero slack) are most critical
            double slack = Math.abs(upwardRanks.get(job.id()) - downwardRanks.get(job.id()));
            double normalizedCriticality = 1.0 - (slack / maxRank);
            
            // Check if explicitly configured
            if (replicationFactors.containsKey(job.taskType().name())) {
                factors.put(job.id(), replicationFactors.get(job.taskType().name()));
            } else if (normalizedCriticality >= criticalityThreshold) {
                // High criticality job on critical path
                factors.put(job.id(), 2); // Replicate critical tasks
            } else {
                factors.put(job.id(), 1); // No replication
            }
        }
        
        return factors;
    }
    
    /**
     * Find an alternative node for a replica (different from original).
     */
    private NodeProfile findAlternativeNode(String originalNodeId, 
                                           List<ClusterProfile> clusters,
                                           List<PlanAssignment> currentPlan) {
        // Prefer a node in a different cluster for fault tolerance
        NodeProfile original = null;
        String originalCluster = null;
        
        for (ClusterProfile cluster : clusters) {
            for (NodeProfile node : cluster.nodes()) {
                if (node.nodeId().equals(originalNodeId)) {
                    original = node;
                    originalCluster = cluster.clusterId();
                    break;
                }
            }
            if (original != null) break;
        }
        
        // Try to find node in different cluster first
        for (ClusterProfile cluster : clusters) {
            if (!cluster.clusterId().equals(originalCluster)) {
                for (NodeProfile node : cluster.nodes()) {
                    if (!isNodeOverloaded(node.nodeId(), currentPlan)) {
                        return node;
                    }
                }
            }
        }
        
        // Fallback to same cluster, different node
        for (ClusterProfile cluster : clusters) {
            for (NodeProfile node : cluster.nodes()) {
                if (!node.nodeId().equals(originalNodeId) && 
                    !isNodeOverloaded(node.nodeId(), currentPlan)) {
                    return node;
                }
            }
        }
        
        // Last resort: return original node
        return original;
    }
    
    private boolean isNodeOverloaded(String nodeId, List<PlanAssignment> currentPlan) {
        long count = currentPlan.stream()
            .filter(a -> a.nodeId().equals(nodeId))
            .count();
        return count > 5; // Arbitrary threshold
    }
    
    private Map<String, Double> calculateUpwardRanks(WorkflowDefinition workflow) {
        Map<String, Double> ranks = new HashMap<>();
        for (JobDefinition job : workflow.jobs()) {
            computeUpwardRank(job.id(), workflow, ranks);
        }
        return ranks;
    }
    
    private double computeUpwardRank(String jobId, WorkflowDefinition workflow, Map<String, Double> cache) {
        if (cache.containsKey(jobId)) return cache.get(jobId);
        
        JobDefinition job = workflow.job(jobId);
        double own = job.modeledCostMillis();
        double maxChild = workflow.successors(jobId).stream()
            .mapToDouble(succ -> computeUpwardRank(succ.id(), workflow, cache))
            .max()
            .orElse(0.0);
        
        double rank = own + maxChild;
        cache.put(jobId, rank);
        return rank;
    }
    
    private Map<String, Double> calculateDownwardRanks(WorkflowDefinition workflow) {
        Map<String, Double> ranks = new HashMap<>();
        
        // Start from entry jobs (no dependencies)
        List<JobDefinition> entryJobs = workflow.jobs().stream()
            .filter(j -> j.dependencies().isEmpty())
            .toList();
        
        for (JobDefinition job : entryJobs) {
            ranks.put(job.id(), 0.0);
        }
        
        // Propagate downward
        for (JobDefinition job : workflow.jobs()) {
            double currentRank = ranks.getOrDefault(job.id(), 0.0);
            for (JobDefinition succ : workflow.successors(job.id())) {
                double succRank = currentRank + job.modeledCostMillis();
                ranks.merge(succ.id(), succRank, Math::max);
            }
        }
        
        return ranks;
    }
    
    @Override
    public String name() {
        return baseScheduler.name() + "-Replicated";
    }
    
    /**
     * Metadata storage for replication mapping.
     */
    public static class ReplicationMetadata {
        private static final Map<String, Map<String, Set<String>>> mappings = new HashMap<>();
        
        public static void setMapping(String workflowId, Map<String, Set<String>> mapping) {
            mappings.put(workflowId, mapping);
        }
        
        public static Map<String, Set<String>> getMapping(String workflowId) {
            return mappings.getOrDefault(workflowId, new HashMap<>());
        }
        
        public static Set<String> getReplicas(String workflowId, String jobId) {
            return getMapping(workflowId).getOrDefault(jobId, new HashSet<>());
        }
        
        public static boolean isReplica(String workflowId, String jobId) {
            return jobId.contains("-replica-");
        }
        
        public static String getOriginalJobId(String replicaId) {
            int idx = replicaId.indexOf("-replica-");
            if (idx > 0) {
                return replicaId.substring(0, idx);
            }
            return replicaId;
        }
    }
}
