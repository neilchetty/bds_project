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
 * Budget/Cost-Aware Scheduler for cloud cost optimization.
 * 
 * This addresses the practical limitation of unconstrained resource usage.
 * In cloud environments, users have budget constraints and want to minimize
 * costs while meeting performance requirements.
 * 
 * Features:
 * - Per-node cost modeling (e.g., $/hour for different instance types)
 * - Budget constraints (hard and soft)
 * - Cost-performance tradeoff optimization
 * - Cost estimation and reporting
 */
public final class BudgetAwareScheduler implements Scheduler {
    
    private final Scheduler baseScheduler;
    private final Map<String, NodeCostProfile> nodeCosts;
    private final double budgetLimit;
    private final boolean hardBudget;
    
    public BudgetAwareScheduler(Scheduler baseScheduler, double budgetLimit) {
        this(baseScheduler, budgetLimit, new HashMap<>(), false);
    }
    
    public BudgetAwareScheduler(Scheduler baseScheduler, 
                                double budgetLimit,
                                Map<String, NodeCostProfile> nodeCosts,
                                boolean hardBudget) {
        this.baseScheduler = baseScheduler;
        this.budgetLimit = budgetLimit;
        this.nodeCosts = nodeCosts;
        this.hardBudget = hardBudget;
    }
    
    /**
     * Set cost profile for a node.
     * 
     * @param nodeId Node identifier
     * @param costPerSecond Cost in dollars per second of execution
     * @param fixedStartupCost Fixed cost to start using this node
     */
    public void setNodeCost(String nodeId, double costPerSecond, double fixedStartupCost) {
        nodeCosts.put(nodeId, new NodeCostProfile(costPerSecond, fixedStartupCost));
    }
    
    @Override
    public List<PlanAssignment> buildPlan(WorkflowDefinition workflow,
                                          List<ClusterProfile> clusters,
                                          TrainingBenchmarks benchmarks) {
        
        // First, estimate costs with base scheduler
        List<PlanAssignment> basePlan = baseScheduler.buildPlan(workflow, clusters, benchmarks);
        double baseCost = calculatePlanCost(basePlan, workflow, clusters);
        
        if (baseCost <= budgetLimit) {
            // Base plan fits within budget
            return annotateWithCost(basePlan, baseCost);
        }
        
        // Need to optimize for cost
        if (hardBudget) {
            return scheduleWithHardBudget(workflow, clusters, benchmarks);
        } else {
            return scheduleWithSoftBudget(workflow, clusters, benchmarks, basePlan);
        }
    }
    
    /**
     * Schedule with hard budget constraint - must not exceed budget.
     * Uses cheaper nodes and slower execution to save costs.
     */
    private List<PlanAssignment> scheduleWithHardBudget(WorkflowDefinition workflow,
                                                        List<ClusterProfile> clusters,
                                                        TrainingBenchmarks benchmarks) {
        Map<String, Long> nodeAvailable = new HashMap<>();
        Map<String, Long> jobFinish = new HashMap<>();
        Map<String, Double> nodeAccumulatedCost = new HashMap<>();
        List<NodeProfile> allNodes = clusters.stream()
            .flatMap(c -> c.nodes().stream())
            .toList();
        List<PlanAssignment> plan = new ArrayList<>();
        
        double totalCost = 0.0;
        
        // Sort nodes by cost (cheapest first)
        List<NodeProfile> sortedNodes = allNodes.stream()
            .sorted(Comparator.comparingDouble(this::getNodeCostPerSecond))
            .toList();
        
        for (JobDefinition job : workflow.jobs()) {
            Candidate best = null;
            
            for (NodeProfile node : sortedNodes) {
                long est = Math.max(nodeAvailable.getOrDefault(node.nodeId(), 0L),
                                  maxDependencyFinish(job, jobFinish));
                
                long execTime = getDuration(job, node, benchmarks);
                long eft = est + execTime;
                
                // Calculate incremental cost
                double nodeCost = getNodeCostPerSecond(node);
                double incrementalCost = nodeCost * (execTime / 1000.0);
                
                // Check if adding this job exceeds budget
                if (totalCost + incrementalCost > budgetLimit) {
                    continue; // Skip this node, too expensive
                }
                
                // Use EFT as tiebreaker among affordable nodes
                if (best == null || eft < best.eft) {
                    best = new Candidate(node, est, eft, execTime, incrementalCost);
                }
            }
            
            if (best == null) {
                throw new BudgetExceededException(
                    "Cannot schedule job " + job.id() + " within budget " + budgetLimit);
            }
            
            nodeAvailable.put(best.node.nodeId(), best.eft);
            jobFinish.put(job.id(), best.eft);
            totalCost += best.cost;
            nodeAccumulatedCost.merge(best.node.nodeId(), best.cost, Double::sum);
            
            plan.add(new PlanAssignment(
                job.id(),
                best.node.clusterId(),
                best.node.nodeId(),
                best.est,
                best.eft,
                0.0,
                name(),
                String.format("cost-%.4f", best.cost)));
        }
        
        return annotateWithCost(plan, totalCost);
    }
    
    /**
     * Schedule with soft budget - try to minimize cost while keeping reasonable performance.
     */
    private List<PlanAssignment> scheduleWithSoftBudget(WorkflowDefinition workflow,
                                                          List<ClusterProfile> clusters,
                                                          TrainingBenchmarks benchmarks,
                                                          List<PlanAssignment> basePlan) {
        // Use cost-performance tradeoff
        // Assign a "score" that combines makespan and cost
        
        Map<String, Long> nodeAvailable = new HashMap<>();
        Map<String, Long> jobFinish = new HashMap<>();
        List<NodeProfile> allNodes = clusters.stream()
            .flatMap(c -> c.nodes().stream())
            .toList();
        List<PlanAssignment> plan = new ArrayList<>();
        
        double totalCost = 0.0;
        
        for (JobDefinition job : workflow.jobs()) {
            Candidate best = null;
            double bestScore = Double.MAX_VALUE;
            
            for (NodeProfile node : allNodes) {
                long est = Math.max(nodeAvailable.getOrDefault(node.nodeId(), 0L),
                                  maxDependencyFinish(job, jobFinish));
                
                long execTime = getDuration(job, node, benchmarks);
                long eft = est + execTime;
                
                double nodeCost = getNodeCostPerSecond(node);
                double incrementalCost = nodeCost * (execTime / 1000.0);
                
                // Score = weighted combination of finish time and cost
                // Normalize by average values
                double normalizedTime = eft / 1000.0; // in seconds
                double normalizedCost = incrementalCost;
                
                // Weight: 70% time, 30% cost (configurable)
                double score = 0.7 * normalizedTime + 0.3 * normalizedCost * 100;
                
                if (score < bestScore) {
                    bestScore = score;
                    best = new Candidate(node, est, eft, execTime, incrementalCost);
                }
            }
            
            if (best == null) {
                throw new IllegalStateException("No valid schedule found for job " + job.id());
            }
            
            nodeAvailable.put(best.node.nodeId(), best.eft);
            jobFinish.put(job.id(), best.eft);
            totalCost += best.cost;
            
            plan.add(new PlanAssignment(
                job.id(),
                best.node.clusterId(),
                best.node.nodeId(),
                best.est,
                best.eft,
                0.0,
                name(),
                String.format("cost-%.4f-score-%.2f", best.cost, bestScore)));
        }
        
        return annotateWithCost(plan, totalCost);
    }
    
    /**
     * Calculate total cost of a schedule plan.
     */
    public double calculatePlanCost(List<PlanAssignment> plan, 
                                   WorkflowDefinition workflow,
                                   List<ClusterProfile> clusters) {
        double totalCost = 0.0;
        Map<String, Double> nodeUsageSeconds = new HashMap<>();
        
        for (PlanAssignment assignment : plan) {
            NodeProfile node = findNode(assignment.nodeId(), clusters);
            if (node != null) {
                double durationSeconds = (assignment.predictedFinishMillis() - assignment.predictedStartMillis()) / 1000.0;
                double nodeCost = getNodeCostPerSecond(node);
                nodeUsageSeconds.merge(assignment.nodeId(), durationSeconds * nodeCost, Double::sum);
            }
        }
        
        // Add startup costs for used nodes
        for (String nodeId : nodeUsageSeconds.keySet()) {
            NodeProfile node = findNode(nodeId, clusters);
            if (node != null) {
                NodeCostProfile costProfile = nodeCosts.getOrDefault(nodeId, 
                    new NodeCostProfile(0.001, 0.0)); // Default: $0.001/sec
                totalCost += nodeUsageSeconds.get(nodeId) + costProfile.fixedStartupCost;
            }
        }
        
        return totalCost;
    }
    
    @Override
    public String name() {
        return baseScheduler.name() + "-Budget";
    }
    
    private double getNodeCostPerSecond(NodeProfile node) {
        return nodeCosts.getOrDefault(node.nodeId(), 
            new NodeCostProfile(0.001, 0.0)).costPerSecond;
    }
    
    private NodeProfile findNode(String nodeId, List<ClusterProfile> clusters) {
        return clusters.stream()
            .flatMap(c -> c.nodes().stream())
            .filter(n -> n.nodeId().equals(nodeId))
            .findFirst()
            .orElse(null);
    }
    
    private long getDuration(JobDefinition job, NodeProfile node, TrainingBenchmarks benchmarks) {
        if (benchmarks.hasMeasurements(job)) {
            return benchmarks.duration(job, node.clusterId());
        }
        return DurationModel.estimateDuration(job, node);
    }
    
    private long maxDependencyFinish(JobDefinition job, Map<String, Long> jobFinish) {
        return job.dependencies().stream()
            .mapToLong(dep -> jobFinish.getOrDefault(dep, 0L))
            .max()
            .orElse(0L);
    }
    
    private List<PlanAssignment> annotateWithCost(List<PlanAssignment> plan, double totalCost) {
        // Could add cost metadata to assignments here
        System.out.println("Estimated plan cost: $" + String.format("%.4f", totalCost));
        return plan;
    }
    
    /**
     * Cost profile for a node.
     */
    public record NodeCostProfile(double costPerSecond, double fixedStartupCost) {
        public double calculateCost(long durationMillis) {
            return fixedStartupCost + costPerSecond * (durationMillis / 1000.0);
        }
    }
    
    /**
     * Exception when budget is exceeded with hard constraint.
     */
    public static class BudgetExceededException extends RuntimeException {
        public BudgetExceededException(String message) {
            super(message);
        }
    }
    
    private record Candidate(NodeProfile node, long est, long eft, long execTime, double cost) {}
}
