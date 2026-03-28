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
 * Deadline-Constrained Scheduler that optimizes for meeting workflow deadlines.
 * 
 * This addresses the limitation of unconstrained optimization (only minimizing makespan).
 * In practice, workflows often have deadlines, and meeting them is more important
 * than pure makespan minimization.
 * 
 * Features:
 * - Hard deadlines (must be met, reject if impossible)
 * - Soft deadlines (minimize tardiness)
 * - Deadline-aware priority assignment
 * - Critical path acceleration for urgent workflows
 */
public final class DeadlineConstrainedScheduler implements Scheduler {
    
    private final Scheduler baseScheduler;
    private final Map<String, WorkflowDeadline> deadlines;
    private final boolean allowRejection;
    
    public DeadlineConstrainedScheduler(Scheduler baseScheduler) {
        this(baseScheduler, new HashMap<>(), true);
    }
    
    public DeadlineConstrainedScheduler(Scheduler baseScheduler, 
                                        Map<String, WorkflowDeadline> deadlines,
                                        boolean allowRejection) {
        this.baseScheduler = baseScheduler;
        this.deadlines = deadlines;
        this.allowRejection = allowRejection;
    }
    
    /**
     * Set a deadline for a workflow.
     * 
     * @param workflowId Workflow identifier
     * @param deadlineMillis Deadline in milliseconds from start
     * @param isHard Whether this is a hard deadline (must be met)
     */
    public void setDeadline(String workflowId, long deadlineMillis, boolean isHard) {
        deadlines.put(workflowId, new WorkflowDeadline(deadlineMillis, isHard));
    }
    
    @Override
    public List<PlanAssignment> buildPlan(WorkflowDefinition workflow, 
                                          List<ClusterProfile> clusters, 
                                          TrainingBenchmarks benchmarks) {
        WorkflowDeadline deadline = deadlines.get(workflow.workflowId());
        
        if (deadline == null) {
            // No deadline constraint, use base scheduler
            return baseScheduler.buildPlan(workflow, clusters, benchmarks);
        }
        
        // Calculate critical path duration
        long criticalPathDuration = calculateCriticalPathDuration(workflow, clusters);
        
        // Check feasibility for hard deadlines
        if (deadline.isHard && criticalPathDuration > deadline.deadlineMillis) {
            if (allowRejection) {
                throw new DeadlineNotFeasibleException(
                    "Workflow " + workflow.workflowId() + 
                    " cannot meet hard deadline. Required: " + criticalPathDuration + 
                    " ms, Deadline: " + deadline.deadlineMillis + " ms");
            }
            // Otherwise, proceed with best effort
        }
        
        // Use deadline-aware scheduling
        if (deadline.isHard) {
            return scheduleForHardDeadline(workflow, clusters, benchmarks, deadline);
        } else {
            return scheduleForSoftDeadline(workflow, clusters, benchmarks, deadline);
        }
    }
    
    /**
     * Schedule to guarantee meeting a hard deadline.
     * Uses critical path acceleration and fastest resources for critical tasks.
     */
    private List<PlanAssignment> scheduleForHardDeadline(WorkflowDefinition workflow,
                                                         List<ClusterProfile> clusters,
                                                         TrainingBenchmarks benchmarks,
                                                         WorkflowDeadline deadline) {
        // Calculate slack for each job (how much it can be delayed)
        Map<String, Long> slacks = calculateSlack(workflow, clusters, benchmarks);
        
        // Sort jobs by slack (least slack first - critical path)
        List<JobDefinition> ordered = workflow.jobs().stream()
            .sorted(Comparator.comparingLong(j -> slacks.getOrDefault(j.id(), Long.MAX_VALUE)))
            .toList();
        
        Map<String, Long> nodeAvailable = new HashMap<>();
        Map<String, Long> jobFinish = new HashMap<>();
        List<NodeProfile> allNodes = clusters.stream()
            .flatMap(c -> c.nodes().stream())
            .toList();
        List<PlanAssignment> plan = new ArrayList<>();
        
        long currentTime = 0;
        
        for (JobDefinition job : ordered) {
            long slack = slacks.getOrDefault(job.id(), 0L);
            boolean isCritical = slack == 0;
            
            // For critical jobs, use the fastest available node
            // For non-critical, use regular EFT
            Candidate best = null;
            
            for (NodeProfile node : allNodes) {
                long est = Math.max(nodeAvailable.getOrDefault(node.nodeId(), 0L),
                                  maxDependencyFinish(job, jobFinish));
                
                long execTime = getDuration(job, node, benchmarks);
                long eft = est + execTime;
                
                // Calculate rank based on deadline urgency
                double rank = calculateDeadlineAwareRank(
                    job, workflow, clusters, benchmarks, deadline, eft);
                
                Candidate candidate = new Candidate(node, est, eft, execTime, rank);
                
                // Selection criteria depends on criticality
                if (best == null) {
                    best = candidate;
                } else if (isCritical) {
                    // Critical jobs: minimize execution time
                    if (candidate.execTime < best.execTime) {
                        best = candidate;
                    }
                } else {
                    // Non-critical: minimize EFT as usual
                    if (candidate.eft < best.eft) {
                        best = candidate;
                    }
                }
            }
            
            nodeAvailable.put(best.node.nodeId(), best.eft);
            jobFinish.put(job.id(), best.eft);
            currentTime = Math.max(currentTime, best.eft);
            
            plan.add(new PlanAssignment(
                job.id(),
                best.node.clusterId(),
                best.node.nodeId(),
                best.est,
                best.eft,
                best.rank,
                name(),
                (isCritical ? "critical-" : "") + job.taskType().defaultClassification()));
        }
        
        // Verify deadline is met
        long makespan = plan.stream().mapToLong(PlanAssignment::predictedFinishMillis).max().orElse(0L);
        if (makespan > deadline.deadlineMillis) {
            // Even with optimization, deadline not met - this shouldn't happen for feasible deadlines
            System.err.println("Warning: Hard deadline may not be met. Predicted: " + makespan + 
                             " ms, Deadline: " + deadline.deadlineMillis + " ms");
        }
        
        return plan;
    }
    
    /**
     * Schedule to minimize tardiness for soft deadline.
     */
    private List<PlanAssignment> scheduleForSoftDeadline(WorkflowDefinition workflow,
                                                         List<ClusterProfile> clusters,
                                                         TrainingBenchmarks benchmarks,
                                                         WorkflowDeadline deadline) {
        // Use base scheduler but add penalty for late completion
        List<PlanAssignment> basePlan = baseScheduler.buildPlan(workflow, clusters, benchmarks);
        
        long predictedMakespan = basePlan.stream()
            .mapToLong(PlanAssignment::predictedFinishMillis)
            .max()
            .orElse(0L);
        
        if (predictedMakespan <= deadline.deadlineMillis) {
            // Already meeting deadline
            return basePlan;
        }
        
        // Need to accelerate - re-schedule with deadline awareness
        return scheduleForHardDeadline(workflow, clusters, benchmarks, deadline);
    }
    
    @Override
    public String name() {
        return baseScheduler.name() + "-Deadline";
    }
    
    /**
     * Calculate slack (float) for each job - how much it can be delayed without affecting makespan.
     */
    private Map<String, Long> calculateSlack(WorkflowDefinition workflow,
                                                List<ClusterProfile> clusters,
                                                TrainingBenchmarks benchmarks) {
        // Calculate earliest start times (forward pass)
        Map<String, Long> earliestStart = new HashMap<>();
        Map<String, Long> earliestFinish = new HashMap<>();
        
        for (JobDefinition job : workflow.jobs()) {
            long est = job.dependencies().stream()
                .mapToLong(dep -> earliestFinish.getOrDefault(dep, 0L))
                .max()
                .orElse(0L);
            long eft = est + getAverageDuration(job, clusters, benchmarks);
            earliestStart.put(job.id(), est);
            earliestFinish.put(job.id(), eft);
        }
        
        // Calculate latest start times (backward pass)
        long makespan = earliestFinish.values().stream().mapToLong(Long::longValue).max().orElse(0L);
        Map<String, Long> latestFinish = new HashMap<>();
        Map<String, Long> latestStart = new HashMap<>();
        
        // Initialize exit jobs
        for (JobDefinition job : workflow.jobs()) {
            if (workflow.successors(job.id()).isEmpty()) {
                latestFinish.put(job.id(), makespan);
                latestStart.put(job.id(), makespan - getAverageDuration(job, clusters, benchmarks));
            }
        }
        
        // Backward pass
        List<JobDefinition> reversed = new ArrayList<>(workflow.jobs());
        java.util.Collections.reverse(reversed);
        for (JobDefinition job : reversed) {
            if (!latestFinish.containsKey(job.id())) {
                long minSuccStart = workflow.successors(job.id()).stream()
                    .mapToLong(succ -> latestStart.getOrDefault(succ.id(), makespan))
                    .min()
                    .orElse(makespan);
                latestFinish.put(job.id(), minSuccStart);
                latestStart.put(job.id(), minSuccStart - getAverageDuration(job, clusters, benchmarks));
            }
        }
        
        // Calculate slack = latestStart - earliestStart
        Map<String, Long> slacks = new HashMap<>();
        for (JobDefinition job : workflow.jobs()) {
            long slack = latestStart.getOrDefault(job.id(), 0L) - earliestStart.getOrDefault(job.id(), 0L);
            slacks.put(job.id(), Math.max(0, slack));
        }
        
        return slacks;
    }
    
    private long calculateCriticalPathDuration(WorkflowDefinition workflow, 
                                                List<ClusterProfile> clusters) {
        return DurationModel.optimisticCriticalPath(workflow, clusters);
    }
    
    private double calculateDeadlineAwareRank(JobDefinition job, WorkflowDefinition workflow,
                                               List<ClusterProfile> clusters,
                                               TrainingBenchmarks benchmarks,
                                               WorkflowDeadline deadline,
                                               long predictedFinish) {
        double baseRank = benchmarks.hasMeasurements(job) ? 
            benchmarks.averageDuration(job) : job.modeledCostMillis();
        
        // Add urgency factor if approaching deadline
        long remainingTime = deadline.deadlineMillis - predictedFinish;
        if (remainingTime < 0) {
            // Already past deadline - high urgency
            baseRank *= 10;
        } else if (remainingTime < baseRank * 2) {
            // Approaching deadline - moderate urgency
            baseRank *= 2;
        }
        
        return baseRank;
    }
    
    private long getDuration(JobDefinition job, NodeProfile node, TrainingBenchmarks benchmarks) {
        if (benchmarks.hasMeasurements(job)) {
            return benchmarks.duration(job, node.clusterId());
        }
        return DurationModel.estimateDuration(job, node);
    }
    
    private long getAverageDuration(JobDefinition job, List<ClusterProfile> clusters,
                                     TrainingBenchmarks benchmarks) {
        if (benchmarks.hasMeasurements(job)) {
            return (long) benchmarks.averageDuration(job);
        }
        return clusters.stream()
            .flatMap(c -> c.nodes().stream())
            .mapToLong(n -> DurationModel.estimateDuration(job, n))
            .sum() / Math.max(1, clusters.stream().mapToInt(c -> c.nodes().size()).sum());
    }
    
    private long maxDependencyFinish(JobDefinition job, Map<String, Long> jobFinish) {
        return job.dependencies().stream()
            .mapToLong(dep -> jobFinish.getOrDefault(dep, 0L))
            .max()
            .orElse(0L);
    }
    
    /**
     * Deadline specification for a workflow.
     */
    public record WorkflowDeadline(long deadlineMillis, boolean isHard) {
        public long remainingTime(long currentTimeMillis) {
            return deadlineMillis - currentTimeMillis;
        }
    }
    
    /**
     * Exception thrown when a hard deadline cannot be met.
     */
    public static class DeadlineNotFeasibleException extends RuntimeException {
        public DeadlineNotFeasibleException(String message) {
            super(message);
        }
    }
    
    private record Candidate(NodeProfile node, long est, long eft, long execTime, double rank) {}
}
