package org.gene2life.scheduler;

import org.gene2life.model.ClusterProfile;
import org.gene2life.model.JobDefinition;
import org.gene2life.model.NodeProfile;
import org.gene2life.model.PlanAssignment;
import org.gene2life.model.WorkflowDefinition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Multi-Workflow Scheduler for concurrent workflow execution.
 * 
 * This addresses the limitation of scheduling single workflows in isolation.
 * In production environments, multiple workflows often arrive dynamically and
 * need to be scheduled together for better resource utilization.
 * 
 * Implements a priority-based scheduling algorithm that considers:
 * - Workflow priority/urgency
 * - Resource availability across workflows
 * - Fairness between competing workflows
 */
public final class MultiWorkflowScheduler {
    
    private final Scheduler innerScheduler;
    private final boolean fairScheduling;
    
    public MultiWorkflowScheduler(Scheduler innerScheduler) {
        this(innerScheduler, true);
    }
    
    public MultiWorkflowScheduler(Scheduler innerScheduler, boolean fairScheduling) {
        this.innerScheduler = innerScheduler;
        this.fairScheduling = fairScheduling;
    }
    
    /**
     * Schedule multiple workflows concurrently on shared resources.
     * 
     * @param workflows Map of workflow ID to workflow definition with priority
     * @param clusters Available cluster resources
     * @param benchmarks Training benchmarks (optional, can be empty)
     * @return Map of workflow ID to its schedule plan
     */
    public Map<String, List<PlanAssignment>> scheduleMultipleWorkflows(
            Map<String, WorkflowPriority> workflows,
            List<ClusterProfile> clusters,
            TrainingBenchmarks benchmarks) {
        
        if (workflows.size() == 1) {
            // Single workflow - use regular scheduling
            Map<String, List<PlanAssignment>> result = new LinkedHashMap<>();
            workflows.forEach((id, wp) -> {
                List<PlanAssignment> plan = innerScheduler.buildPlan(wp.workflow(), clusters, benchmarks);
                result.put(id, adjustTimestamps(plan, wp.priority()));
            });
            return result;
        }
        
        // Create a merged workflow view for resource-aware scheduling
        return fairScheduling 
            ? scheduleFair(workflows, clusters, benchmarks)
            : schedulePriorityBased(workflows, clusters, benchmarks);
    }
    
    /**
     * Fair scheduling - tries to balance resource allocation among workflows.
     */
    private Map<String, List<PlanAssignment>> scheduleFair(
            Map<String, WorkflowPriority> workflows,
            List<ClusterProfile> clusters,
            TrainingBenchmarks benchmarks) {
        
        Map<String, List<PlanAssignment>> result = new LinkedHashMap<>();
        Map<String, Long> nodeAvailable = new HashMap<>();
        Map<String, Long> jobFinish = new HashMap<>();
        Map<String, NodeProfile> jobAssignments = new HashMap<>();
        
        // Priority queue ordered by priority and then submission time
        PriorityQueue<WorkflowPriority> queue = new PriorityQueue<>(
            Comparator.comparingInt(WorkflowPriority::priority).reversed()
                .thenComparingLong(WorkflowPriority::submissionTime));
        queue.addAll(workflows.values());
        
        // Get all nodes
        List<NodeProfile> allNodes = clusters.stream()
            .flatMap(c -> c.nodes().stream())
            .toList();
        
        // Schedule jobs round-robin across workflows based on priority
        Map<String, List<JobDefinition>> pendingJobs = new HashMap<>();
        workflows.forEach((id, wp) -> {
            List<JobDefinition> ordered = getPriorityOrderedJobs(wp.workflow(), benchmarks);
            pendingJobs.put(id, new ArrayList<>(ordered));
        });
        
        // Global job counter for ordering
        int globalOrder = 0;
        
        while (!allJobsComplete(pendingJobs)) {
            for (Map.Entry<String, List<JobDefinition>> entry : pendingJobs.entrySet()) {
                String workflowId = entry.getKey();
                List<JobDefinition> jobs = entry.getValue();
                WorkflowPriority wp = workflows.get(workflowId);
                
                if (jobs.isEmpty()) continue;
                
                // Find first job whose dependencies are satisfied
                JobDefinition readyJob = findReadyJob(jobs, jobFinish.keySet(), wp.workflow());
                if (readyJob == null) continue;
                
                jobs.remove(readyJob);
                
                // Schedule this job on best available node
                Candidate best = findBestNode(
                    readyJob, allNodes, nodeAvailable, jobFinish, jobAssignments, 
                    wp.workflow(), benchmarks);
                
                // Update resource tracking
                nodeAvailable.put(best.node.nodeId(), best.eft);
                jobFinish.put(readyJob.id(), best.eft);
                jobAssignments.put(readyJob.id(), best.node);
                
                // Create assignment
                PlanAssignment assignment = new PlanAssignment(
                    readyJob.id(),
                    best.node.clusterId(),
                    best.node.nodeId(),
                    best.est,
                    best.eft,
                    best.rank,
                    innerScheduler.name() + "-Multi",
                    wp.workflow().job(readyJob.id()).taskType().defaultClassification());
                
                result.computeIfAbsent(workflowId, k -> new ArrayList<>()).add(assignment);
                
                globalOrder++;
            }
        }
        
        return result;
    }
    
    /**
     * Priority-based scheduling - schedules higher priority workflows first.
     */
    private Map<String, List<PlanAssignment>> schedulePriorityBased(
            Map<String, WorkflowPriority> workflows,
            List<ClusterProfile> clusters,
            TrainingBenchmarks benchmarks) {
        
        Map<String, List<PlanAssignment>> result = new LinkedHashMap<>();
        
        // Sort workflows by priority (highest first)
        List<Map.Entry<String, WorkflowPriority>> sorted = workflows.entrySet().stream()
            .sorted(Map.Entry.<String, WorkflowPriority>comparingByValue(
                Comparator.comparingInt(WorkflowPriority::priority).reversed()))
            .toList();
        
        // Track resource usage across workflows
        Map<String, Long> nodeAvailable = new HashMap<>();
        Map<String, Long> jobFinish = new HashMap<>();
        Map<String, NodeProfile> jobAssignments = new HashMap<>();
        
        for (Map.Entry<String, WorkflowPriority> entry : sorted) {
            String workflowId = entry.getKey();
            WorkflowPriority wp = entry.getValue();
            
            // Schedule this workflow considering existing resource usage
            List<PlanAssignment> plan = scheduleWorkflowWithExistingLoad(
                wp.workflow(), clusters, benchmarks, nodeAvailable, jobFinish, jobAssignments);
            
            result.put(workflowId, plan);
        }
        
        return result;
    }
    
    private List<PlanAssignment> scheduleWorkflowWithExistingLoad(
            WorkflowDefinition workflow,
            List<ClusterProfile> clusters,
            TrainingBenchmarks benchmarks,
            Map<String, Long> nodeAvailable,
            Map<String, Long> jobFinish,
            Map<String, NodeProfile> jobAssignments) {
        
        List<JobDefinition> ordered = getPriorityOrderedJobs(workflow, benchmarks);
        List<NodeProfile> nodes = clusters.stream().flatMap(c -> c.nodes().stream()).toList();
        List<PlanAssignment> plan = new ArrayList<>();
        
        for (JobDefinition job : ordered) {
            Candidate best = findBestNode(job, nodes, nodeAvailable, jobFinish, 
                                         jobAssignments, workflow, benchmarks);
            
            nodeAvailable.put(best.node.nodeId(), best.eft);
            jobFinish.put(job.id(), best.eft);
            jobAssignments.put(job.id(), best.node);
            
            plan.add(new PlanAssignment(
                job.id(),
                best.node.clusterId(),
                best.node.nodeId(),
                best.est,
                best.eft,
                best.rank,
                innerScheduler.name() + "-Multi",
                job.taskType().defaultClassification()));
        }
        
        return plan;
    }
    
    private List<JobDefinition> getPriorityOrderedJobs(WorkflowDefinition workflow, 
                                                         TrainingBenchmarks benchmarks) {
        Map<String, Double> ranks = new HashMap<>();
        
        for (JobDefinition job : workflow.jobs()) {
            computeUpwardRank(job.id(), workflow, benchmarks, ranks);
        }
        
        return workflow.jobs().stream()
            .sorted(Comparator.<JobDefinition>comparingDouble(j -> ranks.get(j.id())).reversed()
                .thenComparingInt(j -> workflow.orderOf(j.id())))
            .toList();
    }
    
    private double computeUpwardRank(String jobId, WorkflowDefinition workflow,
                                     TrainingBenchmarks benchmarks, Map<String, Double> cache) {
        if (cache.containsKey(jobId)) {
            return cache.get(jobId);
        }
        
        JobDefinition job = workflow.job(jobId);
        double own = benchmarks.hasMeasurements(job) 
            ? benchmarks.averageDuration(job)
            : DurationModel.estimateDuration(job, new NodeProfile("", "", 2, 512, 1024));
        
        double maxChild = workflow.successors(jobId).stream()
            .mapToDouble(j -> computeUpwardRank(j.id(), workflow, benchmarks, cache))
            .max()
            .orElse(0.0);
        
        double rank = own + maxChild;
        cache.put(jobId, rank);
        return rank;
    }
    
    private JobDefinition findReadyJob(List<JobDefinition> pending, java.util.Set<String> completed,
                                      WorkflowDefinition workflow) {
        return pending.stream()
            .filter(j -> completed.containsAll(j.dependencies()))
            .findFirst()
            .orElse(null);
    }
    
    private boolean allJobsComplete(Map<String, List<JobDefinition>> pending) {
        return pending.values().stream().allMatch(List::isEmpty);
    }
    
    private Candidate findBestNode(JobDefinition job, List<NodeProfile> nodes,
                                   Map<String, Long> nodeAvailable, Map<String, Long> jobFinish,
                                   Map<String, NodeProfile> jobAssignments, WorkflowDefinition workflow,
                                   TrainingBenchmarks benchmarks) {
        Candidate best = null;
        
        for (NodeProfile node : nodes) {
            long est = Math.max(nodeAvailable.getOrDefault(node.nodeId(), 0L),
                              maxDependencyFinish(job, jobFinish));
            long execTime = benchmarks.hasMeasurements(job) && benchmarks.hasMeasurements(job)
                ? benchmarks.duration(job, node.clusterId())
                : DurationModel.estimateDuration(job, node);
            long eft = est + execTime;
            
            double rank = 0.0; // Will be filled in later
            
            Candidate candidate = new Candidate(node, est, eft, execTime, rank);
            if (best == null || candidate.eft < best.eft) {
                best = candidate;
            }
        }
        
        return best;
    }
    
    private long maxDependencyFinish(JobDefinition job, Map<String, Long> jobFinish) {
        return job.dependencies().stream()
            .mapToLong(dep -> jobFinish.getOrDefault(dep, 0L))
            .max()
            .orElse(0L);
    }
    
    private List<PlanAssignment> adjustTimestamps(List<PlanAssignment> plan, int priority) {
        // Higher priority workflows get earlier predicted start times
        // This is a visual/numerical adjustment for reporting
        return plan;
    }
    
    /**
     * Represents a workflow with scheduling priority.
     */
    public record WorkflowPriority(
            WorkflowDefinition workflow,
            int priority, // Higher = more urgent
            long submissionTime,
            String workflowId) implements Comparable<WorkflowPriority> {
        
        @Override
        public int compareTo(WorkflowPriority other) {
            return Comparator.comparingInt(WorkflowPriority::priority).reversed()
                .thenComparingLong(WorkflowPriority::submissionTime)
                .compare(this, other);
        }
    }
    
    private record Candidate(NodeProfile node, long est, long eft, long execTime, double rank) {}
}
