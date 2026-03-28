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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Dynamic/Online Scheduler for workflows that arrive at runtime.
 * 
 * This addresses the limitation of static scheduling where all workflows
 * must be known in advance. In real cloud environments, workflows arrive
 * dynamically and must be scheduled online.
 * 
 * Features:
 * - Accepts workflows arriving at any time
 * - Makes scheduling decisions without knowledge of future workflows
 * - Supports preemption (optional) for critical workflows
 * - Adapts to changing resource conditions
 */
public final class DynamicScheduler {
    
    private final Scheduler baseScheduler;
    private final boolean allowPreemption;
    private final DynamicState state;
    
    public DynamicScheduler(Scheduler baseScheduler) {
        this(baseScheduler, false);
    }
    
    public DynamicScheduler(Scheduler baseScheduler, boolean allowPreemption) {
        this.baseScheduler = baseScheduler;
        this.allowPreemption = allowPreemption;
        this.state = new DynamicState();
    }
    
    /**
     * Submit a workflow for dynamic scheduling.
     * The workflow is queued and will be scheduled based on priority and resource availability.
     * 
     * @param workflowId Unique workflow identifier
     * @param workflow Workflow definition
     * @param priority Priority (higher = more urgent)
     * @return Submission ID
     */
    public String submitWorkflow(String workflowId, WorkflowDefinition workflow, int priority) {
        WorkflowSubmission submission = new WorkflowSubmission(
            workflowId, workflow, priority, System.currentTimeMillis());
        state.pendingWorkflows.offer(submission);
        return submission.submissionId();
    }
    
    /**
     * Schedule the next set of ready jobs across all pending workflows.
     * This should be called periodically or when resources become available.
     * 
     * @param clusters Available clusters
     * @param benchmarks Training benchmarks
     * @return Map of newly scheduled assignments
     */
    public Map<String, List<PlanAssignment>> scheduleNextBatch(
            List<ClusterProfile> clusters,
            TrainingBenchmarks benchmarks) {
        
        Map<String, List<PlanAssignment>> newSchedules = new LinkedHashMap<>();
        
        // Update node availability based on completed jobs
        updateResourceState();
        
        // Process workflows in priority order
        List<WorkflowSubmission> currentBatch = new ArrayList<>();
        WorkflowSubmission submission;
        while ((submission = state.pendingWorkflows.poll()) != null) {
            currentBatch.add(submission);
        }
        
        // Sort by priority
        currentBatch.sort(Comparator.comparingInt(WorkflowSubmission::priority).reversed());
        
        for (WorkflowSubmission ws : currentBatch) {
            // Check if workflow is already complete
            if (isWorkflowComplete(ws)) {
                state.completedWorkflows.put(ws.workflowId(), ws);
                continue;
            }
            
            // Find ready jobs in this workflow
            List<JobDefinition> readyJobs = findReadyJobs(ws);
            
            if (!readyJobs.isEmpty()) {
                // Schedule ready jobs
                List<PlanAssignment> assignments = scheduleJobs(
                    readyJobs, ws, clusters, benchmarks);
                
                if (!assignments.isEmpty()) {
                    newSchedules.put(ws.workflowId(), assignments);
                    
                    // Track scheduled jobs
                    for (PlanAssignment pa : assignments) {
                        state.scheduledJobs.put(pa.jobId(), new ScheduledJobInfo(
                            pa, ws.workflowId(), System.currentTimeMillis()));
                        ws.markJobScheduled(pa.jobId());
                    }
                }
            }
            
            // Re-queue if not complete
            if (!isWorkflowComplete(ws)) {
                state.pendingWorkflows.offer(ws);
            }
        }
        
        return newSchedules;
    }
    
    /**
     * Mark a job as completed (called by executor when job finishes).
     * 
     * @param jobId Completed job ID
     * @param actualFinishTime Actual finish timestamp
     */
    public void markJobComplete(String jobId, long actualFinishTime) {
        ScheduledJobInfo info = state.scheduledJobs.remove(jobId);
        if (info != null) {
            state.nodeAvailable.put(info.assignment.nodeId(), actualFinishTime);
            state.jobFinish.put(jobId, actualFinishTime);
            state.jobAssignments.put(jobId, info.assignment.nodeId());
            
            WorkflowSubmission ws = findWorkflowSubmission(info.workflowId);
            if (ws != null) {
                ws.markJobComplete(jobId);
            }
        }
    }
    
    /**
     * Mark a job as failed (called by executor when job fails).
     * 
     * @param jobId Failed job ID
     * @param canRetry Whether the job can be retried
     */
    public void markJobFailed(String jobId, boolean canRetry) {
        ScheduledJobInfo info = state.scheduledJobs.remove(jobId);
        if (info != null) {
            WorkflowSubmission ws = findWorkflowSubmission(info.workflowId);
            if (ws != null) {
                if (canRetry) {
                    ws.markJobReadyToRetry(jobId);
                } else {
                    ws.markWorkflowFailed();
                }
            }
        }
    }
    
    /**
     * Get current scheduling statistics.
     */
    public DynamicStatistics getStatistics() {
        return new DynamicStatistics(
            state.pendingWorkflows.size(),
            state.scheduledJobs.size(),
            state.completedWorkflows.size(),
            state.scheduledJobs.values().stream()
                .filter(sj -> sj.startTime > 0)
                .count(),
            state.nodeAvailable
        );
    }
    
    private void updateResourceState() {
        // Clean up old scheduled jobs that may have completed
        // In a real implementation, this would poll the executor
    }
    
    private boolean isWorkflowComplete(WorkflowSubmission ws) {
        return ws.completedJobs.containsAll(
            ws.workflow.jobs().stream().map(JobDefinition::id).toList());
    }
    
    private List<JobDefinition> findReadyJobs(WorkflowSubmission ws) {
        List<JobDefinition> ready = new ArrayList<>();
        
        for (JobDefinition job : ws.workflow.jobs()) {
            String jobId = job.id();
            
            // Skip if already scheduled or completed
            if (ws.scheduledJobs.contains(jobId) || ws.completedJobs.contains(jobId)) {
                continue;
            }
            
            // Check if dependencies are satisfied
            boolean depsSatisfied = job.dependencies().stream()
                .allMatch(depId -> ws.completedJobs.contains(depId));
            
            if (depsSatisfied) {
                ready.add(job);
            }
        }
        
        return ready;
    }
    
    private List<PlanAssignment> scheduleJobs(
            List<JobDefinition> jobs,
            WorkflowSubmission ws,
            List<ClusterProfile> clusters,
            TrainingBenchmarks benchmarks) {
        
        List<PlanAssignment> assignments = new ArrayList<>();
        List<NodeProfile> allNodes = clusters.stream()
            .flatMap(c -> c.nodes().stream())
            .toList();
        
        // Sort jobs by priority (using upward rank approximation)
        Map<String, Double> ranks = computeRanks(jobs, ws.workflow, benchmarks);
        jobs.sort(Comparator.<JobDefinition>comparingDouble(j -> ranks.getOrDefault(j.id(), 0.0)).reversed());
        
        for (JobDefinition job : jobs) {
            // Find best node using current resource state
            NodeProfile bestNode = null;
            long bestEft = Long.MAX_VALUE;
            long bestEst = 0;
            
            for (NodeProfile node : allNodes) {
                long nodeAvail = state.nodeAvailable.getOrDefault(node.nodeId(), 0L);
                long depFinish = maxDepFinishWithComm(job, ws.workflow);
                long est = Math.max(nodeAvail, depFinish);
                
                long execTime = benchmarks.hasMeasurements(job)
                    ? benchmarks.duration(job, node.clusterId())
                    : DurationModel.estimateDuration(job, node);
                
                long eft = est + execTime;
                
                if (eft < bestEft) {
                    bestEft = eft;
                    bestNode = node;
                    bestEst = est;
                }
            }
            
            if (bestNode != null) {
                PlanAssignment pa = new PlanAssignment(
                    job.id(),
                    bestNode.clusterId(),
                    bestNode.nodeId(),
                    bestEst,
                    bestEft,
                    ranks.getOrDefault(job.id(), 0.0),
                    baseScheduler.name() + "-Dynamic",
                    job.taskType().defaultClassification());
                
                assignments.add(pa);
                
                // Reserve resources
                state.nodeAvailable.put(bestNode.nodeId(), bestEft);
            }
        }
        
        return assignments;
    }
    
    private long maxDepFinishWithComm(JobDefinition job, WorkflowDefinition workflow) {
        return job.dependencies().stream()
            .mapToLong(depId -> state.jobFinish.getOrDefault(depId, 0L))
            .max()
            .orElse(0L);
    }
    
    private Map<String, Double> computeRanks(List<JobDefinition> jobs, WorkflowDefinition workflow,
                                              TrainingBenchmarks benchmarks) {
        Map<String, Double> ranks = new HashMap<>();
        
        for (JobDefinition job : jobs) {
            double own = benchmarks.hasMeasurements(job)
                ? benchmarks.averageDuration(job)
                : job.modeledCostMillis();
            
            double maxChild = workflow.successors(job.id()).stream()
                .mapToDouble(succ -> succ.modeledCostMillis())
                .max()
                .orElse(0.0);
            
            ranks.put(job.id(), own + maxChild);
        }
        
        return ranks;
    }
    
    private WorkflowSubmission findWorkflowSubmission(String workflowId) {
        return state.pendingWorkflows.stream()
            .filter(ws -> ws.workflowId().equals(workflowId))
            .findFirst()
            .orElse(null);
    }
    
    /**
     * Internal state for dynamic scheduling.
     */
    private static class DynamicState {
        final PriorityBlockingQueue<WorkflowSubmission> pendingWorkflows = new PriorityBlockingQueue<>();
        final ConcurrentHashMap<String, ScheduledJobInfo> scheduledJobs = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, WorkflowSubmission> completedWorkflows = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, Long> nodeAvailable = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, Long> jobFinish = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, String> jobAssignments = new ConcurrentHashMap<>();
    }
    
    /**
     * Represents a submitted workflow in the dynamic scheduler.
     */
    public static class WorkflowSubmission implements Comparable<WorkflowSubmission> {
        private final String submissionId;
        private final String workflowId;
        private final WorkflowDefinition workflow;
        private final int priority;
        private final long submissionTime;
        final java.util.Set<String> scheduledJobs = ConcurrentHashMap.newKeySet();
        final java.util.Set<String> completedJobs = ConcurrentHashMap.newKeySet();
        private volatile boolean failed = false;
        
        WorkflowSubmission(String workflowId, WorkflowDefinition workflow, int priority, long submissionTime) {
            this.submissionId = workflowId + "-" + System.nanoTime();
            this.workflowId = workflowId;
            this.workflow = workflow;
            this.priority = priority;
            this.submissionTime = submissionTime;
        }
        
        public String submissionId() { return submissionId; }
        public String workflowId() { return workflowId; }
        public WorkflowDefinition workflow() { return workflow; }
        public int priority() { return priority; }
        public long submissionTime() { return submissionTime; }
        public boolean isFailed() { return failed; }
        
        void markJobScheduled(String jobId) {
            scheduledJobs.add(jobId);
        }
        
        void markJobComplete(String jobId) {
            completedJobs.add(jobId);
        }
        
        void markJobReadyToRetry(String jobId) {
            scheduledJobs.remove(jobId);
        }
        
        void markWorkflowFailed() {
            failed = true;
        }
        
        @Override
        public int compareTo(WorkflowSubmission other) {
            return Comparator.comparingInt(WorkflowSubmission::priority).reversed()
                .thenComparingLong(WorkflowSubmission::submissionTime)
                .compare(this, other);
        }
    }
    
    /**
     * Information about a scheduled job.
     */
    public record ScheduledJobInfo(
            PlanAssignment assignment,
            String workflowId,
            long startTime) {}
    
    /**
     * Statistics for the dynamic scheduler.
     */
    public record DynamicStatistics(
            int pendingWorkflowCount,
            int scheduledJobCount,
            int completedWorkflowCount,
            long runningJobCount,
            Map<String, Long> nodeAvailability) {}
}
