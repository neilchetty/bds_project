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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * QoS (Quality of Service) Aware Scheduler with Admission Control.
 * 
 * This addresses the limitation of uncontrolled resource contention by implementing
 * service tiers, admission control, and resource reservations.
 * 
 * Features:
 * - Multiple service tiers (Premium, Standard, Basic)
 * - Admission control based on resource availability
 * - Resource reservation for high-priority workflows
 * - SLA monitoring and enforcement
 * - Backpressure handling
 */
public final class QoSScheduler implements Scheduler {
    
    public enum ServiceTier {
        PREMIUM(1.0, 0.9),    // Guaranteed resources, 90% allocation priority
        STANDARD(0.7, 0.6),   // Best effort, 60% allocation priority  
        BASIC(0.4, 0.3),      // Opportunistic, 30% allocation priority
        BEST_EFFORT(0.1, 0.1); // Background, 10% allocation priority
        
        private final double resourceGuarantee;
        private final double allocationPriority;
        
        ServiceTier(double resourceGuarantee, double allocationPriority) {
            this.resourceGuarantee = resourceGuarantee;
            this.allocationPriority = allocationPriority;
        }
        
        public double resourceGuarantee() { return resourceGuarantee; }
        public double allocationPriority() { return allocationPriority; }
    }
    
    private final Scheduler baseScheduler;
    private final Map<String, ServiceTier> workflowTiers;
    private final Map<String, QoSMetrics> workflowMetrics;
    private final ResourceCapacity totalCapacity;
    private final Map<String, ResourceAllocation> currentAllocations;
    private final boolean enableAdmissionControl;
    
    // Atomic counters for statistics
    private final AtomicLong acceptedWorkflows = new AtomicLong(0);
    private final AtomicLong rejectedWorkflows = new AtomicLong(0);
    
    public QoSScheduler(Scheduler baseScheduler, ResourceCapacity totalCapacity) {
        this(baseScheduler, totalCapacity, true);
    }
    
    public QoSScheduler(Scheduler baseScheduler, ResourceCapacity totalCapacity, 
                       boolean enableAdmissionControl) {
        this.baseScheduler = baseScheduler;
        this.totalCapacity = totalCapacity;
        this.enableAdmissionControl = enableAdmissionControl;
        this.workflowTiers = new ConcurrentHashMap<>();
        this.workflowMetrics = new ConcurrentHashMap<>();
        this.currentAllocations = new ConcurrentHashMap<>();
    }
    
    /**
     * Assign service tier to a workflow.
     */
    public void setWorkflowTier(String workflowId, ServiceTier tier) {
        workflowTiers.put(workflowId, tier);
    }
    
    /**
     * Submit workflow with admission control.
     * 
     * @return true if accepted, false if rejected due to capacity
     */
    public boolean submitWorkflow(String workflowId, WorkflowDefinition workflow, ServiceTier tier) {
        setWorkflowTier(workflowId, tier);
        
        if (!enableAdmissionControl) {
            acceptedWorkflows.incrementAndGet();
            return true;
        }
        
        // Estimate resource requirements
        ResourceAllocation estimated = estimateResourceRequirements(workflow);
        
        // Check if we can admit this workflow
        if (canAdmitWorkflow(workflowId, tier, estimated)) {
            currentAllocations.put(workflowId, estimated);
            acceptedWorkflows.incrementAndGet();
            return true;
        } else {
            rejectedWorkflows.incrementAndGet();
            return false;
        }
    }
    
    @Override
    public List<PlanAssignment> buildPlan(WorkflowDefinition workflow,
                                          List<ClusterProfile> clusters,
                                          TrainingBenchmarks benchmarks) {
        
        ServiceTier tier = workflowTiers.getOrDefault(workflow.workflowId(), ServiceTier.STANDARD);
        
        // Check if workflow was admitted
        if (enableAdmissionControl && !currentAllocations.containsKey(workflow.workflowId())) {
            throw new AdmissionRejectedException(
                "Workflow " + workflow.workflowId() + " was not admitted");
        }
        
        // Apply tier-specific scheduling strategy
        Map<String, Long> nodeAvailable = new HashMap<>();
        Map<String, Long> jobFinish = new HashMap<>();
        List<NodeProfile> allNodes = clusters.stream()
            .flatMap(c -> c.nodes().stream())
            .toList();
        List<PlanAssignment> plan = new ArrayList<>();
        
        // Tier-aware node selection
        List<NodeProfile> tierNodes = filterNodesByTier(allNodes, tier);
        
        // Get priority based on tier
        List<JobDefinition> ordered = getTierPriorityOrderedJobs(workflow, benchmarks, tier);
        
        long tierDeadline = calculateTierDeadline(workflow, benchmarks, tier);
        
        for (JobDefinition job : ordered) {
            Candidate best = null;
            
            for (NodeProfile node : tierNodes) {
                long est = Math.max(nodeAvailable.getOrDefault(node.nodeId(), 0L),
                                  maxDependencyFinish(job, jobFinish));
                
                long execTime = getDuration(job, node, benchmarks);
                long eft = est + execTime;
                
                // Check deadline constraint for this tier
                boolean meetsDeadline = eft <= tierDeadline;
                
                double score = calculateQoSScore(eft, meetsDeadline, tier, node);
                
                if (best == null || score < best.score) {
                    best = new Candidate(node, est, eft, execTime, score, meetsDeadline);
                }
            }
            
            if (best == null) {
                throw new ResourceExhaustedException(
                    "No suitable node found for job " + job.id() + " in tier " + tier);
            }
            
            nodeAvailable.put(best.node.nodeId(), best.eft);
            jobFinish.put(job.id(), best.eft);
            
            plan.add(new PlanAssignment(
                job.id(),
                best.node.clusterId(),
                best.node.nodeId(),
                best.est,
                best.eft,
                best.score,
                name(),
                String.format("%s-deadline-%s", tier.name(), best.meetsDeadline ? "met" : "missed")));
        }
        
        // Update metrics
        updateQoSMetrics(workflow.workflowId(), plan, tier);
        
        return plan;
    }
    
    private boolean canAdmitWorkflow(String workflowId, ServiceTier tier, 
                                    ResourceAllocation estimated) {
        // Calculate current total allocation
        ResourceAllocation totalUsed = currentAllocations.values().stream()
            .reduce(new ResourceAllocation(0, 0, 0), ResourceAllocation::add);
        
        // Check resource guarantee for this tier
        double guaranteedCpu = totalCapacity.totalCpu() * tier.resourceGuarantee();
        double guaranteedMem = totalCapacity.totalMemory() * tier.resourceGuarantee();
        
        long tierWorkflows = workflowTiers.values().stream()
            .filter(t -> t == tier).count();
        
        // Per-workflow share of guaranteed resources
        double availableCpu = guaranteedCpu / Math.max(1, tierWorkflows);
        double availableMem = guaranteedMem / Math.max(1, tierWorkflows);
        
        return estimated.cpuThreads <= availableCpu && 
               estimated.memoryMb <= availableMem;
    }
    
    private ResourceAllocation estimateResourceRequirements(WorkflowDefinition workflow) {
        int totalCpu = workflow.jobs().stream()
            .mapToInt(j -> j.taskType().defaultClassification().equals("compute") ? 2 : 1)
            .sum();
        
        int totalMem = workflow.jobs().size() * 512; // 512MB per job estimate
        long totalTime = workflow.jobs().stream()
            .mapToLong(JobDefinition::modeledCostMillis)
            .sum();
        
        return new ResourceAllocation(totalCpu, totalMem, totalTime);
    }
    
    private List<NodeProfile> filterNodesByTier(List<NodeProfile> allNodes, ServiceTier tier) {
        // Premium tiers get exclusive access to high-performance nodes
        // Basic tiers are restricted to lower-performance nodes
        return switch (tier) {
            case PREMIUM -> allNodes.stream()
                .filter(n -> n.cpuThreads() >= 4)
                .toList();
            case STANDARD -> allNodes;
            case BASIC, BEST_EFFORT -> allNodes.stream()
                .filter(n -> n.cpuThreads() <= 2)
                .toList();
        };
    }
    
    private List<JobDefinition> getTierPriorityOrderedJobs(WorkflowDefinition workflow,
                                                            TrainingBenchmarks benchmarks,
                                                            ServiceTier tier) {
        Map<String, Double> ranks = new HashMap<>();
        
        for (JobDefinition job : workflow.jobs()) {
            double own = benchmarks.hasMeasurements(job) ? 
                benchmarks.averageDuration(job) : job.modeledCostMillis();
            double maxChild = workflow.successors(job.id()).stream()
                .mapToDouble(succ -> succ.modeledCostMillis())
                .max()
                .orElse(0.0);
            
            // Apply tier priority multiplier
            double tierMultiplier = tier.allocationPriority();
            ranks.put(job.id(), (own + maxChild) / tierMultiplier);
        }
        
        return workflow.jobs().stream()
            .sorted(Comparator.<JobDefinition>comparingDouble(j -> ranks.get(j.id())).reversed())
            .toList();
    }
    
    private long calculateTierDeadline(WorkflowDefinition workflow, 
                                      TrainingBenchmarks benchmarks,
                                      ServiceTier tier) {
        long criticalPath = DurationModel.optimisticCriticalPath(workflow, 
            List.of(new ClusterProfile("default", List.of())));
        
        // Different tiers have different deadline expectations
        double multiplier = switch (tier) {
            case PREMIUM -> 1.2;  // 20% slack
            case STANDARD -> 1.5; // 50% slack
            case BASIC -> 2.0;    // 100% slack
            case BEST_EFFORT -> 5.0; // 400% slack (very relaxed)
        };
        
        return (long) (criticalPath * multiplier);
    }
    
    private double calculateQoSScore(long eft, boolean meetsDeadline, ServiceTier tier, NodeProfile node) {
        double timeScore = eft / 1000.0;
        double deadlinePenalty = meetsDeadline ? 0 : 1000; // Heavy penalty for missing deadline
        double tierBonus = tier.allocationPriority() * 100; // Prefer higher tiers
        
        return timeScore + deadlinePenalty - tierBonus;
    }
    
    private void updateQoSMetrics(String workflowId, List<PlanAssignment> plan, ServiceTier tier) {
        long makespan = plan.stream().mapToLong(PlanAssignment::predictedFinishMillis).max().orElse(0L);
        boolean deadlineMet = plan.stream().allMatch(p -> p.classification().contains("deadline-met"));
        
        workflowMetrics.put(workflowId, new QoSMetrics(
            tier, makespan, deadlineMet, System.currentTimeMillis()
        ));
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
    
    @Override
    public String name() {
        return baseScheduler.name() + "-QoS";
    }
    
    /**
     * Get QoS statistics.
     */
    public QoSStatistics getStatistics() {
        Map<ServiceTier, Long> tierCounts = new HashMap<>();
        Map<ServiceTier, Double> tierDeadlineMet = new HashMap<>();
        
        for (QoSMetrics metrics : workflowMetrics.values()) {
            tierCounts.merge(metrics.tier, 1L, Long::sum);
            if (metrics.deadlineMet) {
                tierDeadlineMet.merge(metrics.tier, 1.0, Double::sum);
            }
        }
        
        // Calculate deadline meet percentages
        for (ServiceTier tier : ServiceTier.values()) {
            long count = tierCounts.getOrDefault(tier, 0L);
            double met = tierDeadlineMet.getOrDefault(tier, 0.0);
            tierDeadlineMet.put(tier, count > 0 ? met / count : 0.0);
        }
        
        return new QoSStatistics(
            acceptedWorkflows.get(),
            rejectedWorkflows.get(),
            tierCounts,
            tierDeadlineMet,
            currentAllocations.size()
        );
    }
    
    // Records
    public record ResourceCapacity(int totalCpu, int totalMemory, int totalNodes) {}
    public record ResourceAllocation(double cpuThreads, int memoryMb, long estimatedTime) {
        public ResourceAllocation add(ResourceAllocation other) {
            return new ResourceAllocation(
                this.cpuThreads + other.cpuThreads,
                this.memoryMb + other.memoryMb,
                this.estimatedTime + other.estimatedTime
            );
        }
    }
    public record QoSMetrics(ServiceTier tier, long makespan, boolean deadlineMet, long timestamp) {}
    public record QoSStatistics(
        long acceptedWorkflows,
        long rejectedWorkflows,
        Map<ServiceTier, Long> workflowsPerTier,
        Map<ServiceTier, Double> deadlineMeetPercentagePerTier,
        int currentlyActiveWorkflows) {}
    
    // Exceptions
    public static class AdmissionRejectedException extends RuntimeException {
        public AdmissionRejectedException(String message) { super(message); }
    }
    public static class ResourceExhaustedException extends RuntimeException {
        public ResourceExhaustedException(String message) { super(message); }
    }
    
    private record Candidate(NodeProfile node, long est, long eft, long execTime, 
                            double score, boolean meetsDeadline) {}
}
