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
 * Security-Aware Scheduler for sensitive data processing.
 * 
 * This addresses the limitation of security-unaware scheduling by considering:
 * - Data sensitivity levels
 * - Node security clearance levels
 * - Data isolation requirements
 * - Compliance constraints (GDPR, HIPAA, etc.)
 * 
 * Features:
 * - Multi-level security (MLS) support
 * - Data sensitivity classification
 * - Secure node assignment
 * - Isolation enforcement
 * - Audit logging hooks
 */
public final class SecurityAwareScheduler implements Scheduler {
    
    public enum SensitivityLevel {
        PUBLIC(0),           // No restrictions
        INTERNAL(1),         // Internal use only
        CONFIDENTIAL(2),     // Restricted access
        RESTRICTED(3),      // Strictly controlled
        SECRET(4);           // Maximum security
        
        private final int level;
        
        SensitivityLevel(int level) { this.level = level; }
        public int level() { return level; }
        
        public boolean canProcessOn(SecurityClearance clearance) {
            return this.level <= clearance.level();
        }
    }
    
    public enum SecurityClearance {
        UNCLASSIFIED(0),
        RESTRICTED(1),
        CONFIDENTIAL(2),
        SECRET(3),
        TOP_SECRET(4);
        
        private final int level;
        
        SecurityClearance(int level) { this.level = level; }
        public int level() { return level; }
    }
    
    public enum ComplianceRequirement {
        GDPR,      // EU data protection
        HIPAA,     // US healthcare
        PCI_DSS,   // Payment card industry
        SOX,       // Sarbanes-Oxley
        FISMA,     // Federal IT security
        NONE
    }
    
    private final Scheduler baseScheduler;
    private final Map<String, DataClassification> dataClassifications;
    private final Map<String, NodeSecurityProfile> nodeSecurityProfiles;
    private final Map<String, WorkflowSecurityPolicy> workflowPolicies;
    private final boolean strictMode;
    
    public SecurityAwareScheduler(Scheduler baseScheduler) {
        this(baseScheduler, true);
    }
    
    public SecurityAwareScheduler(Scheduler baseScheduler, boolean strictMode) {
        this.baseScheduler = baseScheduler;
        this.strictMode = strictMode;
        this.dataClassifications = new HashMap<>();
        this.nodeSecurityProfiles = new HashMap<>();
        this.workflowPolicies = new HashMap<>();
    }
    
    /**
     * Classify data with sensitivity level.
     */
    public void classifyData(String dataId, SensitivityLevel level, 
                            Set<ComplianceRequirement> compliance) {
        dataClassifications.put(dataId, new DataClassification(level, compliance));
    }
    
    /**
     * Set security profile for a node.
     */
    public void setNodeSecurityProfile(String nodeId, SecurityClearance clearance,
                                      Set<String> trustedZones, boolean encrypted) {
        nodeSecurityProfiles.put(nodeId, 
            new NodeSecurityProfile(clearance, trustedZones, encrypted));
    }
    
    /**
     * Set security policy for a workflow.
     */
    public void setWorkflowSecurityPolicy(String workflowId, SensitivityLevel maxSensitivity,
                                         Set<ComplianceRequirement> requiredCompliance,
                                         boolean requiresIsolation) {
        workflowPolicies.put(workflowId, 
            new WorkflowSecurityPolicy(maxSensitivity, requiredCompliance, requiresIsolation));
    }
    
    @Override
    public List<PlanAssignment> buildPlan(WorkflowDefinition workflow,
                                          List<ClusterProfile> clusters,
                                          TrainingBenchmarks benchmarks) {
        
        WorkflowSecurityPolicy policy = workflowPolicies.getOrDefault(
            workflow.workflowId(), 
            new WorkflowSecurityPolicy(SensitivityLevel.PUBLIC, Set.of(), false));
        
        // Validate security feasibility
        if (!validateSecurityFeasibility(workflow, policy, clusters)) {
            throw new SecurityViolationException(
                "Workflow " + workflow.workflowId() + " cannot be scheduled securely");
        }
        
        // Get security-compliant nodes
        List<NodeProfile> compliantNodes = getSecurityCompliantNodes(clusters, policy);
        
        if (compliantNodes.isEmpty()) {
            throw new SecurityViolationException(
                "No nodes meet security requirements for workflow " + workflow.workflowId());
        }
        
        // Build plan using only compliant nodes
        Map<String, Long> nodeAvailable = new HashMap<>();
        Map<String, Long> jobFinish = new HashMap<>();
        Map<String, NodeProfile> jobAssignments = new HashMap<>();
        List<PlanAssignment> plan = new ArrayList<>();
        
        List<JobDefinition> ordered = getPriorityOrderedJobs(workflow, benchmarks);
        
        for (JobDefinition job : ordered) {
            // Determine required security for this job
            SensitivityLevel requiredLevel = getJobSensitivityLevel(job, policy);
            
            // Filter nodes that meet this job's security requirement
            List<NodeProfile> jobEligibleNodes = compliantNodes.stream()
                .filter(n -> canProcessSecurely(n, requiredLevel))
                .toList();
            
            if (jobEligibleNodes.isEmpty()) {
                if (strictMode) {
                    throw new SecurityViolationException(
                        "No secure node for job " + job.id() + " with sensitivity " + requiredLevel);
                }
                // Fall back to all compliant nodes
                jobEligibleNodes = compliantNodes;
            }
            
            Candidate best = null;
            
            for (NodeProfile node : jobEligibleNodes) {
                long est = Math.max(nodeAvailable.getOrDefault(node.nodeId(), 0L),
                                  maxDependencyFinish(job, jobFinish));
                
                long execTime = getDuration(job, node, benchmarks);
                long eft = est + execTime;
                
                // Calculate security score (prefer more secure nodes for sensitive jobs)
                double securityScore = calculateSecurityScore(node, requiredLevel);
                
                double combinedScore = eft - (securityScore * 100); // Reward security
                
                if (best == null || combinedScore < best.combinedScore) {
                    best = new Candidate(node, est, eft, execTime, securityScore, combinedScore);
                }
            }
            
            if (best == null) {
                throw new SecurityViolationException("No valid secure node for job " + job.id());
            }
            
            nodeAvailable.put(best.node.nodeId(), best.eft);
            jobFinish.put(job.id(), best.eft);
            jobAssignments.put(job.id(), best.node);
            
            // Log security decision for audit
            logSecurityDecision(workflow.workflowId(), job.id(), best.node.nodeId(), requiredLevel);
            
            plan.add(new PlanAssignment(
                job.id(),
                best.node.clusterId(),
                best.node.nodeId(),
                best.est,
                best.eft,
                best.securityScore,
                name(),
                String.format("%s-sec-%.2f", requiredLevel.name(), best.securityScore)));
        }
        
        // Validate isolation if required
        if (policy.requiresIsolation && !validateIsolation(plan, clusters)) {
            throw new SecurityViolationException("Isolation requirement cannot be satisfied");
        }
        
        return plan;
    }
    
    private boolean validateSecurityFeasibility(WorkflowDefinition workflow,
                                               WorkflowSecurityPolicy policy,
                                               List<ClusterProfile> clusters) {
        // Check if we have enough secure nodes
        long secureNodeCount = clusters.stream()
            .flatMap(c -> c.nodes().stream())
            .filter(n -> {
                NodeSecurityProfile profile = nodeSecurityProfiles.get(n.nodeId());
                return profile != null && profile.clearance.level() >= policy.maxSensitivity.level();
            })
            .count();
        
        return secureNodeCount > 0;
    }
    
    private List<NodeProfile> getSecurityCompliantNodes(List<ClusterProfile> clusters,
                                                         WorkflowSecurityPolicy policy) {
        List<NodeProfile> compliant = new ArrayList<>();
        
        for (ClusterProfile cluster : clusters) {
            for (NodeProfile node : cluster.nodes()) {
                NodeSecurityProfile profile = nodeSecurityProfiles.get(node.nodeId());
                
                // Node must have sufficient clearance
                if (profile == null) {
                    if (policy.maxSensitivity == SensitivityLevel.PUBLIC) {
                        compliant.add(node); // Public data can go anywhere
                    }
                    continue;
                }
                
                if (profile.clearance.level() >= policy.maxSensitivity.level()) {
                    // Check compliance requirements
                    boolean meetsCompliance = policy.requiredCompliance.stream()
                        .allMatch(req -> profile.complianceCertifications.contains(req));
                    
                    if (meetsCompliance || policy.requiredCompliance.isEmpty()) {
                        compliant.add(node);
                    }
                }
            }
        }
        
        return compliant;
    }
    
    private boolean canProcessSecurely(NodeProfile node, SensitivityLevel level) {
        NodeSecurityProfile profile = nodeSecurityProfiles.get(node.nodeId());
        if (profile == null) {
            return level == SensitivityLevel.PUBLIC;
        }
        return level.canProcessOn(profile.clearance);
    }
    
    private SensitivityLevel getJobSensitivityLevel(JobDefinition job, WorkflowSecurityPolicy policy) {
        // Determine based on task type and inputs
        SensitivityLevel baseLevel = switch (job.taskType()) {
            case BLAST, CLUSTAL -> SensitivityLevel.INTERNAL; // Genomic data
            case DNAPARS, PROTPARS -> SensitivityLevel.INTERNAL;
            case PREPARE_RECEPTOR, AUTODOCK -> SensitivityLevel.CONFIDENTIAL; // Pharma
            case MAP, PILEUP -> SensitivityLevel.INTERNAL;
            default -> SensitivityLevel.PUBLIC;
        };
        
        // Cap at workflow max
        return baseLevel.level() > policy.maxSensitivity.level() ? 
            policy.maxSensitivity : baseLevel;
    }
    
    private double calculateSecurityScore(NodeProfile node, SensitivityLevel requiredLevel) {
        NodeSecurityProfile profile = nodeSecurityProfiles.get(node.nodeId());
        if (profile == null) {
            return requiredLevel == SensitivityLevel.PUBLIC ? 1.0 : 0.0;
        }
        
        double score = 0.0;
        
        // Clearance match
        if (profile.clearance.level() >= requiredLevel.level()) {
            score += 0.5;
        }
        
        // Encryption support
        if (profile.supportsEncryption) {
            score += 0.3;
        }
        
        // Trusted zone
        if (!profile.trustedZones.isEmpty()) {
            score += 0.2;
        }
        
        return score;
    }
    
    private boolean validateIsolation(List<PlanAssignment> plan, List<ClusterProfile> clusters) {
        // Check that workflow runs on dedicated nodes not shared with other sensitive workflows
        // Simplified: ensure all jobs run in same security zone
        Set<String> usedZones = new HashSet<>();
        
        for (PlanAssignment assignment : plan) {
            NodeSecurityProfile profile = nodeSecurityProfiles.get(assignment.nodeId());
            if (profile != null) {
                usedZones.addAll(profile.trustedZones);
            }
        }
        
        // In strict isolation, workflow should use single zone
        return usedZones.size() <= 1;
    }
    
    private void logSecurityDecision(String workflowId, String jobId, String nodeId, 
                                     SensitivityLevel level) {
        // Hook for audit logging
        System.out.printf("[SECURITY] Workflow=%s Job=%s assigned to Node=%s Sensitivity=%s%n",
                         workflowId, jobId, nodeId, level);
    }
    
    private List<JobDefinition> getPriorityOrderedJobs(WorkflowDefinition workflow, 
                                                         TrainingBenchmarks benchmarks) {
        Map<String, Double> ranks = new HashMap<>();
        
        for (JobDefinition job : workflow.jobs()) {
            double own = benchmarks.hasMeasurements(job) ? 
                benchmarks.averageDuration(job) : job.modeledCostMillis();
            double maxChild = workflow.successors(job.id()).stream()
                .mapToDouble(succ -> succ.modeledCostMillis())
                .max()
                .orElse(0.0);
            ranks.put(job.id(), own + maxChild);
        }
        
        return workflow.jobs().stream()
            .sorted((a, b) -> {
                // Prioritize sensitive jobs first
                int sensitivityDiff = getSensitivityPriority(b) - getSensitivityPriority(a);
                if (sensitivityDiff != 0) return sensitivityDiff;
                return Double.compare(ranks.get(b.id()), ranks.get(a.id()));
            })
            .toList();
    }
    
    private int getSensitivityPriority(JobDefinition job) {
        return switch (job.taskType()) {
            case PREPARE_RECEPTOR, AUTODOCK -> 4; // Highest - pharma data
            case BLAST, CLUSTAL, DNAPARS, PROTPARS -> 3; // Genomic
            case MAP, PILEUP, FILTER_CONTAMS -> 2; // Health data
            default -> 1; // Public
        };
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
        return baseScheduler.name() + "-Secure";
    }
    
    /**
     * Get security audit report.
     */
    public SecurityAuditReport getSecurityAuditReport() {
        return new SecurityAuditReport(
            dataClassifications.size(),
            nodeSecurityProfiles.size(),
            workflowPolicies.size(),
            strictMode
        );
    }
    
    // Records
    public record DataClassification(SensitivityLevel level, Set<ComplianceRequirement> compliance) {}
    public record NodeSecurityProfile(SecurityClearance clearance, Set<String> trustedZones, 
                                     boolean supportsEncryption,
                                     Set<ComplianceRequirement> complianceCertifications) {
        public NodeSecurityProfile(SecurityClearance clearance, Set<String> trustedZones, 
                                  boolean supportsEncryption) {
            this(clearance, trustedZones, supportsEncryption, Set.of());
        }
    }
    public record WorkflowSecurityPolicy(SensitivityLevel maxSensitivity,
                                        Set<ComplianceRequirement> requiredCompliance,
                                        boolean requiresIsolation) {}
    public record SecurityAuditReport(int classifiedDataItems,
                                     int securedNodes,
                                     int securedWorkflows,
                                     boolean strictModeEnforced) {}
    
    // Exception
    public static class SecurityViolationException extends RuntimeException {
        public SecurityViolationException(String message) { super(message); }
    }
    
    private record Candidate(NodeProfile node, long est, long eft, long execTime,
                            double securityScore, double combinedScore) {}
}
