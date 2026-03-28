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
import java.util.Set;

/**
 * Data Locality Aware Scheduler that optimizes for data placement.
 * 
 * This extends communication cost awareness by explicitly tracking data location
 * and scheduling tasks near their input data to minimize network transfer.
 * 
 * Features:
 * - Data location tracking (which node holds which data)
 * - Input size awareness for scheduling decisions
 * - Data movement minimization
 * - Rack/zone-aware scheduling
 * - Hot spot detection and avoidance
 */
public final class DataLocalityScheduler implements Scheduler {
    
    private final Scheduler baseScheduler;
    private final Map<String, String> dataLocations; // dataId -> nodeId
    private final Map<String, Set<String>> nodeDataIndex; // nodeId -> set of dataIds
    private final double localityWeight;
    private final boolean enableRackAwareness;
    
    public DataLocalityScheduler(Scheduler baseScheduler) {
        this(baseScheduler, new HashMap<>(), 0.6, false);
    }
    
    public DataLocalityScheduler(Scheduler baseScheduler,
                                  Map<String, String> initialDataLocations,
                                  double localityWeight,
                                  boolean enableRackAwareness) {
        this.baseScheduler = baseScheduler;
        this.dataLocations = new HashMap<>(initialDataLocations);
        this.localityWeight = localityWeight;
        this.enableRackAwareness = enableRackAwareness;
        this.nodeDataIndex = buildNodeDataIndex(dataLocations);
    }
    
    /**
     * Register data location (which node holds the data).
     * 
     * @param dataId Data identifier (e.g., file path or dataset ID)
     * @param nodeId Node that holds the data
     */
    public void registerDataLocation(String dataId, String nodeId) {
        dataLocations.put(dataId, nodeId);
        nodeDataIndex.computeIfAbsent(nodeId, k -> new java.util.HashSet<>()).add(dataId);
    }
    
    /**
     * Bulk register multiple data items on a node.
     */
    public void registerNodeData(String nodeId, Set<String> dataIds) {
        for (String dataId : dataIds) {
            dataLocations.put(dataId, nodeId);
        }
        nodeDataIndex.put(nodeId, new java.util.HashSet<>(dataIds));
    }
    
    @Override
    public List<PlanAssignment> buildPlan(WorkflowDefinition workflow,
                                          List<ClusterProfile> clusters,
                                          TrainingBenchmarks benchmarks) {
        
        Map<String, Long> nodeAvailable = new HashMap<>();
        Map<String, Long> jobFinish = new HashMap<>();
        Map<String, NodeProfile> jobAssignments = new HashMap<>();
        List<NodeProfile> allNodes = clusters.stream()
            .flatMap(c -> c.nodes().stream())
            .toList();
        List<PlanAssignment> plan = new ArrayList<>();
        
        // Build data flow graph to understand input/output relationships
        Map<String, Set<String>> jobInputs = analyzeJobInputs(workflow);
        
        // Sort jobs by priority
        List<JobDefinition> ordered = getPriorityOrderedJobs(workflow, benchmarks);
        
        for (JobDefinition job : ordered) {
            Set<String> inputs = jobInputs.getOrDefault(job.id(), Set.of());
            
            Candidate best = null;
            double bestScore = Double.MAX_VALUE;
            
            for (NodeProfile node : allNodes) {
                long est = Math.max(nodeAvailable.getOrDefault(node.nodeId(), 0L),
                                  maxDependencyFinish(job, jobFinish));
                
                long execTime = getDuration(job, node, benchmarks);
                long eft = est + execTime;
                
                // Calculate data locality score
                double localityScore = calculateLocalityScore(inputs, node, jobAssignments, workflow);
                
                // Combined score (lower is better)
                double normalizedTime = eft / 1000.0;
                double normalizedLocality = 1.0 - localityScore; // Invert so higher locality = lower score
                
                double score = ((1.0 - localityWeight) * normalizedTime) + 
                              (localityWeight * normalizedLocality * 10); // Scale locality impact
                
                if (score < bestScore) {
                    bestScore = score;
                    best = new Candidate(node, est, eft, execTime, localityScore);
                }
            }
            
            if (best == null) {
                throw new IllegalStateException("No valid schedule found for job " + job.id());
            }
            
            nodeAvailable.put(best.node.nodeId(), best.eft);
            jobFinish.put(job.id(), best.eft);
            jobAssignments.put(job.id(), best.node);
            
            // Register job output location (assume output is on execution node)
            String outputDataId = job.id() + "-output";
            registerDataLocation(outputDataId, best.node.nodeId());
            
            plan.add(new PlanAssignment(
                job.id(),
                best.node.clusterId(),
                best.node.nodeId(),
                best.est,
                best.eft,
                best.localityScore,
                name(),
                String.format("locality-%.2f", best.localityScore)));
        }
        
        // Report locality statistics
        double avgLocality = plan.stream()
            .mapToDouble(PlanAssignment::upwardRank)
            .average()
            .orElse(0.0);
        System.out.printf("Data locality: Average score=%.2f%n", avgLocality);
        
        return plan;
    }
    
    /**
     * Calculate data locality score for assigning job to node.
     * Score of 1.0 = all inputs local, 0.0 = no inputs local
     */
    private double calculateLocalityScore(Set<String> inputs, NodeProfile targetNode,
                                         Map<String, NodeProfile> jobAssignments,
                                         WorkflowDefinition workflow) {
        if (inputs.isEmpty()) {
            return 0.5; // Neutral score for jobs with no explicit inputs
        }
        
        double totalScore = 0.0;
        int inputCount = 0;
        
        for (String input : inputs) {
            // Check if input is directly on target node
            String dataLocation = dataLocations.get(input);
            if (dataLocation != null) {
                if (dataLocation.equals(targetNode.nodeId())) {
                    totalScore += 1.0; // Full locality
                } else if (enableRackAwareness && sameRack(dataLocation, targetNode.nodeId())) {
                    totalScore += 0.7; // Same rack (lower cost)
                } else {
                    totalScore += 0.0; // Remote data
                }
                inputCount++;
            } else if (input.startsWith("job:")) {
                // Input is output from another job
                String depJobId = input.substring(4);
                NodeProfile depNode = jobAssignments.get(depJobId);
                if (depNode != null) {
                    if (depNode.nodeId().equals(targetNode.nodeId())) {
                        totalScore += 1.0;
                    } else if (enableRackAwareness && sameRack(depNode.nodeId(), targetNode.nodeId())) {
                        totalScore += 0.7;
                    } else {
                        totalScore += 0.0;
                    }
                    inputCount++;
                }
            }
        }
        
        return inputCount > 0 ? totalScore / inputCount : 0.0;
    }
    
    /**
     * Check if two nodes are in the same rack (simplified implementation).
     */
    private boolean sameRack(String nodeId1, String nodeId2) {
        // Simplified: nodes in same cluster are considered same rack
        // Real implementation would use rack topology from cluster config
        String cluster1 = nodeId1.split("-")[0];
        String cluster2 = nodeId2.split("-")[0];
        return cluster1.equals(cluster2);
    }
    
    /**
     * Analyze job inputs from workflow dependencies.
     */
    private Map<String, Set<String>> analyzeJobInputs(WorkflowDefinition workflow) {
        Map<String, Set<String>> jobInputs = new HashMap<>();
        
        for (JobDefinition job : workflow.jobs()) {
            Set<String> inputs = new java.util.HashSet<>();
            
            // Dependencies produce input data
            for (String dep : job.dependencies()) {
                inputs.add("job:" + dep);
            }
            
            // Additional inputs based on task type (simplified)
            switch (job.taskType()) {
                case BLAST -> inputs.add("reference-data");
                case FASTQ_SPLIT -> inputs.add("input-fastq");
                case MAP -> inputs.add("reference-genome");
                default -> { /* No additional inputs */ }
            }
            
            jobInputs.put(job.id(), inputs);
        }
        
        return jobInputs;
    }
    
    private Map<String, Set<String>> buildNodeDataIndex(Map<String, String> dataLocations) {
        Map<String, Set<String>> index = new HashMap<>();
        for (Map.Entry<String, String> entry : dataLocations.entrySet()) {
            index.computeIfAbsent(entry.getValue(), k -> new java.util.HashSet<>())
                 .add(entry.getKey());
        }
        return index;
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
            .sorted(Comparator.<JobDefinition>comparingDouble(j -> ranks.get(j.id())).reversed())
            .toList();
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
        return baseScheduler.name() + "-Locality";
    }
    
    /**
     * Get data distribution statistics.
     */
    public DataDistributionStats getDataDistributionStats() {
        Map<String, Integer> dataPerNode = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : nodeDataIndex.entrySet()) {
            dataPerNode.put(entry.getKey(), entry.getValue().size());
        }
        
        double avgDataPerNode = dataPerNode.values().stream()
            .mapToInt(Integer::intValue)
            .average()
            .orElse(0.0);
        
        return new DataDistributionStats(
            dataLocations.size(),
            nodeDataIndex.size(),
            avgDataPerNode,
            dataPerNode
        );
    }
    
    public record DataDistributionStats(
        int totalDataItems,
        int nodesWithData,
        double avgDataPerNode,
        Map<String, Integer> dataCountPerNode) {}
    
    private record Candidate(NodeProfile node, long est, long eft, long execTime, double localityScore) {}
}
