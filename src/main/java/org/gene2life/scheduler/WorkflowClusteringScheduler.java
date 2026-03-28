package org.gene2life.scheduler;

import org.gene2life.model.ClusterProfile;
import org.gene2life.model.JobDefinition;
import org.gene2life.model.NodeProfile;
import org.gene2life.model.PlanAssignment;
import org.gene2life.model.TaskType;
import org.gene2life.model.WorkflowDefinition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Workflow Clustering Scheduler that groups small tasks for efficiency.
 * 
 * This addresses the limitation of scheduling overhead for fine-grained workflows.
 * Small tasks are batched together to reduce scheduling and execution overhead.
 * 
 * Features:
 * - Task clustering by similarity (task type, dependencies)
 * - Batch execution of clustered tasks
 * - Configurable cluster size limits
 * - Dependency-aware clustering
 * - Reduced scheduling overhead
 */
public final class WorkflowClusteringScheduler implements Scheduler {
    
    private final Scheduler baseScheduler;
    private final int minClusterSize;
    private final int maxClusterSize;
    private final long minTaskDurationMs;
    private final boolean preserveDependencies;
    
    public WorkflowClusteringScheduler(Scheduler baseScheduler) {
        this(baseScheduler, 3, 10, 1000L, true);
    }
    
    public WorkflowClusteringScheduler(Scheduler baseScheduler,
                                        int minClusterSize,
                                        int maxClusterSize,
                                        long minTaskDurationMs,
                                        boolean preserveDependencies) {
        this.baseScheduler = baseScheduler;
        this.minClusterSize = minClusterSize;
        this.maxClusterSize = maxClusterSize;
        this.minTaskDurationMs = minTaskDurationMs;
        this.preserveDependencies = preserveDependencies;
    }
    
    @Override
    public List<PlanAssignment> buildPlan(WorkflowDefinition workflow,
                                          List<ClusterProfile> clusters,
                                          TrainingBenchmarks benchmarks) {
        
        // Identify clusterable tasks
        List<JobDefinition> clusterableJobs = identifyClusterableJobs(workflow, benchmarks);
        
        if (clusterableJobs.size() < minClusterSize) {
            // Not enough jobs to cluster, use base scheduler
            return baseScheduler.buildPlan(workflow, clusters, benchmarks);
        }
        
        // Create clusters
        List<JobCluster> clusters_jobs = createClusters(clusterableJobs, workflow);
        
        // Create virtual workflow with clusters as single jobs
        WorkflowDefinition clusteredWorkflow = createClusteredWorkflow(workflow, clusters_jobs);
        
        // Schedule the clustered workflow
        List<PlanAssignment> clusteredPlan = baseScheduler.buildPlan(
            clusteredWorkflow, clusters, benchmarks);
        
        // Expand clustered assignments back to individual job assignments
        List<PlanAssignment> expandedPlan = expandClusterAssignments(
            clusteredPlan, clusters_jobs, workflow);
        
        // Report clustering statistics
        reportClusteringStats(clusters_jobs, workflow.jobs().size());
        
        return expandedPlan;
    }
    
    /**
     * Identify jobs that are candidates for clustering.
     */
    private List<JobDefinition> identifyClusterableJobs(WorkflowDefinition workflow,
                                                         TrainingBenchmarks benchmarks) {
        return workflow.jobs().stream()
            .filter(job -> isClusterable(job, benchmarks))
            .toList();
    }
    
    /**
     * Check if a job is suitable for clustering.
     */
    private boolean isClusterable(JobDefinition job, TrainingBenchmarks benchmarks) {
        // Short duration jobs are good candidates
        long duration = benchmarks.hasMeasurements(job) ? 
            (long) benchmarks.averageDuration(job) : job.modeledCostMillis();
        
        if (duration > minTaskDurationMs) {
            return false; // Long jobs shouldn't be clustered
        }
        
        // Certain task types cluster well
        return switch (job.taskType()) {
            case BLAST, CLUSTAL, MAP, AUTODOCK -> true; // Embarrassingly parallel
            case FILTER_CONTAMS, SOL2SANGER, FASTQ_TO_BFQ -> true; // Data parallel
            default -> false; // Sequential/dependent tasks don't cluster well
        };
    }
    
    /**
     * Create job clusters based on similarity.
     */
    private List<JobCluster> createClusters(List<JobDefinition> jobs, WorkflowDefinition workflow) {
        List<JobCluster> clusters = new ArrayList<>();
        
        // Group by task type first
        Map<TaskType, List<JobDefinition>> byType = new HashMap<>();
        for (JobDefinition job : jobs) {
            byType.computeIfAbsent(job.taskType(), k -> new ArrayList<>()).add(job);
        }
        
        // Create clusters for each type
        for (Map.Entry<TaskType, List<JobDefinition>> entry : byType.entrySet()) {
            List<JobDefinition> typeJobs = entry.getValue();
            
            // Further group by common dependencies (for dependency-aware clustering)
            Map<Set<String>, List<JobDefinition>> byDeps = new HashMap<>();
            for (JobDefinition job : typeJobs) {
                byDeps.computeIfAbsent(job.dependencies(), k -> new ArrayList<>()).add(job);
            }
            
            // Create clusters respecting size limits
            for (List<JobDefinition> depGroup : byDeps.values()) {
                for (int i = 0; i < depGroup.size(); i += maxClusterSize) {
                    int end = Math.min(i + maxClusterSize, depGroup.size());
                    List<JobDefinition> clusterJobs = depGroup.subList(i, end);
                    
                    if (clusterJobs.size() >= minClusterSize) {
                        clusters.add(new JobCluster(
                            "cluster-" + entry.getKey().name() + "-" + clusters.size(),
                            entry.getKey(),
                            new ArrayList<>(clusterJobs)
                        ));
                    }
                }
            }
        }
        
        return clusters;
    }
    
    /**
     * Create a virtual workflow where clusters appear as single jobs.
     */
    private WorkflowDefinition createClusteredWorkflow(WorkflowDefinition original,
                                                         List<JobCluster> clusters) {
        List<JobDefinition> virtualJobs = new ArrayList<>();
        Map<String, String> jobToCluster = new HashMap<>();
        
        // Build job to cluster mapping
        for (JobCluster cluster : clusters) {
            for (JobDefinition job : cluster.jobs) {
                jobToCluster.put(job.id(), cluster.id);
            }
        }
        
        // Add non-clustered jobs as-is
        for (JobDefinition job : original.jobs()) {
            if (!jobToCluster.containsKey(job.id())) {
                virtualJobs.add(job);
            }
        }
        
        // Add cluster jobs
        for (JobCluster cluster : clusters) {
            // Calculate aggregated properties
            long totalCost = cluster.jobs.stream()
                .mapToLong(JobDefinition::modeledCostMillis)
                .sum();
            
            // Dependencies are union of all jobs' external dependencies
            Set<String> externalDeps = new java.util.HashSet<>();
            for (JobDefinition job : cluster.jobs) {
                for (String dep : job.dependencies()) {
                    if (!jobToCluster.getOrDefault(dep, "").equals(cluster.id)) {
                        externalDeps.add(jobToCluster.getOrDefault(dep, dep));
                    }
                }
            }
            
            JobDefinition clusterJob = new JobDefinition(
                cluster.id,
                "Cluster " + cluster.type.name(),
                List.copyOf(externalDeps),
                cluster.type,
                totalCost,
                cluster.jobs.get(0).trainingProfileKey(),
                "cluster-input",
                "cluster-output",
                Map.of("cluster_size", Integer.toString(cluster.jobs.size()))
            );
            
            virtualJobs.add(clusterJob);
        }
        
        return new WorkflowDefinition(
            original.workflowId() + "-clustered",
            original.displayName() + " (Clustered)",
            virtualJobs
        );
    }
    
    /**
     * Expand clustered assignments back to individual jobs.
     */
    private List<PlanAssignment> expandClusterAssignments(List<PlanAssignment> clusteredPlan,
                                                           List<JobCluster> clusters,
                                                           WorkflowDefinition original) {
        List<PlanAssignment> expanded = new ArrayList<>();
        Map<String, JobCluster> clusterMap = new HashMap<>();
        for (JobCluster c : clusters) {
            clusterMap.put(c.id, c);
        }
        
        for (PlanAssignment assignment : clusteredPlan) {
            JobCluster cluster = clusterMap.get(assignment.jobId());
            
            if (cluster == null) {
                // Non-clustered job
                expanded.add(assignment);
            } else {
                // Expand cluster
                long baseTime = assignment.predictedStartMillis();
                long clusterDuration = assignment.predictedFinishMillis() - baseTime;
                long timePerJob = clusterDuration / cluster.jobs.size();
                
                for (int i = 0; i < cluster.jobs.size(); i++) {
                    JobDefinition job = cluster.jobs.get(i);
                    long jobStart = baseTime + (i * timePerJob);
                    long jobEnd = jobStart + timePerJob;
                    
                    expanded.add(new PlanAssignment(
                        job.id(),
                        assignment.clusterId(),
                        assignment.nodeId(),
                        jobStart,
                        jobEnd,
                        assignment.upwardRank(),
                        name(),
                        "clustered-" + assignment.classification()
                    ));
                }
            }
        }
        
        return expanded;
    }
    
    private void reportClusteringStats(List<JobCluster> clusters, int totalJobs) {
        int clusteredJobs = clusters.stream().mapToInt(c -> c.jobs.size()).sum();
        System.out.printf("Workflow clustering: %d jobs in %d clusters, %d jobs unclustered%n",
            clusteredJobs, clusters.size(), totalJobs - clusteredJobs);
    }
    
    @Override
    public String name() {
        return baseScheduler.name() + "-Clustered";
    }
    
    /**
     * Get clustering statistics.
     */
    public ClusteringStats getClusteringStats(WorkflowDefinition workflow,
                                               TrainingBenchmarks benchmarks) {
        List<JobDefinition> clusterable = identifyClusterableJobs(workflow, benchmarks);
        List<JobCluster> clusters = createClusters(clusterable, workflow);
        
        return new ClusteringStats(
            workflow.jobs().size(),
            clusterable.size(),
            clusters.size(),
            clusters.stream().mapToInt(c -> c.jobs.size()).average().orElse(0.0)
        );
    }
    
    // Records
    public record JobCluster(String id, TaskType type, List<JobDefinition> jobs) {}
    public record ClusteringStats(
        int totalJobs,
        int clusterableJobs,
        int clustersCreated,
        double avgJobsPerCluster) {}
}
