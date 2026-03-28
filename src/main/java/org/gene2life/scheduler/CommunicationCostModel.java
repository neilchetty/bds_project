package org.gene2life.scheduler;

import org.gene2life.model.ClusterProfile;
import org.gene2life.model.JobDefinition;
import org.gene2life.model.NodeProfile;
import org.gene2life.model.WorkflowDefinition;

import java.util.HashMap;
import java.util.Map;

/**
 * Models communication costs (data transfer time) between dependent tasks.
 * This addresses the paper limitation: "communication cost is ignored".
 */
public final class CommunicationCostModel {
    
    // Bandwidth between clusters in MB/s (configurable)
    private final Map<String, Map<String, Double>> interClusterBandwidth;
    // Intra-cluster bandwidth (high speed, e.g., shared memory or fast network)
    private final double intraClusterBandwidth;
    // Default bandwidth if not specified
    private final double defaultBandwidth;
    
    public CommunicationCostModel() {
        this.interClusterBandwidth = new HashMap<>();
        this.intraClusterBandwidth = 1000.0; // 1 GB/s intra-cluster
        this.defaultBandwidth = 100.0; // 100 MB/s inter-cluster default
    }
    
    /**
     * Configure bandwidth between two clusters (symmetric).
     */
    public void setInterClusterBandwidth(String clusterId1, String clusterId2, double bandwidthMBps) {
        interClusterBandwidth
            .computeIfAbsent(clusterId1, k -> new HashMap<>())
            .put(clusterId2, bandwidthMBps);
        interClusterBandwidth
            .computeIfAbsent(clusterId2, k -> new HashMap<>())
            .put(clusterId1, bandwidthMBps);
    }
    
    /**
     * Estimate data transfer time between two nodes for a given data size.
     * 
     * @param fromNode Source node
     * @param toNode Destination node
     * @param dataSizeBytes Size of data to transfer
     * @return Estimated transfer time in milliseconds
     */
    public long estimateTransferTime(NodeProfile fromNode, NodeProfile toNode, long dataSizeBytes) {
        if (fromNode.nodeId().equals(toNode.nodeId())) {
            return 0L; // Same node, no transfer cost
        }
        
        double bandwidth;
        if (fromNode.clusterId().equals(toNode.clusterId())) {
            bandwidth = intraClusterBandwidth;
        } else {
            bandwidth = interClusterBandwidth
                .getOrDefault(fromNode.clusterId(), new HashMap<>())
                .getOrDefault(toNode.clusterId(), defaultBandwidth);
        }
        
        // Convert bytes to MB and calculate time
        double dataSizeMB = dataSizeBytes / (1024.0 * 1024.0);
        double timeSeconds = dataSizeMB / bandwidth;
        return Math.round(timeSeconds * 1000.0);
    }
    
    /**
     * Estimate output data size produced by a job.
     * This is a simplified model - in production, this would be learned from historical data.
     */
    public long estimateOutputSize(JobDefinition job) {
        return switch (job.taskType()) {
            case BLAST -> 100_000L; // ~100KB hits file
            case CLUSTAL -> 50_000L; // ~50KB alignment
            case DNAPARS, PROTPARS -> 4_000L; // ~4KB tree
            case DRAWGRAM -> 35_000L; // ~35KB output
            case PREPARE_RECEPTOR -> 20_000L;
            case PREPARE_GPF -> 10_000L;
            case PREPARE_DPF -> 5_000L;
            case AUTOGRID -> 50_000L;
            case AUTODOCK -> 5_000L;
            case FASTQ_SPLIT -> 1_000L; // manifest is small
            case FILTER_CONTAMS -> 500_000L; // FASTQ data
            case SOL2SANGER -> 500_000L;
            case FASTQ_TO_BFQ -> 400_000L;
            case MAP -> 200_000L;
            case MAP_MERGE -> 2_000_000L; // merged alignments
            case MAQ_INDEX -> 100_000L;
            case PILEUP -> 50_000L;
        };
    }
    
    /**
     * Calculate communication cost for a job when scheduled on a specific node.
     * This considers all dependencies and where they were executed.
     * 
     * @param job Job to schedule
     * @param targetNode Node where job will execute
     * @param workflow Workflow definition
     * @param jobAssignments Map of jobId -> assigned NodeProfile for completed jobs
     * @return Total communication cost in milliseconds
     */
    public long calculateCommunicationCost(
            JobDefinition job,
            NodeProfile targetNode,
            WorkflowDefinition workflow,
            Map<String, NodeProfile> jobAssignments) {
        
        long totalCost = 0L;
        
        for (String depId : job.dependencies()) {
            NodeProfile sourceNode = jobAssignments.get(depId);
            if (sourceNode == null) {
                continue; // Dependency not yet scheduled
            }
            
            JobDefinition dependency = workflow.job(depId);
            long dataSize = estimateOutputSize(dependency);
            long transferTime = estimateTransferTime(sourceNode, targetNode, dataSize);
            totalCost += transferTime;
        }
        
        return totalCost;
    }
    
    /**
     * Communication-aware earliest finish time calculation.
     */
    public long calculateAftWithCommunication(
            JobDefinition job,
            NodeProfile targetNode,
            long est, // earliest start time based on availability
            WorkflowDefinition workflow,
            Map<String, NodeProfile> jobAssignments,
            Map<String, Long> jobFinishTimes) {
        
        long maxDependencyFinishWithComm = 0L;
        
        for (String depId : job.dependencies()) {
            Long depFinish = jobFinishTimes.get(depId);
            NodeProfile depNode = jobAssignments.get(depId);
            
            if (depFinish == null || depNode == null) {
                continue;
            }
            
            JobDefinition dependency = workflow.job(depId);
            long dataSize = estimateOutputSize(dependency);
            long transferTime = estimateTransferTime(depNode, targetNode, dataSize);
            
            // Dependency finishes, then data transfers, then this job can start
            long availableTime = depFinish + transferTime;
            maxDependencyFinishWithComm = Math.max(maxDependencyFinishWithComm, availableTime);
        }
        
        // Job can start when node is available AND all dependencies finish with data transfer
        long actualStart = Math.max(est, maxDependencyFinishWithComm);
        return actualStart;
    }
}
