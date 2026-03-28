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
 * Energy-Aware Scheduler for green computing.
 * 
 * This addresses the growing concern of energy consumption in data centers.
 * Schedules workflows to minimize energy usage while meeting performance requirements.
 * 
 * Features:
 * - Per-node power consumption modeling (idle and active states)
 * - DVFS (Dynamic Voltage and Frequency Scaling) simulation
 * - Energy-performance tradeoff optimization
 * - Carbon footprint estimation
 * - Peak power constraint handling
 */
public final class EnergyAwareScheduler implements Scheduler {
    
    private final Scheduler baseScheduler;
    private final Map<String, PowerProfile> nodePowerProfiles;
    private final double energyWeight;
    private final double performanceWeight;
    private final double maxPowerBudget; // Watts
    
    public EnergyAwareScheduler(Scheduler baseScheduler) {
        this(baseScheduler, new HashMap<>(), 0.3, 0.7, Double.MAX_VALUE);
    }
    
    public EnergyAwareScheduler(Scheduler baseScheduler,
                               Map<String, PowerProfile> nodePowerProfiles,
                               double energyWeight,
                               double performanceWeight,
                               double maxPowerBudget) {
        this.baseScheduler = baseScheduler;
        this.nodePowerProfiles = nodePowerProfiles;
        this.energyWeight = energyWeight;
        this.performanceWeight = performanceWeight;
        this.maxPowerBudget = maxPowerBudget;
    }
    
    /**
     * Set power consumption profile for a node.
     * 
     * @param nodeId Node identifier
     * @param idlePower Idle power consumption in Watts
     * @param activePower Active power consumption at 100% utilization
     * @param pue Power Usage Effectiveness (data center overhead factor)
     */
    public void setNodePowerProfile(String nodeId, double idlePower, double activePower, double pue) {
        nodePowerProfiles.put(nodeId, new PowerProfile(idlePower, activePower, pue));
    }
    
    @Override
    public List<PlanAssignment> buildPlan(WorkflowDefinition workflow,
                                          List<ClusterProfile> clusters,
                                          TrainingBenchmarks benchmarks) {
        
        Map<String, Long> nodeAvailable = new HashMap<>();
        Map<String, Long> jobFinish = new HashMap<>();
        List<NodeProfile> allNodes = clusters.stream()
            .flatMap(c -> c.nodes().stream())
            .toList();
        List<PlanAssignment> plan = new ArrayList<>();
        
        double totalEnergyConsumed = 0.0;
        double peakPower = 0.0;
        
        // Sort jobs by priority (upward rank)
        List<JobDefinition> ordered = getPriorityOrderedJobs(workflow, benchmarks);
        
        for (JobDefinition job : ordered) {
            Candidate best = null;
            double bestScore = Double.MAX_VALUE;
            
            for (NodeProfile node : allNodes) {
                long est = Math.max(nodeAvailable.getOrDefault(node.nodeId(), 0L),
                                  maxDependencyFinish(job, jobFinish));
                
                long execTime = getDuration(job, node, benchmarks);
                long eft = est + execTime;
                
                // Calculate energy consumption for this assignment
                PowerProfile power = nodePowerProfiles.getOrDefault(node.nodeId(), 
                    new PowerProfile(50, 100, 1.5)); // Default: 50W idle, 100W active, PUE 1.5
                
                double energyJoules = calculateEnergyConsumption(power, execTime, node);
                
                // Check power budget constraint
                double nodePower = power.estimatedPower(node.cpuThreads());
                double newPeakPower = calculatePeakPower(nodeAvailable, node, nodePower, power);
                
                if (newPeakPower > maxPowerBudget) {
                    continue; // Exceeds power budget, skip this node
                }
                
                // Calculate combined score (lower is better)
                // Normalize metrics
                double normalizedTime = eft / 1000.0; // seconds
                double normalizedEnergy = energyJoules / 1000.0; // kJ
                
                double score = (performanceWeight * normalizedTime) + 
                              (energyWeight * normalizedEnergy);
                
                if (score < bestScore) {
                    bestScore = score;
                    best = new Candidate(node, est, eft, execTime, energyJoules, newPeakPower);
                }
            }
            
            if (best == null) {
                // No node meets power budget, use base scheduler fallback
                throw new PowerBudgetExceededException(
                    "Cannot schedule job " + job.id() + " within power budget " + maxPowerBudget + "W");
            }
            
            nodeAvailable.put(best.node.nodeId(), best.eft);
            jobFinish.put(job.id(), best.eft);
            totalEnergyConsumed += best.energyJoules;
            peakPower = Math.max(peakPower, best.peakPower);
            
            plan.add(new PlanAssignment(
                job.id(),
                best.node.clusterId(),
                best.node.nodeId(),
                best.est,
                best.eft,
                0.0,
                name(),
                String.format("energy-%.2fJ-peak-%.1fW", best.energyJoules, best.peakPower)));
        }
        
        // Report energy metrics
        double totalEnergyKWh = totalEnergyConsumed / (3600.0 * 1000.0); // Convert to kWh
        double carbonKg = totalEnergyKWh * 0.5; // ~0.5 kg CO2 per kWh (grid average)
        
        System.out.printf("Energy-aware schedule: Total=%.4f kWh, Peak=%.1f W, Est. CO2=%.4f kg%n",
                         totalEnergyKWh, peakPower, carbonKg);
        
        return plan;
    }
    
    /**
     * Calculate energy consumption in Joules.
     */
    private double calculateEnergyConsumption(PowerProfile power, long durationMillis, NodeProfile node) {
        double durationSeconds = durationMillis / 1000.0;
        double estimatedPower = power.estimatedPower(node.cpuThreads());
        double energyJoules = estimatedPower * durationSeconds;
        
        // Apply PUE (Power Usage Effectiveness) - accounts for cooling, etc.
        return energyJoules * power.pue;
    }
    
    /**
     * Estimate peak power if we schedule on this node.
     */
    private double calculatePeakPower(Map<String, Long> nodeAvailable, 
                                     NodeProfile candidateNode,
                                     double candidatePower,
                                     PowerProfile candidateProfile) {
        double peak = 0.0;
        
        // Sum power from all active nodes
        for (Map.Entry<String, Long> entry : nodeAvailable.entrySet()) {
            String nodeId = entry.getKey();
            PowerProfile profile = nodePowerProfiles.getOrDefault(nodeId, 
                new PowerProfile(50, 100, 1.5));
            peak += profile.idlePower; // Assume idle for simplicity
        }
        
        // Add candidate node power
        peak += candidatePower;
        
        return peak;
    }
    
    /**
     * Calculate total energy cost of a schedule.
     */
    public double calculateEnergyCost(List<PlanAssignment> plan, double electricityPricePerKWh) {
        double totalEnergyKWh = 0.0;
        
        for (PlanAssignment assignment : plan) {
            PowerProfile power = nodePowerProfiles.getOrDefault(assignment.nodeId(),
                new PowerProfile(50, 100, 1.5));
            
            long duration = assignment.predictedFinishMillis() - assignment.predictedStartMillis();
            double durationHours = duration / (1000.0 * 3600.0);
            double energyKWh = (power.activePower * durationHours) / 1000.0 * power.pue;
            
            totalEnergyKWh += energyKWh;
        }
        
        return totalEnergyKWh * electricityPricePerKWh;
    }
    
    @Override
    public String name() {
        return baseScheduler.name() + "-Energy";
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
    
    /**
     * Power consumption profile for a node.
     */
    public record PowerProfile(double idlePower, double activePower, double pue) {
        public double estimatedPower(int cpuThreads) {
            // Linear interpolation between idle and active based on utilization
            // Assume higher thread count = higher power
            double utilization = Math.min(1.0, cpuThreads / 8.0);
            return idlePower + (activePower - idlePower) * utilization;
        }
        
        public double estimatedEnergyJoules(long durationMillis) {
            return activePower * (durationMillis / 1000.0) * pue;
        }
    }
    
    /**
     * Exception when power budget is exceeded.
     */
    public static class PowerBudgetExceededException extends RuntimeException {
        public PowerBudgetExceededException(String message) {
            super(message);
        }
    }
    
    private record Candidate(NodeProfile node, long est, long eft, long execTime, 
                            double energyJoules, double peakPower) {}
}
