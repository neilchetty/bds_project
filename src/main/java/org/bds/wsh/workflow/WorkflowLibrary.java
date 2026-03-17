package org.bds.wsh.workflow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.bds.wsh.model.Task;
import org.bds.wsh.model.Workflow;

/**
 * Built-in workflow definitions with big-data-scale edge data.
 * Edge data sizes represent production bioinformatics and scientific workloads
 * where significant data is transferred between pipeline stages.
 *
 * <p><b>Where is the big data?</b><br>
 * Every workflow here transfers data between tasks at a scale typical of
 * real scientific pipelines:
 * <ul>
 *   <li>{@link #gene2life()} — genomics pipeline: ~150 MB total edge data</li>
 *   <li>{@link #avianfluSmall()} — molecular docking: ~20 GB total edge data</li>
 *   <li>{@link #epigenomics()} — epigenomic analysis: multi-GB total edge data</li>
 *   <li>{@link #cyberShake()} — seismic hazard analysis: ~1 TB total edge data
 *       (1 000 tasks, representative of production CyberShake runs on national
 *       HPC facilities)</li>
 * </ul>
 * Use the {@code workflow-info} CLI command to print a full data-volume summary
 * for every workflow.
 */
public final class WorkflowLibrary {
    private WorkflowLibrary() {
    }

    public static Workflow gene2life() {
        List<Task> tasks = List.of(
                task("Blast1", 62.0, 1.1 * MB, List.of(), Map.of()),
                task("Blast2", 62.0, 1.1 * MB, List.of(), Map.of()),
                task("Clustalw1", 90.0, 104_857.6 + 4 * KB, List.of("Blast1"),
                        Map.of("Blast1", 50 * MB)),
                task("Clustalw2", 90.0, 104_857.6 + 4 * KB, List.of("Blast2"),
                        Map.of("Blast2", 50 * MB)),
                task("Dnapars", 19.0, 4 * KB, List.of("Clustalw1"),
                        Map.of("Clustalw1", 20 * MB)),
                task("Protpars", 16.0, 4 * KB, List.of("Clustalw2"),
                        Map.of("Clustalw2", 20 * MB)),
                task("Drawgram1", 18.0, 35 * KB, List.of("Dnapars"),
                        Map.of("Dnapars", 5 * MB)),
                task("Drawgram2", 18.0, 35 * KB, List.of("Protpars"),
                        Map.of("Protpars", 5 * MB))
        );
        return new Workflow("Gene2life", tasks);
    }

    public static Workflow avianfluSmall() {
        List<Task> tasks = new ArrayList<>();
        tasks.add(task("prepare", 120.0, 151 * KB, List.of(), Map.of()));
        tasks.add(task("autogrid", 240.0, 351 * KB, List.of("prepare"),
                Map.of("prepare", 100 * MB)));
        for (int jobIndex = 1; jobIndex <= 102; jobIndex++) {
            String taskId = String.format("autodock%03d", jobIndex);
            tasks.add(task(taskId, 1800.0, 7.75 * MB, List.of("autogrid"),
                    Map.of("autogrid", 200 * MB)));
        }
        return new Workflow("Avianflu_small", tasks);
    }

    public static Workflow epigenomics() {
        return layeredWorkflow("Epigenomics", 100, 2026L, 5, 12, 25.0, 240.0, 0.15, 0.60);
    }

    /**
     * CyberShake seismic-hazard analysis workflow (1 000 tasks).
     *
     * <p>Models a production CyberShake run on a national HPC facility.
     * The pipeline performs seismic wave propagation simulations for thousands
     * of rupture scenarios and aggregates them into a probabilistic hazard
     * curve.  Structured as three stages:
     * <ol>
     *   <li><b>PreProcess</b> — 50 parallel tasks that read large velocity
     *       models and seismic station metadata (~2 GB/task input).</li>
     *   <li><b>Simulate</b> — 900 CPU-intensive wave-propagation simulations,
     *       each consuming the pre-processed velocity model and outputting a
     *       synthetic seismogram file (~1 GB/task output).</li>
     *   <li><b>PostProcess</b> — 50 tasks that merge seismograms from the
     *       simulation stage into site-specific hazard curves.</li>
     * </ol>
     * Total simulated data volume: &gt; 1 TB, making this a textbook Big Data
     * scientific workflow.
     */
    public static Workflow cyberShake() {
        final int preCount = 50;
        final int simCount = 900;
        final int postCount = 50;

        List<Task> tasks = new ArrayList<>();

        // Stage 1: PreProcess tasks — read large velocity-model files.
        for (int i = 1; i <= preCount; i++) {
            String id = String.format("PreProcess_%03d", i);
            // 2 GB input per task, ~120 s CPU (velocity-model parsing).
            tasks.add(task(id, 120.0, 2.0 * GB, List.of(), Map.of()));
        }

        // Stage 2: Simulate — 900 wave-propagation jobs, each depending on
        // one pre-process task (round-robin assignment).
        for (int i = 1; i <= simCount; i++) {
            String id = String.format("Simulate_%04d", i);
            String predecessor = String.format("PreProcess_%03d", ((i - 1) % preCount) + 1);
            // Each job reads ~2 GB velocity model + outputs ~1 GB seismogram.
            tasks.add(task(id, 3600.0, 1.0 * GB, List.of(predecessor),
                    Map.of(predecessor, 2.0 * GB)));
        }

        // Stage 3: PostProcess — aggregate seismograms into hazard curves.
        // Each post-process task consumes the output of 18 simulation tasks.
        for (int i = 1; i <= postCount; i++) {
            String id = String.format("PostProcess_%03d", i);
            List<String> predecessors = new ArrayList<>();
            Map<String, Double> edgeData = new LinkedHashMap<>();
            int base = (i - 1) * (simCount / postCount);
            for (int j = 1; j <= simCount / postCount; j++) {
                String simId = String.format("Simulate_%04d", base + j);
                predecessors.add(simId);
                edgeData.put(simId, 1.0 * GB);
            }
            tasks.add(task(id, 600.0, (double) (simCount / postCount) * GB, predecessors, edgeData));
        }

        return new Workflow("CyberShake", tasks);
    }

    public static List<Workflow> defaultWorkflows() {
        return List.of(gene2life(), avianfluSmall(), epigenomics());
    }

    /**
     * Returns all workflows including the large-scale {@link #cyberShake()} workflow.
     * This is the full set that demonstrates the project's big-data capabilities.
     */
    public static List<Workflow> allWorkflows() {
        return List.of(gene2life(), avianfluSmall(), epigenomics(), cyberShake());
    }

    private static Workflow layeredWorkflow(
            String name,
            int taskCount,
            long seed,
            int minLayerSize,
            int maxLayerSize,
            double minWorkload,
            double maxWorkload,
            double minIo,
            double maxIo
    ) {
        Random random = new Random(seed);
        List<Task> tasks = new ArrayList<>();
        List<String> previousLayer = new ArrayList<>();
        int created = 0;

        while (created < taskCount) {
            int remaining = taskCount - created;
            int layerSize = Math.min(remaining, minLayerSize + random.nextInt(maxLayerSize - minLayerSize + 1));
            List<String> currentLayer = new ArrayList<>();
            for (int index = 0; index < layerSize; index++) {
                created++;
                String taskId = String.format("%s_t%03d", name, created);
                double workload = minWorkload + random.nextDouble() * (maxWorkload - minWorkload);
                double ioWeight = minIo + random.nextDouble() * (maxIo - minIo);
                List<String> predecessors;
                Map<String, Double> edgeData = new LinkedHashMap<>();
                if (previousLayer.isEmpty()) {
                    predecessors = List.of();
                } else {
                    int dependencyCount = Math.min(previousLayer.size(), 1 + random.nextInt(Math.min(3, previousLayer.size())));
                    predecessors = new ArrayList<>(previousLayer);
                    predecessors.sort(Comparator.naturalOrder());
                    while (predecessors.size() > dependencyCount) {
                        predecessors.remove(random.nextInt(predecessors.size()));
                    }
                    predecessors.sort(Comparator.naturalOrder());
                    for (String pred : predecessors) {
                        double edgeBytes = (50.0 + random.nextDouble() * 450.0) * MB;
                        edgeData.put(pred, edgeBytes);
                    }
                }
                tasks.add(new Task(taskId, workload, ioWeight, predecessors, edgeData));
                currentLayer.add(taskId);
            }
            previousLayer = currentLayer;
        }
        return new Workflow(name, tasks);
    }

    private static Task task(String id, double workloadSeconds, double totalBytes,
                             List<String> predecessors, Map<String, Double> edgeDataBytes) {
        return new Task(id, workloadSeconds, estimateIoWeight(totalBytes, workloadSeconds),
                predecessors, edgeDataBytes);
    }

    private static double estimateIoWeight(double totalBytes, double workloadSeconds) {
        double dataMb = totalBytes / MB;
        double ioSecondsEquivalent = dataMb / 25.0;
        double ioWeight = ioSecondsEquivalent / (ioSecondsEquivalent + workloadSeconds);
        return Math.max(0.05, Math.min(0.95, ioWeight));
    }

    private static final double KB = 1024.0;
    private static final double MB = 1024.0 * 1024.0;
    private static final double GB = 1024.0 * MB;
}
