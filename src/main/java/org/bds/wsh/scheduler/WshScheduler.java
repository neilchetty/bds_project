package org.bds.wsh.scheduler;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.bds.wsh.model.Node;
import org.bds.wsh.model.ScheduleResult;
import org.bds.wsh.model.ScheduledTask;
import org.bds.wsh.model.Task;
import org.bds.wsh.model.TaskKind;
import org.bds.wsh.model.Workflow;

/**
 * WSH (Workflow Scheduling for Heterogeneous computing) algorithm.
 *
 * Paper-faithful implementation of the scheduler described in:
 * "Scheduling of Big Data Workflows in the Hadoop Framework with Heterogeneous Computing Cluster" (2025).
 *
 * Key ideas (matching the paper's pseudocode):
 * - Prioritize jobs by upward rank (communication cost ignored).
 * - For each job, run a "training task" on the first node of each cluster to
 *   obtain FT[task, cluster] and sort clusters by FT (ties: IO tasks go to
 *   lower processing power).
 * - During scheduling, consider nodes by iterating clusters in that priority
 *   order, building a candidate node list based on whether nodes in a cluster
 *   have executed at least one job yet, then pick the node minimizing EFT.
 */
public final class WshScheduler extends AbstractScheduler {

    public WshScheduler() {
        this(new StaticRuntimeModel());
    }

    public WshScheduler(RuntimeModel runtimeModel) {
        super(runtimeModel);
    }

    @Override
    public String name() {
        return "WSH";
    }

    @Override
    public ScheduleResult schedule(Workflow workflow, List<Node> nodes) {
        List<String> jobPriorityQueue = prioritizedTasks(workflow, nodes);

        Map<String, Node> nodeMap = new LinkedHashMap<>();
        for (Node node : nodes) {
            nodeMap.put(node.id(), node);
        }
        Map<String, Double> nodeAvailable = new LinkedHashMap<>();
        for (Node node : nodes) {
            nodeAvailable.put(node.id(), 0.0);
        }
        Map<String, ScheduledTask> scheduled = new LinkedHashMap<>();

        // Pre-group nodes by cluster id (stable ordering by node id).
        Map<String, List<Node>> clusterNodes = nodes.stream()
                .collect(Collectors.groupingBy(Node::clusterId, LinkedHashMap::new,
                        Collectors.collectingAndThen(Collectors.toList(), list -> list.stream()
                                .sorted(Comparator.comparing(Node::id))
                                .toList())));

        // For each cluster, how many nodes are "unlocked" (i.e., have executed at least one job).
        // This matches the paper's progressive node consideration policy.
        Map<String, Integer> unlockedCount = new HashMap<>();
        for (String clusterId : clusterNodes.keySet()) {
            unlockedCount.put(clusterId, 0);
        }

        for (String taskId : jobPriorityQueue) {
            Task task = workflow.tasks().get(taskId);

            // TrainingTask_Order_cluster: sort clusters for this task by FT[task, firstNode(cluster)].
            List<String> clusterOrder = clusterOrderForTask(task, clusterNodes);

            // jobScheduling: build list-of-nodes based on cluster order + executed flags.
            CandidateList candidateList = buildCandidateList(clusterOrder, clusterNodes, unlockedCount);

            Candidate best = chooseBestNode(task, candidateList.nodes(), nodeAvailable, scheduled, nodeMap);

            scheduled.put(taskId, new ScheduledTask(taskId, best.node().id(), best.node().clusterId(),
                    best.startSeconds(), best.finishSeconds()));
            nodeAvailable.put(best.node().id(), best.finishSeconds());

            // Unlock expansion logic per paper:
            // Only when the selected node is the "new" node (last in list) do we unlock it.
            if (candidateList.newlyConsideredNode() != null && best.node().id().equals(candidateList.newlyConsideredNode().id())) {
                String c = best.node().clusterId();
                unlockedCount.put(c, Math.min(unlockedCount.get(c) + 1, clusterNodes.get(c).size()));

                // If we had to unlock on the last cluster in priority order, reset all clusters as "ready"
                // (paper: all nodes declared available when final cluster reaches next job).
                boolean lastCluster = c.equals(clusterOrder.get(clusterOrder.size() - 1));
                boolean unlockedLastNode = unlockedCount.get(c).equals(clusterNodes.get(c).size());
                if (lastCluster && unlockedLastNode) {
                    for (String clusterId : unlockedCount.keySet()) {
                        unlockedCount.put(clusterId, 0);
                    }
                }
            }
        }

        return new ScheduleResult(name(), workflow.name(), scheduled);
    }

    private List<String> clusterOrderForTask(Task task, Map<String, List<Node>> clusterNodes) {
        record ClusterScore(String clusterId, double trainingSeconds, double avgCpuFactor) { }

        List<ClusterScore> scores = clusterNodes.entrySet().stream()
                .map(e -> {
                    List<Node> nodes = e.getValue();
                    Node first = nodes.get(0);
                    double ft = runtimeModel.estimateSeconds(task, first);
                    double avgCpu = nodes.stream().mapToDouble(Node::cpuFactor).average().orElse(1.0);
                    return new ClusterScore(e.getKey(), ft, avgCpu);
                })
                .toList();

        boolean ioTask = task.kind() == TaskKind.IO_INTENSIVE;

        return scores.stream()
                .sorted((a, b) -> {
                    int cmp = Double.compare(a.trainingSeconds(), b.trainingSeconds());
                    if (cmp != 0) {
                        return cmp;
                    }
                    // If clusters tie in FT: IO tasks go to lowest processing power to keep fast nodes available.
                    if (ioTask) {
                        return Double.compare(a.avgCpuFactor(), b.avgCpuFactor());
                    }
                    return Double.compare(b.avgCpuFactor(), a.avgCpuFactor());
                })
                .map(ClusterScore::clusterId)
                .toList();
    }

    private record CandidateList(List<Node> nodes, Node newlyConsideredNode) { }

    private static CandidateList buildCandidateList(
            List<String> clusterOrder,
            Map<String, List<Node>> clusterNodes,
            Map<String, Integer> unlockedCount
    ) {
        List<Node> listOfNodes = new ArrayList<>();
        Node newlyConsidered = null;

        for (String clusterId : clusterOrder) {
            List<Node> nodes = clusterNodes.get(clusterId);
            int unlocked = unlockedCount.getOrDefault(clusterId, 0);
            if (unlocked >= nodes.size()) {
                listOfNodes.addAll(nodes);
                continue;
            }

            // Add all already-unlocked nodes (nodeA set), then add the next not-yet-unlocked node (nodeIn), then stop.
            if (unlocked > 0) {
                listOfNodes.addAll(nodes.subList(0, unlocked));
            }
            newlyConsidered = nodes.get(unlocked);
            listOfNodes.add(newlyConsidered);
            break;
        }

        if (listOfNodes.isEmpty()) {
            // Should not happen, but keep deterministic fallback.
            Node first = clusterNodes.values().iterator().next().get(0);
            listOfNodes.add(first);
            newlyConsidered = first;
        }
        return new CandidateList(listOfNodes, newlyConsidered);
    }
}
