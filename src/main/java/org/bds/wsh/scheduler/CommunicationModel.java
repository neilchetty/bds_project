package org.bds.wsh.scheduler;

import java.util.List;

import org.bds.wsh.model.Node;

/**
 * Models data-transfer costs between nodes in a heterogeneous cluster.
 * <ul>
 *   <li>Same node &rarr; zero cost (data already local).</li>
 *   <li>Same sub-cluster &rarr; fast intra-cluster LAN.</li>
 *   <li>Different sub-clusters &rarr; slower inter-cluster link.</li>
 * </ul>
 * Bandwidth values are based on the paper's heterogeneous cluster setup
 * where sub-clusters are connected via a shared campus network.
 */
public final class CommunicationModel {

    /** Intra-cluster bandwidth: 1 Gbps = 125 MB/s. */
    private static final double INTRA_CLUSTER_BW = 125_000_000.0;

    /** Inter-cluster bandwidth: 16 Mbps = 2 MB/s (shared campus link between subclusters). */
    private static final double INTER_CLUSTER_BW = 2_000_000.0;

    /** Exposed for WSH savings computation. */
    static final double INTER_BW = INTER_CLUSTER_BW;
    static final double INTRA_BW = INTRA_CLUSTER_BW;

    private CommunicationModel() {
    }

    /** Transfer time across sub-clusters for the given data size. */
    public static double interClusterSeconds(double dataBytes) {
        return dataBytes > 0.0 ? dataBytes / INTER_CLUSTER_BW : 0.0;
    }

    /** Transfer time within a sub-cluster for the given data size. */
    public static double intraClusterSeconds(double dataBytes) {
        return dataBytes > 0.0 ? dataBytes / INTRA_CLUSTER_BW : 0.0;
    }

    /**
     * Returns the data-transfer time in seconds between two nodes.
     * Same node = 0, same cluster = data/INTRA_BW, different cluster = data/INTER_BW.
     */
    public static double communicationSeconds(Node from, Node to, double dataBytes) {
        if (dataBytes <= 0.0 || from.id().equals(to.id())) {
            return 0.0;
        }
        if (from.clusterId().equals(to.clusterId())) {
            return dataBytes / INTRA_CLUSTER_BW;
        }
        return dataBytes / INTER_CLUSTER_BW;
    }

    /**
     * Returns the average communication cost over all node pairs (including
     * same-node pairs with cost 0). Used in the upward-rank formula.
     */
    public static double averageCommunicationSeconds(List<Node> nodes, double dataBytes) {
        if (nodes.size() <= 1 || dataBytes <= 0.0) {
            return 0.0;
        }
        double total = 0.0;
        int count = nodes.size() * nodes.size();
        for (Node from : nodes) {
            for (Node to : nodes) {
                total += communicationSeconds(from, to, dataBytes);
            }
        }
        return total / count;
    }
}
