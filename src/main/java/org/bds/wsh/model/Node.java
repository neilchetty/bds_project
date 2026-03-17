package org.bds.wsh.model;

import java.util.Objects;

/**
 * Represents a compute node in the heterogeneous cluster.
 * Each node belongs to a cluster tier (C1-C4) and maps to a Docker container
 * that may run on a local or remote machine.
 */
public final class Node {
    private final String id;
    private final String clusterId;
    private final double cpuFactor;
    private final double ioFactor;
    private final int ramMb;
    private final String containerName;
    private final String dockerHost; // null = localhost, otherwise "tcp://ip:2375"

    public Node(String id, String clusterId, double cpuFactor, double ioFactor,
                int ramMb, String containerName, String dockerHost) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Node id must not be blank.");
        }
        if (clusterId == null || clusterId.isBlank()) {
            throw new IllegalArgumentException("Cluster id must not be blank.");
        }
        if (cpuFactor <= 0.0 || ioFactor <= 0.0) {
            throw new IllegalArgumentException("Node factors must be positive.");
        }
        if (ramMb <= 0) {
            throw new IllegalArgumentException("Node RAM must be positive.");
        }
        this.id = id;
        this.clusterId = clusterId;
        this.cpuFactor = cpuFactor;
        this.ioFactor = ioFactor;
        this.ramMb = ramMb;
        this.containerName = containerName == null ? id : containerName;
        this.dockerHost = (dockerHost == null || dockerHost.isBlank()) ? null : dockerHost.trim();
    }

    /** Backward-compatible constructor (localhost). */
    public Node(String id, String clusterId, double cpuFactor, double ioFactor,
                int ramMb, String containerName) {
        this(id, clusterId, cpuFactor, ioFactor, ramMb, containerName, null);
    }

    public String id() { return id; }
    public String clusterId() { return clusterId; }
    public double cpuFactor() { return cpuFactor; }
    public double ioFactor() { return ioFactor; }
    public int ramMb() { return ramMb; }
    public String containerName() { return containerName; }

    /**
     * Docker host for remote execution. Null means localhost.
     * Format: "tcp://172.16.x.x:2375"
     */
    public String dockerHost() { return dockerHost; }

    /** Whether this node runs on a remote machine. */
    public boolean isRemote() { return dockerHost != null; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof Node node)) return false;
        return id.equals(node.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        String host = dockerHost != null ? "@" + dockerHost : "";
        return id + "[" + clusterId + host + "]";
    }
}
