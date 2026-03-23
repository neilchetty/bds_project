package org.gene2life.model;

public record NodeProfile(
        String clusterId,
        String nodeId,
        int cpuThreads,
        int ioBufferKb,
        int memoryMb,
        String cpuSet) {

    public NodeProfile(String clusterId, String nodeId, int cpuThreads, int ioBufferKb, int memoryMb) {
        this(clusterId, nodeId, cpuThreads, ioBufferKb, memoryMb, "");
    }

    public int effectiveReadBufferBytes() {
        return Math.max(8 * 1024, ioBufferKb * 1024);
    }

    public boolean hasDedicatedCpuSet() {
        return cpuSet != null && !cpuSet.isBlank();
    }
}
