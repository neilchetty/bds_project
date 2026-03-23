package org.gene2life.model;

public record NodeProfile(
        String clusterId,
        String nodeId,
        int cpuThreads,
        int ioBufferKb,
        int memoryMb) {

    public int effectiveReadBufferBytes() {
        return Math.max(8 * 1024, ioBufferKb * 1024);
    }
}
