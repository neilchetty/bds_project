package org.bds.wsh.metrics;

public record MetricSet(
        String workflow,
        String algorithm,
        int nodeCount,
        double makespanSeconds,
        double slr,
        double speedup
) {
}
