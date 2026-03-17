package org.bds.wsh.cli;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class MetricsVerifier {
    public void verify(Path metricsPath) throws IOException {
        if (!Files.exists(metricsPath)) {
            throw new IllegalStateException(metricsPath + " does not exist.");
        }
        List<String> lines = Files.readAllLines(metricsPath, StandardCharsets.UTF_8);
        if (lines.size() <= 1) {
            throw new IllegalStateException(metricsPath + " does not contain metric rows.");
        }
        Set<String> workflows = new HashSet<>();
        Set<String> algorithms = new HashSet<>();
        Set<Integer> nodeCounts = new HashSet<>();
        for (int index = 1; index < lines.size(); index++) {
            String[] parts = lines.get(index).split(",");
            workflows.add(parts[0]);
            algorithms.add(parts[1]);
            nodeCounts.add(Integer.parseInt(parts[2]));
            double makespan = Double.parseDouble(parts[3]);
            double slr = Double.parseDouble(parts[4]);
            double speedup = Double.parseDouble(parts[5]);
            if (makespan <= 0.0 || slr <= 0.0 || speedup <= 0.0) {
                throw new IllegalStateException("Non-positive metric in row " + index + ": " + lines.get(index));
            }
        }
        if (!algorithms.containsAll(Set.of("HEFT", "WSH"))) {
            throw new IllegalStateException("Missing expected algorithms in metrics file.");
        }
    }
}
