package org.bds.wsh.config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bds.wsh.model.Node;

/**
 * Factory for creating node configurations.
 * Supports loading from CSV files (with optional docker_host column for multi-machine setups)
 * and building paper-faithful cluster configurations.
 */
public final class ClusterFactory {
    private ClusterFactory() {
    }

    /**
     * Builds a heterogeneous cluster matching the paper's VM table (4 tiers).
     * Used for simulation-mode benchmarks without real Docker containers.
     */
    public static List<Node> buildPaperCluster(int totalNodes) {
        if (totalNodes <= 0) {
            throw new IllegalArgumentException("totalNodes must be positive.");
        }
        record Profile(String clusterId, double cpuFactor, double ioFactor, int ramMb) {}
        List<Profile> profiles = List.of(
                new Profile("C1", 4.0, 2.5, 4096),
                new Profile("C2", 2.0, 1.5, 2048),
                new Profile("C3", 1.0, 1.0, 1024),
                new Profile("C4", 0.5, 0.6, 512)
        );
        Map<String, Integer> counters = new LinkedHashMap<>();
        List<Node> nodes = new ArrayList<>();
        for (int index = 0; index < totalNodes; index++) {
            Profile profile = profiles.get(index % profiles.size());
            int ordinal = counters.getOrDefault(profile.clusterId(), 0) + 1;
            counters.put(profile.clusterId(), ordinal);
            String nodeId = profile.clusterId() + "-node" + ordinal;
            String containerName = "worker-" + profile.clusterId().toLowerCase() + "-" + ordinal;
            nodes.add(new Node(nodeId, profile.clusterId(), profile.cpuFactor(),
                    profile.ioFactor(), profile.ramMb(), containerName, null));
        }
        return nodes;
    }

    /**
     * Loads node configuration from a CSV file.
     * Required columns: node_id, cluster_id, cpu_factor, io_factor, ram_mb, container_name
     * Optional column: docker_host (for remote machines, e.g., "tcp://172.16.x.x:2375")
     */
    public static List<Node> loadFromCsv(Path path) throws IOException {
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("Node CSV file is empty: " + path);
        }

        // Parse header to find column indices.
        String[] headers = lines.get(0).split(",");
        Map<String, Integer> headerIndex = new LinkedHashMap<>();
        for (int i = 0; i < headers.length; i++) {
            headerIndex.put(headers[i].trim().toLowerCase().replace("\uFEFF", ""), i);
        }

        int dockerHostIdx = headerIndex.getOrDefault("docker_host", -1);

        List<Node> nodes = new ArrayList<>();
        for (int lineIdx = 1; lineIdx < lines.size(); lineIdx++) {
            String line = lines.get(lineIdx).trim();
            if (line.isBlank()) continue;
            String[] parts = line.split(",", -1);
            String dockerHost = (dockerHostIdx >= 0 && dockerHostIdx < parts.length)
                    ? parts[dockerHostIdx].trim() : null;
            nodes.add(new Node(
                    parts[0].trim(),
                    parts[1].trim(),
                    Double.parseDouble(parts[2].trim()),
                    Double.parseDouble(parts[3].trim()),
                    Integer.parseInt(parts[4].trim()),
                    parts.length > 5 ? parts[5].trim() : parts[0].trim(),
                    dockerHost
            ));
        }
        return nodes;
    }
}
