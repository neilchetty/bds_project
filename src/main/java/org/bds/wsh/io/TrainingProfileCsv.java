package org.bds.wsh.io;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bds.wsh.scheduler.ClusterTrainingProfile;

public final class TrainingProfileCsv {
    public Map<String, ClusterTrainingProfile> load(Path path) throws IOException {
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("Training profile file is empty: " + path);
        }
        String[] headers = lines.get(0).split(",");
        Map<String, Integer> headerIndex = new LinkedHashMap<>();
        for (int index = 0; index < headers.length; index++) {
            headerIndex.put(normalizeHeader(headers[index]), index);
        }
        Map<String, ClusterTrainingProfile> profiles = new LinkedHashMap<>();
        for (int lineIndex = 1; lineIndex < lines.size(); lineIndex++) {
            String line = lines.get(lineIndex).trim();
            if (line.isEmpty()) {
                continue;
            }
            String[] parts = line.split(",");
            String clusterId = value(parts, headerIndex, "cluster_id");
            String containerName = value(parts, headerIndex, "container_name", clusterId);
            double cpuSeconds = durationSeconds(parts, headerIndex, "cpu_training_seconds", "cpu_training_ms");
            double ioSeconds = durationSeconds(parts, headerIndex, "io_training_seconds", "io_training_ms");
            profiles.put(clusterId, new ClusterTrainingProfile(clusterId, cpuSeconds, ioSeconds, containerName));
        }
        return profiles;
    }

    private static String value(String[] parts, Map<String, Integer> headerIndex, String key) {
        return value(parts, headerIndex, key, null);
    }

    private static String value(String[] parts, Map<String, Integer> headerIndex, String key, String defaultValue) {
        Integer index = headerIndex.get(key);
        if (index == null) {
            if (defaultValue != null) {
                return defaultValue;
            }
            throw new IllegalArgumentException("Missing required CSV column: " + key);
        }
        return cleanValue(parts[index]);
    }

    private static double durationSeconds(String[] parts, Map<String, Integer> headerIndex, String secondsKey, String millisKey) {
        Integer secondsIndex = headerIndex.get(secondsKey);
        if (secondsIndex != null) {
            return Double.parseDouble(cleanValue(parts[secondsIndex]));
        }
        Integer millisIndex = headerIndex.get(millisKey);
        if (millisIndex != null) {
            return Double.parseDouble(cleanValue(parts[millisIndex])) / 1000.0;
        }
        throw new IllegalArgumentException("Missing duration column: " + secondsKey + " or " + millisKey);
    }

    private static String normalizeHeader(String raw) {
        return cleanValue(raw).toLowerCase();
    }

    private static String cleanValue(String raw) {
        return raw == null ? "" : raw.replace("\uFEFF", "").trim();
    }
}
