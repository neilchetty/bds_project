package org.bds.wsh.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Task {
    private final String id;
    private final double workloadSeconds;
    private final double ioWeight;
    private final TaskKind kind;
    private final List<String> predecessors;
    private final Map<String, Double> edgeDataBytes;

    public Task(String id, double workloadSeconds, double ioWeight, List<String> predecessors) {
        this(id, workloadSeconds, ioWeight, predecessors, Collections.emptyMap());
    }

    public Task(String id, double workloadSeconds, double ioWeight, List<String> predecessors, Map<String, Double> edgeDataBytes) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Task id must not be blank.");
        }
        if (workloadSeconds <= 0.0) {
            throw new IllegalArgumentException("Task workload must be positive.");
        }
        if (ioWeight < 0.0 || ioWeight > 1.0) {
            throw new IllegalArgumentException("Task ioWeight must be in [0,1].");
        }
        this.id = id;
        this.workloadSeconds = workloadSeconds;
        this.ioWeight = ioWeight;
        this.kind = TaskKind.fromIoWeight(ioWeight);
        this.predecessors = Collections.unmodifiableList(new ArrayList<>(predecessors));
        this.edgeDataBytes = Collections.unmodifiableMap(new LinkedHashMap<>(edgeDataBytes));
    }

    public String id() { return id; }
    public double workloadSeconds() { return workloadSeconds; }
    public double ioWeight() { return ioWeight; }
    public TaskKind kind() { return kind; }
    public List<String> predecessors() { return predecessors; }
    /** Data bytes flowing from each predecessor to this task. */
    public Map<String, Double> edgeDataBytes() { return edgeDataBytes; }
}
