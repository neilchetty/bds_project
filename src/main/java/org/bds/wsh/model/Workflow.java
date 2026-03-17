package org.bds.wsh.model;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

public final class Workflow {
    private final String name;
    private final Map<String, Task> tasks;

    public Workflow(String name, Collection<Task> tasks) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Workflow name must not be blank.");
        }
        if (tasks == null || tasks.isEmpty()) {
            throw new IllegalArgumentException("Workflow must contain at least one task.");
        }
        this.name = name;
        LinkedHashMap<String, Task> ordered = new LinkedHashMap<>();
        for (Task task : tasks) {
            Task previous = ordered.put(task.id(), task);
            if (previous != null) {
                throw new IllegalArgumentException("Duplicate task id: " + task.id());
            }
        }
        this.tasks = Collections.unmodifiableMap(ordered);
        validate();
    }

    public String name() { return name; }
    public Map<String, Task> tasks() { return tasks; }

    public void validate() {
        for (Task task : tasks.values()) {
            for (String predecessor : task.predecessors()) {
                if (!tasks.containsKey(predecessor)) {
                    throw new IllegalArgumentException("Task '" + task.id() + "' references unknown predecessor '" + predecessor + "'.");
                }
            }
        }
        topologicalOrder();
    }

    public List<String> topologicalOrder() {
        Map<String, Integer> indegree = new LinkedHashMap<>();
        Map<String, List<String>> successors = successors();
        for (String taskId : tasks.keySet()) {
            indegree.put(taskId, 0);
        }
        for (Task task : tasks.values()) {
            indegree.put(task.id(), task.predecessors().size());
        }

        TreeSet<String> ready = new TreeSet<>();
        for (Map.Entry<String, Integer> entry : indegree.entrySet()) {
            if (entry.getValue() == 0) {
                ready.add(entry.getKey());
            }
        }

        ArrayList<String> order = new ArrayList<>();
        while (!ready.isEmpty()) {
            String current = ready.pollFirst();
            order.add(current);
            for (String successor : successors.get(current)) {
                int remaining = indegree.get(successor) - 1;
                indegree.put(successor, remaining);
                if (remaining == 0) {
                    ready.add(successor);
                }
            }
        }

        if (order.size() != tasks.size()) {
            throw new IllegalArgumentException("Workflow '" + name + "' must be a DAG.");
        }
        return Collections.unmodifiableList(order);
    }

    public Map<String, List<String>> successors() {
        LinkedHashMap<String, List<String>> successors = new LinkedHashMap<>();
        for (String taskId : tasks.keySet()) {
            successors.put(taskId, new ArrayList<>());
        }
        for (Task task : tasks.values()) {
            for (String predecessor : task.predecessors()) {
                successors.get(predecessor).add(task.id());
            }
        }
        for (Map.Entry<String, List<String>> entry : successors.entrySet()) {
            entry.getValue().sort(String::compareTo);
        }
        return successors;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Workflow workflow)) {
            return false;
        }
        return Objects.equals(name, workflow.name) && Objects.equals(tasks, workflow.tasks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, tasks);
    }
}
