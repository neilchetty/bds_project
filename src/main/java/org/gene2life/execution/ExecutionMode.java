package org.gene2life.execution;

public enum ExecutionMode {
    LOCAL,
    DOCKER,
    HADOOP;

    public static ExecutionMode fromCliValue(String value) {
        return switch (value.toLowerCase()) {
            case "local" -> LOCAL;
            case "docker" -> DOCKER;
            case "hadoop" -> HADOOP;
            default -> throw new IllegalArgumentException("Unsupported executor: " + value);
        };
    }
}
