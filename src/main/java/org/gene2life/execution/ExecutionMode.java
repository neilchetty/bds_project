package org.gene2life.execution;

public enum ExecutionMode {
    LOCAL,
    DOCKER;

    public static ExecutionMode fromCliValue(String value) {
        return switch (value.toLowerCase()) {
            case "local" -> LOCAL;
            case "docker" -> DOCKER;
            default -> throw new IllegalArgumentException("Unsupported executor: " + value);
        };
    }
}
