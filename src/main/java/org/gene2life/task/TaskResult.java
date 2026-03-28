package org.gene2life.task;

import java.nio.file.Path;

public record TaskResult(Path outputPath, String description) {
}
