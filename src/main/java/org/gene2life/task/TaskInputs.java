package org.gene2life.task;

import java.nio.file.Path;

public record TaskInputs(Path primaryInput, Path secondaryInput, Path outputDirectory) {
}
