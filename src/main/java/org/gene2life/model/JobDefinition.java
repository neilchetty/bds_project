package org.gene2life.model;

import java.util.List;

public record JobDefinition(
        JobId id,
        String displayName,
        List<JobId> dependencies,
        String paperInputHint,
        String paperOutputHint) {
}
