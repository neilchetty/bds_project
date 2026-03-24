package org.gene2life.workflow;

import java.util.Map;

public final class WorkflowRegistry {
    private static final Map<String, WorkflowSpec> WORKFLOWS = Map.of(
            "gene2life", new Gene2LifeWorkflowSpec(),
            "avianflu_small", new AvianfluSmallWorkflowSpec(),
            "epigenomics", new EpigenomicsWorkflowSpec());

    private WorkflowRegistry() {
    }

    public static WorkflowSpec byId(String workflowId) {
        WorkflowSpec spec = WORKFLOWS.get(workflowId.toLowerCase());
        if (spec == null) {
            throw new IllegalArgumentException("Unsupported workflow: " + workflowId);
        }
        return spec;
    }
}
