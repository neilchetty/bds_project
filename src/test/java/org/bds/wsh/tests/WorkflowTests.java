package org.bds.wsh.tests;

import java.util.List;

import org.bds.wsh.model.Task;
import org.bds.wsh.model.Workflow;
import org.bds.wsh.workflow.WorkflowLibrary;

public final class WorkflowTests {
    public void run() {
        Workflow workflow = WorkflowLibrary.gene2life();
        Workflow avianflu = WorkflowLibrary.avianfluSmall();
        TestSupport.assertEquals(8, workflow.tasks().size(), "Gene2life should contain 8 tasks.");
        TestSupport.assertTrue(workflow.topologicalOrder().size() == 8, "Topological order should include all tasks.");
        TestSupport.assertEquals(104, avianflu.tasks().size(), "Avianflu_small should contain 104 tasks.");
        TestSupport.assertTrue(avianflu.tasks().get("autogrid").predecessors().contains("prepare"), "autogrid should depend on prepare.");
        TestSupport.assertTrue(avianflu.tasks().get("autodock001").predecessors().contains("autogrid"), "autodock tasks should depend on autogrid.");

        boolean failed = false;
        try {
            new Workflow("Broken", List.of(
                    new Task("A", 10.0, 0.2, List.of("B")),
                    new Task("B", 10.0, 0.2, List.of("A"))
            ));
        } catch (IllegalArgumentException expected) {
            failed = true;
        }
        TestSupport.assertTrue(failed, "Cyclic workflow should fail validation.");
    }
}
