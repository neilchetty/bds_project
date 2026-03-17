package org.bds.wsh.tests;

import java.nio.file.Path;

import org.bds.wsh.io.DaxWorkflowLoader;
import org.bds.wsh.model.Task;

public final class DaxLoaderTests {
    public void run() throws Exception {
        var workflow = new DaxWorkflowLoader().load(Path.of("src", "test", "resources", "sample-workflow.xml"));
        TestSupport.assertEquals(2, workflow.tasks().size(), "DAX loader should load two tasks.");
        TestSupport.assertTrue(workflow.tasks().containsKey("prep"), "DAX loader should include prep task.");
        TestSupport.assertTrue(workflow.tasks().get("compute").predecessors().contains("prep"), "DAX loader should preserve parent/child edges.");

        // Verify edge data: prep outputs prep.dat (2048 bytes), compute inputs prep.dat
        Task compute = workflow.tasks().get("compute");
        TestSupport.assertTrue(compute.edgeDataBytes().containsKey("prep"), "Edge data should be computed for prep->compute.");
        TestSupport.assertTrue(compute.edgeDataBytes().get("prep") > 0.0, "Edge data bytes must be positive.");
    }
}
