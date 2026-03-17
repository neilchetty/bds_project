package org.bds.wsh.tests;

import java.util.List;
import java.util.Map;

import org.bds.wsh.data.WorkflowDataSummary;
import org.bds.wsh.model.Task;
import org.bds.wsh.model.Workflow;
import org.bds.wsh.workflow.WorkflowLibrary;

public final class WorkflowDataSummaryTests {
    public void run() {
        testGene2lifeIsNotBigData();
        testAvianfluIsBigData();
        testCyberShakeIsVeryLargeBigData();
        testFormatBytes();
        testEdgeCounts();
        testAllWorkflowsHavePositiveData();
    }

    private void testGene2lifeIsNotBigData() {
        WorkflowDataSummary summary = new WorkflowDataSummary(WorkflowLibrary.gene2life());
        TestSupport.assertEquals(8, summary.taskCount(), "Gene2life should have 8 tasks.");
        // Gene2life total edge data is ~150 MB, which is below the 1 GB Big Data threshold.
        TestSupport.assertTrue(!summary.isBigData(),
                "Gene2life total edge data is < 1 GB and should not be classified as Big Data.");
        TestSupport.assertTrue(summary.totalEdgeDataBytes() > 0,
                "Gene2life should have positive edge data.");
    }

    private void testAvianfluIsBigData() {
        WorkflowDataSummary summary = new WorkflowDataSummary(WorkflowLibrary.avianfluSmall());
        TestSupport.assertEquals(104, summary.taskCount(), "Avianflu_small should have 104 tasks.");
        // 102 autodock jobs × 200 MB = ~20 GB total edge data.
        TestSupport.assertTrue(summary.isBigData(),
                "Avianflu_small transfers ~20 GB and must be classified as Big Data.");
        double totalGb = summary.totalEdgeDataBytes() / (1024.0 * 1024.0 * 1024.0);
        TestSupport.assertTrue(totalGb > 15.0,
                "Avianflu_small should transfer at least 15 GB.");
    }

    private void testCyberShakeIsVeryLargeBigData() {
        WorkflowDataSummary summary = new WorkflowDataSummary(WorkflowLibrary.cyberShake());
        TestSupport.assertEquals(1000, summary.taskCount(),
                "CyberShake should have exactly 1000 tasks (50 pre + 900 sim + 50 post).");
        TestSupport.assertTrue(summary.isBigData(),
                "CyberShake must be classified as Big Data.");
        // CyberShake has 900 simulation→pre-process edges (2 GB each) +
        // 50 post-process tasks each consuming 18 GB = total >> 1 TB.
        double totalTb = summary.totalEdgeDataBytes() / (1024.0 * 1024.0 * 1024.0 * 1024.0);
        TestSupport.assertTrue(totalTb >= 1.0,
                "CyberShake should transfer at least 1 TB in total.");
        TestSupport.assertTrue(summary.edgeCount() > 0,
                "CyberShake should have DAG edges.");
        TestSupport.assertTrue(summary.maxEdgeDataBytes() > 0,
                "CyberShake max edge should be positive.");
    }

    private void testFormatBytes() {
        TestSupport.assertTrue(WorkflowDataSummary.formatBytes(500 * 1024.0 * 1024.0).contains("MB"),
                "500 MB should be formatted as MB.");
        TestSupport.assertTrue(WorkflowDataSummary.formatBytes(2.5 * 1024.0 * 1024.0 * 1024.0).contains("GB"),
                "2.5 GB should be formatted as GB.");
        TestSupport.assertTrue(WorkflowDataSummary.formatBytes(1.5 * 1024.0 * 1024.0 * 1024.0 * 1024.0).contains("TB"),
                "1.5 TB should be formatted as TB.");
    }

    private void testEdgeCounts() {
        // A simple two-task workflow: one edge.
        Workflow simple = new Workflow("Simple", List.of(
                new Task("A", 10.0, 0.1, List.of(), Map.of()),
                new Task("B", 10.0, 0.1, List.of("A"), Map.of("A", 1_000_000.0))
        ));
        WorkflowDataSummary summary = new WorkflowDataSummary(simple);
        TestSupport.assertEquals(1, (int) summary.edgeCount(), "Simple workflow should have 1 edge.");
        TestSupport.assertClose(1_000_000.0, summary.totalEdgeDataBytes(), 1.0,
                "Total edge data should be 1 000 000 bytes.");
        TestSupport.assertClose(1_000_000.0, summary.maxEdgeDataBytes(), 1.0,
                "Max edge data should be 1 000 000 bytes.");
        TestSupport.assertClose(1_000_000.0, summary.avgEdgeDataBytes(), 1.0,
                "Avg edge data should be 1 000 000 bytes.");
    }

    private void testAllWorkflowsHavePositiveData() {
        for (Workflow wf : WorkflowLibrary.allWorkflows()) {
            WorkflowDataSummary summary = new WorkflowDataSummary(wf);
            TestSupport.assertTrue(summary.taskCount() > 0,
                    wf.name() + " should have at least one task.");
            TestSupport.assertTrue(summary.totalWorkloadSeconds() > 0,
                    wf.name() + " should have positive total workload.");
            String display = summary.toDisplayString();
            TestSupport.assertTrue(display.contains(wf.name()),
                    "toDisplayString should include the workflow name.");
        }
    }
}
