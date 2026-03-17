package org.bds.wsh.tests;

import java.util.List;

import org.bds.wsh.config.ClusterFactory;
import org.bds.wsh.model.ScheduleResult;
import org.bds.wsh.model.ScheduledTask;
import org.bds.wsh.model.Workflow;
import org.bds.wsh.scheduler.HeftScheduler;
import org.bds.wsh.scheduler.WshScheduler;
import org.bds.wsh.workflow.WorkflowLibrary;

public final class SchedulerTests {
    public void run() {
        List<Workflow> workflows = WorkflowLibrary.defaultWorkflows();
        var nodes = ClusterFactory.buildPaperCluster(7);
        HeftScheduler heft = new HeftScheduler();
        WshScheduler wsh = new WshScheduler();

        for (Workflow workflow : workflows) {
            ScheduleResult heftResult = heft.schedule(workflow, nodes);
            ScheduleResult wshResult = wsh.schedule(workflow, nodes);
            TestSupport.assertEquals(workflow.tasks().size(), heftResult.scheduledTasks().size(), "HEFT should schedule every task.");
            TestSupport.assertEquals(workflow.tasks().size(), wshResult.scheduledTasks().size(), "WSH should schedule every task.");
            assertDependenciesRespected(workflow, heftResult);
            assertDependenciesRespected(workflow, wshResult);
        }

        ScheduleResult first = wsh.schedule(WorkflowLibrary.gene2life(), ClusterFactory.buildPaperCluster(10));
        ScheduleResult second = wsh.schedule(WorkflowLibrary.gene2life(), ClusterFactory.buildPaperCluster(10));
        TestSupport.assertClose(first.makespanSeconds(), second.makespanSeconds(), 1e-9, "WSH must be deterministic.");
    }

    private void assertDependenciesRespected(Workflow workflow, ScheduleResult result) {
        for (var entry : workflow.tasks().entrySet()) {
            ScheduledTask scheduledTask = result.scheduledTasks().get(entry.getKey());
            for (String predecessor : entry.getValue().predecessors()) {
                double predecessorFinish = result.scheduledTasks().get(predecessor).finishSeconds();
                TestSupport.assertTrue(scheduledTask.startSeconds() + 1e-9 >= predecessorFinish,
                        "Task " + entry.getKey() + " starts before predecessor " + predecessor + " finishes.");
            }
        }
    }
}
