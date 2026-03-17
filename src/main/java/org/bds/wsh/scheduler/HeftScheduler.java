package org.bds.wsh.scheduler;

import java.util.ArrayList;
import java.util.List;

import org.bds.wsh.model.Node;
import org.bds.wsh.model.ScheduleResult;
import org.bds.wsh.model.Workflow;

public final class HeftScheduler extends AbstractScheduler {
    public HeftScheduler() {
        this(new StaticRuntimeModel());
    }

    public HeftScheduler(RuntimeModel runtimeModel) {
        super(runtimeModel);
    }

    @Override
    public String name() {
        return "HEFT";
    }

    @Override
    public ScheduleResult schedule(Workflow workflow, List<Node> nodes) {
        return scheduleWithCandidates(workflow, nodes, (task, wf, allNodes, scheduledTasks, nodeAvailable) -> new ArrayList<>(allNodes));
    }
}
