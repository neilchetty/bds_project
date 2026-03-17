package org.bds.wsh.scheduler;

import java.util.List;

import org.bds.wsh.model.Node;
import org.bds.wsh.model.ScheduleResult;
import org.bds.wsh.model.Workflow;

public interface Scheduler {
    String name();
    ScheduleResult schedule(Workflow workflow, List<Node> nodes);
}
