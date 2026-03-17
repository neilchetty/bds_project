package org.bds.wsh.scheduler;

import org.bds.wsh.model.Node;
import org.bds.wsh.model.Task;

public interface RuntimeModel {
    double estimateSeconds(Task task, Node node);
}
