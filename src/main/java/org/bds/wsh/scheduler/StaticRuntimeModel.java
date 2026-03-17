package org.bds.wsh.scheduler;

import org.bds.wsh.model.Node;
import org.bds.wsh.model.Task;

public final class StaticRuntimeModel implements RuntimeModel {
    @Override
    public double estimateSeconds(Task task, Node node) {
        double computeTerm = (1.0 - task.ioWeight()) / node.cpuFactor();
        double ioTerm = task.ioWeight() / node.ioFactor();
        return task.workloadSeconds() * (computeTerm + ioTerm);
    }
}
