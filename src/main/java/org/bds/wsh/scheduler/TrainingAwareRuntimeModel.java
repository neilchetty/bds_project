package org.bds.wsh.scheduler;

import java.util.Map;

import org.bds.wsh.model.Node;
import org.bds.wsh.model.Task;

public final class TrainingAwareRuntimeModel implements RuntimeModel {
    private final RuntimeModel fallback;
    private final Map<String, ClusterTrainingProfile> profiles;
    private final double fastestCpuSeconds;
    private final double fastestIoSeconds;

    public TrainingAwareRuntimeModel(RuntimeModel fallback, Map<String, ClusterTrainingProfile> profiles) {
        this.fallback = fallback;
        this.profiles = Map.copyOf(profiles);
        this.fastestCpuSeconds = this.profiles.values().stream()
                .mapToDouble(ClusterTrainingProfile::cpuTrainingSeconds)
                .min()
                .orElse(Double.NaN);
        this.fastestIoSeconds = this.profiles.values().stream()
                .mapToDouble(ClusterTrainingProfile::ioTrainingSeconds)
                .min()
                .orElse(Double.NaN);
    }

    @Override
    public double estimateSeconds(Task task, Node node) {
        ClusterTrainingProfile profile = profiles.get(node.clusterId());
        if (profile == null || Double.isNaN(fastestCpuSeconds) || Double.isNaN(fastestIoSeconds)) {
            return fallback.estimateSeconds(task, node);
        }
        double computeScale = profile.cpuTrainingSeconds() / fastestCpuSeconds;
        double ioScale = profile.ioTrainingSeconds() / fastestIoSeconds;
        return task.workloadSeconds() * ((1.0 - task.ioWeight()) * computeScale + task.ioWeight() * ioScale);
    }
}
