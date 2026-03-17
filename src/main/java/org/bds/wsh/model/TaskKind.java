package org.bds.wsh.model;

public enum TaskKind {
    COMPUTE_INTENSIVE,
    IO_INTENSIVE;

    public static TaskKind fromIoWeight(double ioWeight) {
        return ioWeight >= 0.5 ? IO_INTENSIVE : COMPUTE_INTENSIVE;
    }
}
