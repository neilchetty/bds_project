package org.bds.wsh.execution;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bds.wsh.model.Node;
import org.bds.wsh.model.Task;

/**
 * Executes real workloads on Docker containers via {@code docker exec}.
 * Supports both local and remote Docker hosts for multi-machine execution.
 *
 * <p>Each workflow task is translated into a concrete workload:
 * <ul>
 *   <li>CPU-intensive: parallel {@code dd if=/dev/urandom | sha256sum} chains</li>
 *   <li>IO-intensive: {@code dd} write + read cycles</li>
 * </ul>
 *
 * <p>Workload sizes are calibrated so execution time is proportional to the
 * DAX-specified runtime, producing real measurable differences between
 * heterogeneous container tiers.
 */
public final class WorkloadExecutor {

    /** Baseline MB/s for CPU work (sha256sum throughput on C3 tier). */
    private static final double CPU_MB_PER_SECOND = 80.0;

    /** Baseline MB/s for IO work (dd write+read throughput on C3 tier). */
    private static final double IO_MB_PER_SECOND = 100.0;

    /** Maximum workload in MB to prevent excessively long single-task runs. */
    private static final int MAX_MB = 2048;

    /** Minimum workload in MB to ensure measurable execution. */
    private static final int MIN_MB = 1;

    /** Timeout per task execution (10 minutes). */
    private static final long TIMEOUT_SECONDS = 600;

    /**
     * Executes the workload for a task on the specified container.
     *
     * @param task workflow task definition
     * @param node target node (contains container name and optional remote host)
     * @return execution result with wall-clock timing
     */
    public TaskExecutionResult execute(Task task, Node node) {
        String script = buildWorkloadScript(task);

        System.out.printf("  [EXEC] Task %-20s on %-25s ... ", task.id(), node.containerName());

        long startMs = System.currentTimeMillis();
        try {
            runDockerExec(node, script);
        } catch (Exception e) {
            System.out.println("FAILED: " + e.getMessage());
            throw new RuntimeException("Task " + task.id() + " failed on " + node.containerName(), e);
        }
        long finishMs = System.currentTimeMillis();

        System.out.printf("%.2fs%n", (finishMs - startMs) / 1000.0);
        return TaskExecutionResult.of(task.id(), node.id(), node.containerName(), startMs, finishMs);
    }

    /**
     * Time compression factor. The paper's workloads are huge (e.g., 1800s per task).
     * To run real benchmarks in ~20 minutes instead of days, we compress the actual
     * Docker CPU/IO workload by 50x. The schedules still use the exact paper 
     * values, so the algorithmic results and relative speedups are identical,
     * it just executes faster.
     */
    private static final double TIME_COMPRESSION_FACTOR = 50.0;

    /**
     * Builds a bash script producing real CPU + IO workload proportional
     * to the task's workloadSeconds and ioWeight.
     */
    String buildWorkloadScript(Task task) {
        double totalSec = task.workloadSeconds() / TIME_COMPRESSION_FACTOR;
        double ioW = task.ioWeight();
        double cpuW = 1.0 - ioW;

        int cpuMbRaw = (int) Math.round(cpuW * totalSec * CPU_MB_PER_SECOND);
        int ioMbRaw  = (int) Math.round(ioW * totalSec * IO_MB_PER_SECOND);
        int cpuMb = cpuMbRaw > 0 ? clamp(cpuMbRaw) : 0;
        int ioMb  = ioMbRaw  > 0 ? clamp(ioMbRaw)  : 0;

        StringBuilder s = new StringBuilder("set -e; ");

        // CPU: parallel sha256sum workers.
        if (cpuMb > 0) {
            int workers = 4;
            int perWorker = Math.max(1, cpuMb / workers);
            for (int i = 0; i < workers; i++) {
                s.append(String.format(
                        "dd if=/dev/urandom bs=1M count=%d status=none | sha256sum >/dev/null & ", perWorker));
            }
            s.append("wait; ");
        }

        // IO: sequential write then read.
        if (ioMb > 0) {
            String tmp = "/tmp/wsh-task-" + sanitize(task.id()) + ".bin";
            s.append(String.format("dd if=/dev/urandom of=%s bs=1M count=%d conv=fdatasync status=none; ", tmp, ioMb));
            s.append(String.format("dd if=%s of=/dev/null bs=1M status=none; ", tmp));
            s.append(String.format("rm -f %s; ", tmp));
        }

        if (cpuMb == 0 && ioMb == 0) {
            s.append("sleep 0.1; ");
        }

        return s.toString();
    }

    /**
     * Runs a command inside a Docker container.
     * Uses {@code docker -H <host>} for remote machines.
     */
    static void runDockerExec(Node node, String script) throws Exception {
        List<String> cmd = buildDockerCommand(node, script);
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        // Drain output.
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append('\n');
            }
        }

        boolean finished = process.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
            throw new RuntimeException("Timed out after " + TIMEOUT_SECONDS + "s");
        }
        if (process.exitValue() != 0) {
            throw new RuntimeException("Exit code " + process.exitValue() + ": " + output);
        }
    }

    /**
     * Builds the docker exec command, including -H flag for remote hosts.
     */
    static List<String> buildDockerCommand(Node node, String script) {
        List<String> cmd = new ArrayList<>();
        cmd.add("docker");
        if (node.isRemote()) {
            cmd.add("-H");
            cmd.add(node.dockerHost());
        }
        cmd.add("exec");
        cmd.add(node.containerName());
        cmd.add("bash");
        cmd.add("-c");
        cmd.add(script);
        return cmd;
    }

    private static int clamp(int mb) {
        return Math.max(MIN_MB, Math.min(MAX_MB, mb));
    }

    static String sanitize(String id) {
        return id.replaceAll("[^a-zA-Z0-9_-]", "_");
    }
}
