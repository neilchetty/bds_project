package org.bds.wsh.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.bds.wsh.model.Node;
import org.bds.wsh.model.ScheduledTask;
import org.bds.wsh.model.Task;

/**
 * Handles real data transfer between Docker containers, including
 * cross-machine transfers over the network.
 *
 * <p>When a task's predecessor ran on a different container:
 * <ul>
 *   <li>Same machine → pipe via docker exec (fast, local)</li>
 *   <li>Different machines → pipe across Docker hosts via network (produces real
 *       inter-cluster network traffic matching the paper's setup)</li>
 * </ul>
 */
public final class DataTransferManager {

    private static final long TIMEOUT_SECONDS = 300;

    /**
     * Transfers predecessor output data to the target container.
     * Skips if predecessor is on the same container.
     */
    public void transferPredecessorData(Task task, Node targetNode,
                                        Map<String, ScheduledTask> scheduledTasks,
                                        Map<String, Node> nodeMap) {
        for (String predId : task.predecessors()) {
            ScheduledTask predScheduled = scheduledTasks.get(predId);
            if (predScheduled == null) continue;

            Node sourceNode = nodeMap.get(predScheduled.nodeId());
            if (sourceNode == null) continue;

            // Same container → data is already local, skip.
            if (sourceNode.containerName().equals(targetNode.containerName())
                    && sameHost(sourceNode, targetNode)) {
                continue;
            }

            // Determine data size from workflow edge data.
            double dataBytes = task.edgeDataBytes().getOrDefault(predId, 0.0);
            if (dataBytes <= 0.0) continue;

            int dataMb = Math.max(1, Math.min(512, (int) Math.round(dataBytes / (1024.0 * 1024.0))));
            String xferId = WorkloadExecutor.sanitize(predId) + "_to_" + WorkloadExecutor.sanitize(task.id());

            System.out.printf("  [XFER] %s → %s (%d MB) ... ",
                    sourceNode.containerName(), targetNode.containerName(), dataMb);

            try {
                transfer(sourceNode, targetNode, xferId, dataMb);
                System.out.println("done");
            } catch (Exception e) {
                System.out.println("FAILED: " + e.getMessage());
                throw new RuntimeException("Transfer failed: " + predId + " → " + task.id(), e);
            }
        }
    }

    /**
     * Transfers data between containers. Generates data on source, pipes through
     * host(s), writes to target. For cross-machine transfers, this produces real
     * network traffic over the LAN.
     */
    private void transfer(Node source, Node target, String xferId, int dataMb) throws Exception {
        String sourceFile = "/tmp/wsh-xfer-" + xferId + ".bin";
        String targetFile = "/tmp/wsh-xfer-" + xferId + ".bin";

        // Generate data on source container.
        WorkloadExecutor.runDockerExec(source,
                String.format("dd if=/dev/urandom of=%s bs=1M count=%d status=none", sourceFile, dataMb));

        // Pipe: read from source container → write to target container.
        List<String> readCmd = new ArrayList<>();
        readCmd.add("docker");
        if (source.isRemote()) { readCmd.add("-H"); readCmd.add(source.dockerHost()); }
        readCmd.add("exec");
        readCmd.add(source.containerName());
        readCmd.add("cat");
        readCmd.add(sourceFile);

        List<String> writeCmd = new ArrayList<>();
        writeCmd.add("docker");
        if (target.isRemote()) { writeCmd.add("-H"); writeCmd.add(target.dockerHost()); }
        writeCmd.add("exec");
        writeCmd.add("-i");
        writeCmd.add(target.containerName());
        writeCmd.add("bash");
        writeCmd.add("-c");
        writeCmd.add("cat > " + targetFile);

        ProcessBuilder readPb = new ProcessBuilder(readCmd);
        readPb.redirectErrorStream(false);
        Process readProc = readPb.start();

        ProcessBuilder writePb = new ProcessBuilder(writeCmd);
        writePb.redirectErrorStream(true);
        Process writeProc = writePb.start();

        // Pipe data: source stdout → target stdin.
        Thread pipeThread = new Thread(() -> {
            try {
                readProc.getInputStream().transferTo(writeProc.getOutputStream());
                writeProc.getOutputStream().close();
            } catch (Exception ignored) { }
        });
        pipeThread.start();

        boolean readDone = readProc.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        pipeThread.join(TIMEOUT_SECONDS * 1000);
        boolean writeDone = writeProc.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        if (!readDone || !writeDone) {
            readProc.destroyForcibly();
            writeProc.destroyForcibly();
            throw new RuntimeException("Transfer timed out");
        }

        if (readProc.exitValue() != 0) {
            throw new RuntimeException("Transfer read failed with exit code " + readProc.exitValue());
        }
        if (writeProc.exitValue() != 0) {
            throw new RuntimeException("Transfer write failed with exit code " + writeProc.exitValue());
        }

        // Cleanup source and target files.
        WorkloadExecutor.runDockerExec(source, "rm -f " + sourceFile);
        WorkloadExecutor.runDockerExec(target, "rm -f " + targetFile);
    }

    private static boolean sameHost(Node a, Node b) {
        if (a.dockerHost() == null && b.dockerHost() == null) return true;
        if (a.dockerHost() == null || b.dockerHost() == null) return false;
        return a.dockerHost().equals(b.dockerHost());
    }
}
