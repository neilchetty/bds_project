package org.gene2life.execution;

import org.gene2life.model.JobId;
import org.gene2life.model.NodeProfile;
import org.gene2life.task.TaskInputs;
import org.gene2life.task.TaskResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class DockerNodePool implements AutoCloseable {
    private final String dockerImage;
    private final Path mountRoot;
    private final Map<String, NodeProfile> nodesByContainerName;

    public DockerNodePool(String dockerImage, Path mountRoot, String namespace, List<NodeProfile> nodes) throws IOException, InterruptedException {
        this.dockerImage = dockerImage;
        this.mountRoot = mountRoot.toAbsolutePath();
        this.nodesByContainerName = new LinkedHashMap<>();
        Files.createDirectories(this.mountRoot);
        for (NodeProfile node : nodes) {
            String containerName = containerName(namespace, node);
            nodesByContainerName.put(containerName, node);
            removeIfExists(containerName);
            startContainer(containerName, node);
        }
    }

    public TaskResult execute(NodeProfile nodeProfile, JobId jobId, TaskInputs inputs) throws IOException, InterruptedException {
        String containerName = findContainerName(nodeProfile);
        List<String> command = new java.util.ArrayList<>();
        command.add("docker");
        command.add("exec");
        command.add(containerName);
        command.add("java");
        command.add("-cp");
        command.add("/app/classes");
        command.add("org.gene2life.cli.Main");
        command.add("run-job");
        command.add("--job");
        command.add(jobId.cliName());
        command.add("--primary-input");
        command.add(inputs.primaryInput().toAbsolutePath().toString());
        if (inputs.secondaryInput() != null) {
            command.add("--secondary-input");
            command.add(inputs.secondaryInput().toAbsolutePath().toString());
        }
        command.add("--output-dir");
        command.add(inputs.outputDirectory().toAbsolutePath().toString());
        command.add("--cluster-id");
        command.add(nodeProfile.clusterId());
        command.add("--node-id");
        command.add(nodeProfile.nodeId());
        command.add("--cpu-threads");
        command.add(Integer.toString(nodeProfile.cpuThreads()));
        command.add("--io-buffer-kb");
        command.add(Integer.toString(nodeProfile.ioBufferKb()));
        command.add("--memory-mb");
        command.add(Integer.toString(nodeProfile.memoryMb()));
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();
        byte[] combined = process.getInputStream().readAllBytes();
        int exit = process.waitFor();
        if (exit != 0) {
            throw new IOException("Docker exec failed with exit code " + exit + " for " + jobId.cliName()
                    + System.lineSeparator() + new String(combined, StandardCharsets.UTF_8));
        }
        Path outputPath = JobOutputs.outputPath(jobId, inputs.outputDirectory());
        if (!Files.exists(outputPath)) {
            throw new IOException("Expected output not found after docker exec: " + outputPath);
        }
        return new TaskResult(outputPath, JobOutputs.outputDescription(jobId));
    }

    @Override
    public void close() {
        for (String containerName : nodesByContainerName.keySet()) {
            try {
                removeIfExists(containerName);
            } catch (Exception ignored) {
            }
        }
    }

    private void startContainer(String containerName, NodeProfile nodeProfile) throws IOException, InterruptedException {
        List<String> command = List.of(
                "docker", "run", "-d", "--rm",
                "--name", containerName,
                "--cpus", Integer.toString(nodeProfile.cpuThreads()),
                "--memory", nodeProfile.memoryMb() + "m",
                "-v", mountRoot.toString() + ":" + mountRoot.toString(),
                "--entrypoint", "sh",
                dockerImage,
                "-lc",
                "while true; do sleep 3600; done");
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();
        byte[] combined = process.getInputStream().readAllBytes();
        int exit = process.waitFor();
        if (exit != 0) {
            throw new IOException("Failed to start docker node container " + containerName
                    + System.lineSeparator() + new String(combined, StandardCharsets.UTF_8));
        }
    }

    private void removeIfExists(String containerName) throws IOException, InterruptedException {
        Process process = new ProcessBuilder("docker", "rm", "-f", containerName)
                .redirectErrorStream(true)
                .start();
        process.getInputStream().readAllBytes();
        process.waitFor();
    }

    private String findContainerName(NodeProfile nodeProfile) {
        return nodesByContainerName.entrySet().stream()
                .filter(entry -> entry.getValue().nodeId().equals(nodeProfile.nodeId()))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No container registered for node " + nodeProfile.nodeId()));
    }

    private static String containerName(String namespace, NodeProfile nodeProfile) {
        String safeNamespace = namespace.toLowerCase().replaceAll("[^a-z0-9]+", "-");
        if (safeNamespace.length() > 24) {
            safeNamespace = safeNamespace.substring(0, 24);
        }
        return "g2l-" + safeNamespace + "-" + nodeProfile.nodeId().toLowerCase();
    }
}
