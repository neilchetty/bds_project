package org.bds.wsh.io;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;

import org.bds.wsh.model.Task;
import org.bds.wsh.model.Workflow;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public final class DaxWorkflowLoader {
    public Workflow load(Path path) throws IOException {
        try {
            Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(path.toFile());
            document.getDocumentElement().normalize();

            Element root = document.getDocumentElement();
            String workflowName = root.getAttribute("name");
            if (workflowName == null || workflowName.isBlank()
                    || workflowName.equalsIgnoreCase("test")
                    || workflowName.equalsIgnoreCase("untitled")) {
                String fileName = path.getFileName().toString();
                int dot = fileName.lastIndexOf('.');
                workflowName = dot > 0 ? fileName.substring(0, dot) : fileName;
            }

            Map<String, TaskDraft> drafts = new LinkedHashMap<>();
            Map<String, Map<String, Long>> jobOutputFiles = new LinkedHashMap<>();
            Map<String, Map<String, Long>> jobInputFiles = new LinkedHashMap<>();

            NodeList jobs = root.getElementsByTagName("job");
            for (int index = 0; index < jobs.getLength(); index++) {
                Element job = (Element) jobs.item(index);
                String id = job.getAttribute("id");
                String runtimeRaw = job.getAttribute("runtime");
                double workloadSeconds = runtimeRaw == null || runtimeRaw.isBlank() ? 60.0 : Double.parseDouble(runtimeRaw);
                if (workloadSeconds <= 0.0) {
                    workloadSeconds = 60.0;
                }
                long inputBytes = 0L;
                long outputBytes = 0L;
                Map<String, Long> outputs = new LinkedHashMap<>();
                Map<String, Long> inputs = new LinkedHashMap<>();
                NodeList uses = job.getElementsByTagName("uses");
                for (int useIndex = 0; useIndex < uses.getLength(); useIndex++) {
                    Element use = (Element) uses.item(useIndex);
                    String fileName = use.getAttribute("file");
                    long size = parseSize(use.getAttribute("size"));
                    String link = use.getAttribute("link");
                    if ("output".equalsIgnoreCase(link)) {
                        outputBytes += size;
                        if (fileName != null && !fileName.isBlank()) {
                            outputs.put(fileName, size);
                        }
                    } else {
                        inputBytes += size;
                        if (fileName != null && !fileName.isBlank()) {
                            inputs.put(fileName, size);
                        }
                    }
                }
                jobOutputFiles.put(id, outputs);
                jobInputFiles.put(id, inputs);
                drafts.put(id, new TaskDraft(id, workloadSeconds, inputBytes, outputBytes));
            }

            NodeList children = root.getElementsByTagName("child");
            for (int index = 0; index < children.getLength(); index++) {
                Element child = (Element) children.item(index);
                String childId = child.getAttribute("ref");
                NodeList parents = child.getElementsByTagName("parent");
                for (int parentIndex = 0; parentIndex < parents.getLength(); parentIndex++) {
                    Element parent = (Element) parents.item(parentIndex);
                    String parentId = parent.getAttribute("ref");
                    drafts.get(childId).predecessors.add(parentId);

                    // Compute edge data: sum of file sizes shared between parent output and child input
                    Map<String, Long> pOutputs = jobOutputFiles.getOrDefault(parentId, Map.of());
                    Map<String, Long> cInputs = jobInputFiles.getOrDefault(childId, Map.of());
                    double sharedBytes = 0.0;
                    for (Map.Entry<String, Long> entry : pOutputs.entrySet()) {
                        if (cInputs.containsKey(entry.getKey())) {
                            sharedBytes += entry.getValue();
                        }
                    }
                    // Fallback: if no shared filenames, use parent's total output
                    if (sharedBytes == 0.0) {
                        sharedBytes = pOutputs.values().stream().mapToLong(Long::longValue).sum();
                    }
                    drafts.get(childId).edgeDataBytes.put(parentId, sharedBytes);
                }
            }

            List<Task> tasks = new ArrayList<>();
            for (TaskDraft draft : drafts.values()) {
                double ioWeight = estimateIoWeight(draft.inputBytes + draft.outputBytes, draft.workloadSeconds);
                tasks.add(new Task(draft.id, draft.workloadSeconds, ioWeight, draft.predecessors, draft.edgeDataBytes));
            }
            return new Workflow(workflowName, tasks);
        } catch (Exception exception) {
            throw new IOException("Failed to load DAX workflow from " + path, exception);
        }
    }

    private static double estimateIoWeight(long totalBytes, double workloadSeconds) {
        double dataMb = totalBytes / (1024.0 * 1024.0);
        double ioSecondsEquivalent = dataMb / 25.0;
        double ioWeight = ioSecondsEquivalent / (ioSecondsEquivalent + workloadSeconds);
        return Math.max(0.05, Math.min(0.95, ioWeight));
    }

    private static long parseSize(String raw) {
        if (raw == null || raw.isBlank()) {
            return 0L;
        }
        return (long) Double.parseDouble(raw);
    }

    private static final class TaskDraft {
        private final String id;
        private final double workloadSeconds;
        private final long inputBytes;
        private final long outputBytes;
        private final List<String> predecessors = new ArrayList<>();
        private final Map<String, Double> edgeDataBytes = new LinkedHashMap<>();

        private TaskDraft(String id, double workloadSeconds, long inputBytes, long outputBytes) {
            this.id = id;
            this.workloadSeconds = workloadSeconds;
            this.inputBytes = inputBytes;
            this.outputBytes = outputBytes;
        }
    }
}
