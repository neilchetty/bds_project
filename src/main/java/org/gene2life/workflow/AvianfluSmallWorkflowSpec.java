package org.gene2life.workflow;

import org.gene2life.cli.CliArguments;
import org.gene2life.model.JobDefinition;
import org.gene2life.model.JobRun;
import org.gene2life.model.TaskType;
import org.gene2life.model.WorkflowDefinition;
import org.gene2life.task.TaskInputs;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Future;

public final class AvianfluSmallWorkflowSpec implements WorkflowSpec {
    private static final int DEFAULT_DOCKING_TASKS = 100;
    private static final WorkflowDefinition DEFINITION = buildDefinition(DEFAULT_DOCKING_TASKS);

    @Override
    public WorkflowDefinition definition() {
        return DEFINITION;
    }

    @Override
    public void generateData(Path dataRoot, CliArguments cli) throws Exception {
        Files.createDirectories(dataRoot.resolve("ligands"));
        Files.createDirectories(dataRoot.resolve("training/ligands"));
        Random random = new Random(Long.parseLong(cli.option("seed", "42")));
        int dockingTasks = dockingTaskCount();
        int receptorFeatures = cli.optionInt("receptor-feature-count", 256);
        int ligandAtoms = cli.optionInt("ligand-atom-count", 48);
        int trainingLigands = Math.max(8, Math.min(16, dockingTasks / 8));

        try (BufferedWriter writer = Files.newBufferedWriter(dataRoot.resolve("receptor.tsv"), StandardCharsets.UTF_8)) {
            writer.write("feature_id\tx\ty\tz\tweight");
            writer.newLine();
            for (int i = 0; i < receptorFeatures; i++) {
                writer.write("rec-" + i + "\t" + random.nextInt(40) + "\t" + random.nextInt(40)
                        + "\t" + random.nextInt(40) + "\t" + String.format("%.5f", 0.2 + random.nextDouble()));
                writer.newLine();
            }
        }
        Files.copy(dataRoot.resolve("receptor.tsv"), dataRoot.resolve("training/receptor.tsv"), java.nio.file.StandardCopyOption.REPLACE_EXISTING);

        try (BufferedWriter writer = Files.newBufferedWriter(dataRoot.resolve("grid-template.tsv"), StandardCharsets.UTF_8)) {
            writer.write("cell_id\tx\ty\tz");
            writer.newLine();
            for (int i = 0; i < 64; i++) {
                writer.write("cell-" + i + "\t" + (i % 8) + "\t" + ((i / 8) % 8) + "\t" + (i / 16));
                writer.newLine();
            }
        }
        Files.copy(dataRoot.resolve("grid-template.tsv"), dataRoot.resolve("training/grid-template.tsv"), java.nio.file.StandardCopyOption.REPLACE_EXISTING);

        try (BufferedWriter writer = Files.newBufferedWriter(dataRoot.resolve("ligand-library.tsv"), StandardCharsets.UTF_8)) {
            writer.write("ligand_id\tfamily\tpolarity\tflexibility");
            writer.newLine();
            for (int i = 1; i <= dockingTasks; i++) {
                writer.write("ligand-" + pad(i) + "\tfam-" + (i % 11) + "\t"
                        + String.format("%.4f", 0.1 + random.nextDouble())
                        + "\t" + String.format("%.4f", 0.1 + random.nextDouble()));
                writer.newLine();
            }
        }
        copyLigandLibrarySubset(dataRoot.resolve("ligand-library.tsv"), dataRoot.resolve("training/ligand-library.tsv"), trainingLigands);

        for (int i = 1; i <= dockingTasks; i++) {
            writeLigandFile(dataRoot.resolve("ligands").resolve("ligand-" + pad(i) + ".tsv"), ligandAtoms, random, i);
        }
        for (int i = 1; i <= trainingLigands; i++) {
            Files.copy(
                    dataRoot.resolve("ligands").resolve("ligand-" + pad(i) + ".tsv"),
                    dataRoot.resolve("training/ligands").resolve("ligand-" + pad(i) + ".tsv"),
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Override
    public TaskInputs resolveInputs(String jobId, Path dataRoot, Path runRoot, Map<String, Future<JobRun>> futures) throws Exception {
        Path outputDirectory = runRoot.resolve("jobs").resolve(jobId);
        Files.createDirectories(outputDirectory);
        return switch (jobId) {
            case "prepare-receptor" -> new TaskInputs(List.of(dataRoot.resolve("receptor.tsv")), outputDirectory, Map.of());
            case "prepare-gpf" -> new TaskInputs(List.of(futures.get("prepare-receptor").get().outputPath(), dataRoot.resolve("grid-template.tsv")), outputDirectory, Map.of());
            case "prepare-dpf" -> new TaskInputs(List.of(dataRoot.resolve("ligand-library.tsv")), outputDirectory, Map.of());
            case "auto-grid" -> new TaskInputs(List.of(futures.get("prepare-gpf").get().outputPath()), outputDirectory, Map.of());
            default -> {
                if (!jobId.startsWith("autodock-")) {
                    throw new IllegalArgumentException("Unknown avianflu_small job: " + jobId);
                }
                String ligandId = jobId.substring("autodock-".length());
                yield new TaskInputs(
                        List.of(
                                futures.get("auto-grid").get().outputPath(),
                                futures.get("prepare-dpf").get().outputPath(),
                                dataRoot.resolve("ligands").resolve("ligand-" + ligandId + ".tsv")),
                        outputDirectory,
                        Map.of("ligand_id", ligandId));
            }
        };
    }

    @Override
    public TaskInputs resolveTrainingInputs(String jobId, Path dataRoot) {
        Path outputDirectory = dataRoot.resolve("training/generated").resolve(jobId);
        return switch (jobId) {
            case "prepare-receptor" -> new TaskInputs(List.of(dataRoot.resolve("training/receptor.tsv")), outputDirectory, Map.of());
            case "prepare-gpf" -> new TaskInputs(List.of(trainingOutputPath(dataRoot, "prepare-receptor"), dataRoot.resolve("training/grid-template.tsv")), outputDirectory, Map.of());
            case "prepare-dpf" -> new TaskInputs(List.of(dataRoot.resolve("training/ligand-library.tsv")), outputDirectory, Map.of());
            case "auto-grid" -> new TaskInputs(List.of(trainingOutputPath(dataRoot, "prepare-gpf")), outputDirectory, Map.of());
            case "autodock-001" -> new TaskInputs(
                    List.of(
                            trainingOutputPath(dataRoot, "auto-grid"),
                            trainingOutputPath(dataRoot, "prepare-dpf"),
                            dataRoot.resolve("training/ligands").resolve("ligand-001.tsv")),
                    outputDirectory,
                    Map.of("ligand_id", "001"));
            default -> throw new IllegalArgumentException("Unknown avianflu_small training job: " + jobId);
        };
    }

    private static WorkflowDefinition buildDefinition(int dockingTasks) {
        List<JobDefinition> jobs = new ArrayList<>();
        jobs.add(new JobDefinition("prepare-receptor", "Prepare Receptor", List.of(), TaskType.PREPARE_RECEPTOR, 18_000L,
                "prepare-receptor", "receptor model", "prepared receptor", Map.of()));
        jobs.add(new JobDefinition("prepare-gpf", "PrepareGPF", List.of("prepare-receptor"), TaskType.PREPARE_GPF, 16_000L,
                "prepare-gpf", "receptor + grid template", "grid parameters", Map.of()));
        jobs.add(new JobDefinition("prepare-dpf", "PrepareDPF", List.of(), TaskType.PREPARE_DPF, 14_000L,
                "prepare-dpf", "ligand library", "docking parameters", Map.of()));
        jobs.add(new JobDefinition("auto-grid", "AutoGrid", List.of("prepare-gpf"), TaskType.AUTOGRID, 24_000L,
                "auto-grid", "grid parameters", "autogrid cells", Map.of()));
        for (int i = 1; i <= dockingTasks; i++) {
            String ligandId = pad(i);
            jobs.add(new JobDefinition("autodock-" + ligandId, "Autodock " + ligandId, List.of("auto-grid", "prepare-dpf"),
                    TaskType.AUTODOCK, 28_000L, "autodock", "grid + docking parameters + ligand", "docking result",
                    Map.of("ligand_id", ligandId)));
        }
        return new WorkflowDefinition("avianflu_small", "Avianflu_small", jobs);
    }

    private int dockingTaskCount() {
        return (int) DEFINITION.jobs().stream().filter(job -> job.taskType() == TaskType.AUTODOCK).count();
    }

    private static void copyLigandLibrarySubset(Path input, Path output, int rows) throws Exception {
        List<String> lines = Files.readAllLines(input, StandardCharsets.UTF_8);
        Files.write(output, lines.subList(0, Math.min(lines.size(), rows + 1)), StandardCharsets.UTF_8);
    }

    private static void writeLigandFile(Path output, int atomCount, Random random, int seed) throws Exception {
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("atom_id\tx\ty\tz\tcharge");
            writer.newLine();
            for (int i = 0; i < atomCount; i++) {
                double charge = ((seed + i) % 9 - 4) / 10.0;
                writer.write("a" + i + "\t" + (random.nextInt(30)) + "\t" + (random.nextInt(30))
                        + "\t" + (random.nextInt(30)) + "\t" + String.format("%.3f", charge));
                writer.newLine();
            }
        }
    }

    private static String pad(int value) {
        return String.format("%03d", value);
    }
}
