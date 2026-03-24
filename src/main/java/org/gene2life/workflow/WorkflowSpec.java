package org.gene2life.workflow;

import org.gene2life.cli.CliArguments;
import org.gene2life.model.JobDefinition;
import org.gene2life.model.JobRun;
import org.gene2life.model.WorkflowDefinition;
import org.gene2life.task.TaskExecutor;
import org.gene2life.task.TaskInputs;
import org.gene2life.task.WorkflowTaskExecutors;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Future;

public interface WorkflowSpec {
    WorkflowDefinition definition();

    void generateData(Path dataRoot, CliArguments cli) throws Exception;

    TaskInputs resolveInputs(String jobId, Path dataRoot, Path runRoot, Map<String, Future<JobRun>> futures) throws Exception;

    TaskInputs resolveTrainingInputs(String jobId, Path dataRoot) throws Exception;

    default Map<String, TaskExecutor> executors() {
        Map<String, TaskExecutor> executors = new LinkedHashMap<>();
        for (JobDefinition job : definition().jobs()) {
            executors.put(job.id(), WorkflowTaskExecutors.executor(job.taskType()));
        }
        return Map.copyOf(executors);
    }

    default String workflowId() {
        return definition().workflowId();
    }

    default String displayName() {
        return definition().displayName();
    }

    default Path outputPath(String jobId, Path outputDirectory) {
        return outputDirectory.resolve(definition().job(jobId).taskType().outputFileName());
    }

    default String outputDescription(String jobId) {
        return definition().job(jobId).taskType().outputDescription();
    }

    default Path trainingOutputPath(Path dataRoot, String jobId) {
        return dataRoot.resolve("training/generated")
                .resolve(jobId)
                .resolve(definition().job(jobId).taskType().outputFileName());
    }

    default void ensureTrainingParents(Path dataRoot) throws IOException {
        Files.createDirectories(dataRoot.resolve("training/generated"));
        for (JobDefinition job : definition().trainingRepresentativeJobs()) {
            Files.createDirectories(dataRoot.resolve("training/generated").resolve(job.id()));
        }
    }
}
