package org.gene2life.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.gene2life.model.NodeProfile;
import org.gene2life.task.TaskResult;
import org.gene2life.workflow.WorkflowSpec;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public final class HadoopJobRunner {
    private final HadoopSupport hadoopSupport;

    public HadoopJobRunner(HadoopSupport hadoopSupport) {
        this.hadoopSupport = hadoopSupport;
    }

    public void syncDataRoot(Path localDataRoot) throws Exception {
        hadoopSupport.syncLocalDirectoryToHdfs(localDataRoot, hadoopSupport.normalizedDataRoot());
    }

    public TaskResult executeWorkflowJob(
            WorkflowSpec workflowSpec,
            String jobId,
            NodeProfile nodeProfile,
            Path localRunRoot,
            String hdfsRunRoot) throws Exception {
        HadoopTaskInputs inputs = workflowSpec.resolveHadoopInputs(jobId, hadoopSupport.normalizedDataRoot(), hdfsRunRoot);
        Path localOutputDirectory = localRunRoot.resolve("jobs").resolve(jobId);
        String hdfsOutputPath = workflowSpec.hadoopOutputPath(jobId, inputs.outputDirectory());
        return submit(workflowSpec, jobId, nodeProfile, inputs, localOutputDirectory, hdfsOutputPath, hdfsRunRoot, nodeProfile.clusterId());
    }

    public TaskResult executeTrainingJob(
            WorkflowSpec workflowSpec,
            String jobId,
            NodeProfile nodeProfile,
            Path localDataRoot) throws Exception {
        HadoopTaskInputs inputs = workflowSpec.resolveHadoopTrainingInputs(jobId, hadoopSupport.normalizedDataRoot());
        Path localOutputDirectory = localDataRoot.resolve("training/generated").resolve(jobId);
        String hdfsOutputPath = workflowSpec.hadoopTrainingOutputPath(hadoopSupport.normalizedDataRoot(), jobId);
        return submit(workflowSpec, jobId, nodeProfile, inputs, localOutputDirectory, hdfsOutputPath,
                hadoopSupport.normalizedWorkspaceRoot() + "/training", nodeProfile.clusterId());
    }

    private TaskResult submit(
            WorkflowSpec workflowSpec,
            String jobId,
            NodeProfile nodeProfile,
            HadoopTaskInputs inputs,
            Path localOutputDirectory,
            String hdfsOutputPath,
            String hdfsControlRoot,
            String nodeLabelExpression) throws Exception {
        Files.createDirectories(localOutputDirectory);
        hadoopSupport.deleteIfExists(inputs.outputDirectory(), true);
        String controlPath = hdfsControlRoot + "/_control/" + jobId + "-" + UUID.randomUUID() + ".txt";
        hadoopSupport.writeUtf8(controlPath, "run\n");
        try {
            Configuration configuration = hadoopSupport.configuration();
            configuration.setInt("mapreduce.map.memory.mb", Math.max(512, nodeProfile.memoryMb()));
            configuration.setInt("mapreduce.map.cpu.vcores", Math.max(1, nodeProfile.cpuThreads()));
            if (hadoopSupport.enableNodeLabels() && nodeLabelExpression != null && !nodeLabelExpression.isBlank()) {
                configuration.set("mapreduce.job.node-label-expression", nodeLabelExpression);
                configuration.set("yarn.app.mapreduce.am.node-label-expression", nodeLabelExpression);
            }
            HadoopTaskJob.configureTask(
                    configuration,
                    workflowSpec,
                    jobId,
                    nodeProfile,
                    inputs,
                    hdfsOutputPath,
                    workflowSpec.outputDescription(jobId));
            var job = HadoopTaskJob.createJob(configuration, workflowSpec.workflowId() + "-" + jobId + "-" + nodeProfile.nodeId(), controlPath);
            if (!job.waitForCompletion(true)) {
                throw new IllegalStateException("Hadoop job failed for " + workflowSpec.workflowId() + "/" + jobId);
            }
            Path localOutputPath = workflowSpec.outputPath(jobId, localOutputDirectory);
            hadoopSupport.copyHdfsFileToLocal(hdfsOutputPath, localOutputPath);
            return new TaskResult(localOutputPath, workflowSpec.outputDescription(jobId));
        } finally {
            hadoopSupport.deleteIfExists(controlPath, false);
        }
    }
}
