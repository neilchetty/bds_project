package org.gene2life.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.stream.Stream;

public final class HadoopSupport {
    private final HadoopExecutionConfig executionConfig;
    private final Configuration configuration;

    public HadoopSupport(HadoopExecutionConfig executionConfig) {
        this.executionConfig = executionConfig;
        this.configuration = buildConfiguration(executionConfig);
    }

    public Configuration configuration() {
        return new Configuration(configuration);
    }

    public String normalizedDataRoot() {
        return executionConfig.normalizedDataRoot();
    }

    public String normalizedWorkspaceRoot() {
        return executionConfig.normalizedWorkspaceRoot();
    }

    public boolean enableNodeLabels() {
        return executionConfig.enableNodeLabels();
    }

    public void syncLocalDirectoryToHdfs(Path localRoot, String hdfsRoot) throws Exception {
        String metadata = localGenerationMetadata(localRoot);
        if (metadata != null) {
            String remoteMetadata = readHdfsTextOrNull(hdfsRoot + "/.generation-metadata.env");
            if (metadata.equals(remoteMetadata)) {
                return;
            }
        }
        deleteIfExists(hdfsRoot, true);
        mkdirs(hdfsRoot);
        try (Stream<Path> stream = Files.walk(localRoot)) {
            for (Path path : stream.sorted().toList()) {
                if (path.equals(localRoot)) {
                    continue;
                }
                Path relative = localRoot.relativize(path);
                String destination = normalize(hdfsRoot + "/" + toUnixPath(relative));
                if (Files.isDirectory(path)) {
                    mkdirs(destination);
                } else {
                    copyLocalFileToHdfs(path, destination);
                }
            }
        }
    }

    public void copyLocalFileToHdfs(Path localPath, String hdfsPath) throws Exception {
        try (FileSystem fileSystem = FileSystem.get(configuration())) {
            org.apache.hadoop.fs.Path target = new org.apache.hadoop.fs.Path(normalize(hdfsPath));
            org.apache.hadoop.fs.Path parent = target.getParent();
            if (parent != null) {
                fileSystem.mkdirs(parent);
            }
            fileSystem.copyFromLocalFile(false, true, new org.apache.hadoop.fs.Path(localPath.toAbsolutePath().toString()), target);
        }
    }

    public void copyHdfsFileToLocal(String hdfsPath, Path localPath) throws Exception {
        Files.createDirectories(localPath.getParent());
        try (FileSystem fileSystem = FileSystem.get(configuration())) {
            fileSystem.copyToLocalFile(false, new org.apache.hadoop.fs.Path(normalize(hdfsPath)),
                    new org.apache.hadoop.fs.Path(localPath.toAbsolutePath().toString()), true);
        }
    }

    public void deleteIfExists(String hdfsPath, boolean recursive) throws Exception {
        try (FileSystem fileSystem = FileSystem.get(configuration())) {
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(normalize(hdfsPath));
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, recursive);
            }
        }
    }

    public void mkdirs(String hdfsPath) throws Exception {
        try (FileSystem fileSystem = FileSystem.get(configuration())) {
            fileSystem.mkdirs(new org.apache.hadoop.fs.Path(normalize(hdfsPath)));
        }
    }

    public void writeUtf8(String hdfsPath, String value) throws Exception {
        try (FileSystem fileSystem = FileSystem.get(configuration())) {
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(normalize(hdfsPath));
            org.apache.hadoop.fs.Path parent = path.getParent();
            if (parent != null) {
                fileSystem.mkdirs(parent);
            }
            try (var outputStream = fileSystem.create(path, true)) {
                outputStream.write(value.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    public boolean exists(String hdfsPath) throws Exception {
        try (FileSystem fileSystem = FileSystem.get(configuration())) {
            return fileSystem.exists(new org.apache.hadoop.fs.Path(normalize(hdfsPath)));
        }
    }

    public String readHdfsTextOrNull(String hdfsPath) throws Exception {
        try (FileSystem fileSystem = FileSystem.get(configuration())) {
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(normalize(hdfsPath));
            if (!fileSystem.exists(path)) {
                return null;
            }
            try (InputStream inputStream = fileSystem.open(path)) {
                return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
    }

    public void deleteChildrenIfExists(String hdfsPath) throws Exception {
        try (FileSystem fileSystem = FileSystem.get(configuration())) {
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(normalize(hdfsPath));
            if (!fileSystem.exists(path)) {
                return;
            }
            for (FileStatus status : fileSystem.listStatus(path)) {
                fileSystem.delete(status.getPath(), true);
            }
        }
    }

    public String normalize(String value) {
        return normalizePath(value);
    }

    private static Configuration buildConfiguration(HadoopExecutionConfig executionConfig) {
        Configuration configuration = new Configuration();
        if (executionConfig.hadoopConfDir() != null && !executionConfig.hadoopConfDir().isBlank()) {
            Path confDir = Path.of(executionConfig.hadoopConfDir());
            addIfPresent(configuration, confDir.resolve("core-site.xml"));
            addIfPresent(configuration, confDir.resolve("hdfs-site.xml"));
            addIfPresent(configuration, confDir.resolve("mapred-site.xml"));
            addIfPresent(configuration, confDir.resolve("yarn-site.xml"));
        }
        if (executionConfig.fsDefaultFs() != null && !executionConfig.fsDefaultFs().isBlank()) {
            configuration.set("fs.defaultFS", executionConfig.fsDefaultFs());
        }
        configuration.setIfUnset("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.setIfUnset("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        configuration.setIfUnset("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");
        if (executionConfig.frameworkName() != null && !executionConfig.frameworkName().isBlank()) {
            configuration.set("mapreduce.framework.name", executionConfig.frameworkName());
        }
        if (executionConfig.yarnResourceManagerAddress() != null && !executionConfig.yarnResourceManagerAddress().isBlank()) {
            configuration.set("yarn.resourcemanager.address", executionConfig.yarnResourceManagerAddress());
        }
        return configuration;
    }

    private static void addIfPresent(Configuration configuration, Path path) {
        if (Files.exists(path)) {
            configuration.addResource(new org.apache.hadoop.fs.Path(path.toAbsolutePath().toString()));
        }
    }

    private static String localGenerationMetadata(Path localRoot) throws IOException {
        Path marker = localRoot.resolve(".generation-metadata.env");
        if (!Files.exists(marker)) {
            return null;
        }
        return Files.readString(marker, StandardCharsets.UTF_8);
    }

    private static String toUnixPath(Path relative) {
        return relative.toString().replace('\\', '/');
    }

    private static String normalizePath(String value) {
        Objects.requireNonNull(value, "value");
        String normalized = value.replaceAll("/{2,}", "/");
        if (!normalized.startsWith("/")) {
            normalized = "/" + normalized;
        }
        if (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }
}
