package org.bds.wsh.tests;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.bds.wsh.io.TrainingProfileCsv;
import org.bds.wsh.scheduler.ClusterTrainingProfile;

public final class TrainingProfileCsvTests {
    public void run() throws Exception {
        Path csv = Path.of("results", "bom-training.csv");
        Files.createDirectories(csv.getParent());
        String content = "\uFEFFcluster_id,container_name,cpu_training_seconds,io_training_seconds\n"
                + "C1,wsh-nodemanager-c1,1.0,2.0\n";
        Files.writeString(csv, content, StandardCharsets.UTF_8);

        Map<String, ClusterTrainingProfile> profiles = new TrainingProfileCsv().load(csv);
        TestSupport.assertTrue(profiles.containsKey("C1"), "CSV loader should handle UTF-8 BOM header.");
        TestSupport.assertClose(1.0, profiles.get("C1").cpuTrainingSeconds(), 1e-9, "CPU training seconds should parse.");
        TestSupport.assertClose(2.0, profiles.get("C1").ioTrainingSeconds(), 1e-9, "IO training seconds should parse.");
    }
}
