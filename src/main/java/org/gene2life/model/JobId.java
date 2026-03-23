package org.gene2life.model;

public enum JobId {
    BLAST1("blast1"),
    BLAST2("blast2"),
    CLUSTALW1("clustalw1"),
    CLUSTALW2("clustalw2"),
    DNAPARS("dnapars"),
    PROTPARS("protpars"),
    DRAWGRAM1("drawgram1"),
    DRAWGRAM2("drawgram2");

    private final String cliName;

    JobId(String cliName) {
        this.cliName = cliName;
    }

    public String cliName() {
        return cliName;
    }

    public static JobId fromCliName(String value) {
        for (JobId jobId : values()) {
            if (jobId.cliName.equalsIgnoreCase(value)) {
                return jobId;
            }
        }
        throw new IllegalArgumentException("Unknown job id: " + value);
    }
}
