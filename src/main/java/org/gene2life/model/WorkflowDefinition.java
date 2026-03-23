package org.gene2life.model;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public final class WorkflowDefinition {
    private final Map<JobId, JobDefinition> jobs;

    private WorkflowDefinition(Map<JobId, JobDefinition> jobs) {
        this.jobs = jobs;
    }

    public static WorkflowDefinition gene2life() {
        Map<JobId, JobDefinition> jobs = new EnumMap<>(JobId.class);
        jobs.put(JobId.BLAST1, new JobDefinition(
                JobId.BLAST1, "Blast 1", List.of(), "source DNA sequence", "0.1 MB blast hits"));
        jobs.put(JobId.BLAST2, new JobDefinition(
                JobId.BLAST2, "Blast 2", List.of(), "source DNA sequence", "0.1 MB blast hits"));
        jobs.put(JobId.CLUSTALW1, new JobDefinition(
                JobId.CLUSTALW1, "ClustalW 1", List.of(JobId.BLAST1), "blast1 hits", "0.1 MB alignment"));
        jobs.put(JobId.CLUSTALW2, new JobDefinition(
                JobId.CLUSTALW2, "ClustalW 2", List.of(JobId.BLAST2), "blast2 hits", "0.1 MB alignment"));
        jobs.put(JobId.DNAPARS, new JobDefinition(
                JobId.DNAPARS, "Dnapars", List.of(JobId.CLUSTALW1), "clustalw1 alignment", "4 KB DNA tree"));
        jobs.put(JobId.PROTPARS, new JobDefinition(
                JobId.PROTPARS, "Protpars", List.of(JobId.CLUSTALW2), "clustalw2 alignment", "4 KB protein tree"));
        jobs.put(JobId.DRAWGRAM1, new JobDefinition(
                JobId.DRAWGRAM1, "Drawgram 1", List.of(JobId.DNAPARS), "dnapars tree", "35 KB tree files"));
        jobs.put(JobId.DRAWGRAM2, new JobDefinition(
                JobId.DRAWGRAM2, "Drawgram 2", List.of(JobId.PROTPARS), "protpars tree", "35 KB tree files"));
        return new WorkflowDefinition(jobs);
    }

    public List<JobDefinition> jobs() {
        List<JobDefinition> values = new ArrayList<>(jobs.values());
        values.sort(Comparator.comparing(def -> def.id().ordinal()));
        return values;
    }

    public JobDefinition job(JobId jobId) {
        return jobs.get(jobId);
    }

    public List<JobDefinition> successors(JobId jobId) {
        return jobs().stream()
                .filter(def -> def.dependencies().contains(jobId))
                .toList();
    }
}
