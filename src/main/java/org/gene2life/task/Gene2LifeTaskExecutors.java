package org.gene2life.task;

import org.gene2life.model.JobId;
import org.gene2life.model.NodeProfile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

public final class Gene2LifeTaskExecutors {
    private static final Map<String, Character> CODON_TABLE = buildCodonTable();

    private Gene2LifeTaskExecutors() {
    }

    public static Map<JobId, TaskExecutor> executors() {
        return Map.of(
                JobId.BLAST1, Gene2LifeTaskExecutors::runBlast,
                JobId.BLAST2, Gene2LifeTaskExecutors::runBlast,
                JobId.CLUSTALW1, Gene2LifeTaskExecutors::runClustal,
                JobId.CLUSTALW2, Gene2LifeTaskExecutors::runClustal,
                JobId.DNAPARS, Gene2LifeTaskExecutors::runDnaPars,
                JobId.PROTPARS, Gene2LifeTaskExecutors::runProtPars,
                JobId.DRAWGRAM1, Gene2LifeTaskExecutors::runDrawgram,
                JobId.DRAWGRAM2, Gene2LifeTaskExecutors::runDrawgram);
    }

    private static TaskResult runBlast(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Files.createDirectories(inputs.outputDirectory());
        Path output = inputs.outputDirectory().resolve("hits.tsv");
        Map<String, QueryProfile> queryProfiles = loadQueries(inputs.primaryInput(), nodeProfile);
        Map<String, PriorityQueue<Hit>> hitsByQuery = new LinkedHashMap<>();
        for (String queryId : queryProfiles.keySet()) {
            hitsByQuery.put(queryId, new PriorityQueue<>(Comparator.comparingDouble(Hit::score)));
        }
        int chunkSize = Math.max(64, Math.min(4096, (nodeProfile.cpuThreads() * 96) + (nodeProfile.memoryMb() / 64)));
        List<FastaRecord> chunk = new ArrayList<>(chunkSize);
        try (BufferedReader reader = bufferedReader(inputs.secondaryInput(), nodeProfile)) {
            FastaRecord record;
            while ((record = nextRecord(reader)) != null) {
                chunk.add(record);
                if (chunk.size() >= chunkSize) {
                    processBlastChunk(chunk, queryProfiles, hitsByQuery, nodeProfile.cpuThreads());
                    chunk.clear();
                }
            }
        }
        if (!chunk.isEmpty()) {
            processBlastChunk(chunk, queryProfiles, hitsByQuery, nodeProfile.cpuThreads());
        }
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("query_id\tref_id\tscore\torganism\tfunction\tsequence");
            writer.newLine();
            for (Map.Entry<String, PriorityQueue<Hit>> entry : hitsByQuery.entrySet()) {
                List<Hit> ordered = new ArrayList<>(entry.getValue());
                ordered.sort(Comparator.comparingDouble(Hit::score).reversed());
                for (Hit hit : ordered) {
                    writer.write(hit.queryId + "\t" + hit.refId + "\t" + String.format("%.5f", hit.score)
                            + "\t" + hit.organism + "\t" + hit.function + "\t" + hit.sequence);
                    writer.newLine();
                }
            }
        }
        return new TaskResult(output, "blast hits");
    }

    private static void processBlastChunk(
            List<FastaRecord> chunk,
            Map<String, QueryProfile> queryProfiles,
            Map<String, PriorityQueue<Hit>> hitsByQuery,
            int parallelism) throws ExecutionException, InterruptedException {
        ForkJoinPool pool = new ForkJoinPool(Math.max(1, parallelism));
        try {
            List<Hit> hits = pool.submit(() -> chunk.parallelStream()
                    .map(record -> bestHit(record, queryProfiles))
                    .filter(hit -> hit != null)
                    .toList()).get();
            for (Hit hit : hits) {
                PriorityQueue<Hit> queue = hitsByQuery.get(hit.queryId);
                queue.offer(hit);
                while (queue.size() > 24) {
                    queue.poll();
                }
            }
        } finally {
            pool.shutdown();
        }
    }

    private static Hit bestHit(FastaRecord record, Map<String, QueryProfile> queryProfiles) {
        String motif = record.metadata.getOrDefault("motif", "");
        Hit best = null;
        for (QueryProfile profile : queryProfiles.values()) {
            if (!profile.motif.equals(motif) && !profile.signature.startsWith(signaturePrefix(record.sequence(), 2))) {
                continue;
            }
            double score = jaccard(profile.signature, signature(record.sequence()));
            if (best == null || score > best.score) {
                best = new Hit(profile.queryId, record.id(), score,
                        record.metadata.getOrDefault("organism", "unknown"),
                        record.metadata.getOrDefault("function", "unknown"),
                        record.sequence());
            }
        }
        return best;
    }

    private static TaskResult runClustal(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Files.createDirectories(inputs.outputDirectory());
        Path output = inputs.outputDirectory().resolve("alignment.tsv");
        Map<String, List<Hit>> hits = readHits(inputs.primaryInput(), nodeProfile);
        ForkJoinPool pool = new ForkJoinPool(Math.max(1, nodeProfile.cpuThreads()));
        try {
            List<String> rows = pool.submit(() -> hits.entrySet().parallelStream()
                    .map(entry -> {
                        String consensus = consensus(entry.getValue().stream().map(Hit::sequence).toList());
                        double avgScore = entry.getValue().stream().mapToDouble(Hit::score).average().orElse(0.0);
                        String organism = entry.getValue().stream().collect(Collectors.groupingBy(Hit::organism, Collectors.counting()))
                                .entrySet().stream().max(Map.Entry.comparingByValue()).map(Map.Entry::getKey).orElse("unknown");
                        String function = entry.getValue().stream().collect(Collectors.groupingBy(Hit::function, Collectors.counting()))
                                .entrySet().stream().max(Map.Entry.comparingByValue()).map(Map.Entry::getKey).orElse("unknown");
                        return entry.getKey() + "\t" + entry.getValue().size() + "\t"
                                + String.format("%.5f", avgScore) + "\t" + organism + "\t" + function + "\t" + consensus;
                    })
                    .sorted()
                    .toList()).get();
            try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
                writer.write("query_id\thit_count\tavg_score\torganism\tfunction\tconsensus_dna");
                writer.newLine();
                for (String row : rows) {
                    writer.write(row);
                    writer.newLine();
                }
            }
        } finally {
            pool.shutdown();
        }
        return new TaskResult(output, "consensus DNA alignments");
    }

    private static TaskResult runDnaPars(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Files.createDirectories(inputs.outputDirectory());
        Path output = inputs.outputDirectory().resolve("dna-tree.newick");
        List<SequenceNode> sequences = readAlignmentSequences(inputs.primaryInput(), nodeProfile, false);
        Files.writeString(output, buildTree(sequences), StandardCharsets.UTF_8);
        return new TaskResult(output, "DNA phylogenetic tree");
    }

    private static TaskResult runProtPars(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Files.createDirectories(inputs.outputDirectory());
        Path output = inputs.outputDirectory().resolve("protein-tree.newick");
        List<SequenceNode> sequences = readAlignmentSequences(inputs.primaryInput(), nodeProfile, true);
        Files.writeString(output, buildTree(sequences), StandardCharsets.UTF_8);
        return new TaskResult(output, "protein phylogenetic tree");
    }

    private static TaskResult runDrawgram(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Files.createDirectories(inputs.outputDirectory());
        Path dot = inputs.outputDirectory().resolve("tree.dot");
        Path text = inputs.outputDirectory().resolve("tree.txt");
        String newick = Files.readString(inputs.primaryInput(), StandardCharsets.UTF_8).trim();
        String dotGraph = "digraph tree {\n  rankdir=LR;\n  root [label=\"" + newick.replace("\"", "\\\"") + "\"];\n}\n";
        Files.writeString(dot, dotGraph, StandardCharsets.UTF_8);
        Files.writeString(text, "TREE\n====\n" + newick + System.lineSeparator(), StandardCharsets.UTF_8);
        return new TaskResult(text, "drawgram text tree");
    }

    private static Map<String, QueryProfile> loadQueries(Path queryFasta, NodeProfile nodeProfile) throws IOException {
        Map<String, QueryProfile> queries = new LinkedHashMap<>();
        try (BufferedReader reader = bufferedReader(queryFasta, nodeProfile)) {
            FastaRecord record;
            while ((record = nextRecord(reader)) != null) {
                queries.put(record.id(), new QueryProfile(
                        record.id(),
                        record.metadata.getOrDefault("motif", ""),
                        signature(record.sequence())));
            }
        }
        return queries;
    }

    private static Map<String, List<Hit>> readHits(Path path, NodeProfile nodeProfile) throws IOException {
        Map<String, List<Hit>> hits = new LinkedHashMap<>();
        try (BufferedReader reader = bufferedReader(path, nodeProfile)) {
            String line = reader.readLine();
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                Hit hit = new Hit(parts[0], parts[1], Double.parseDouble(parts[2]), parts[3], parts[4], parts[5]);
                hits.computeIfAbsent(hit.queryId, ignored -> new ArrayList<>()).add(hit);
            }
        }
        return hits;
    }

    private static List<SequenceNode> readAlignmentSequences(Path path, NodeProfile nodeProfile, boolean translate) throws IOException {
        List<SequenceNode> nodes = new ArrayList<>();
        try (BufferedReader reader = bufferedReader(path, nodeProfile)) {
            String line = reader.readLine();
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                String sequence = translate ? translate(parts[5]) : parts[5];
                nodes.add(new SequenceNode(parts[0], sequence));
            }
        }
        return nodes;
    }

    private static String buildTree(List<SequenceNode> nodes) {
        List<String> labels = nodes.stream()
                .sorted(Comparator.comparing(SequenceNode::name))
                .map(node -> node.name + ":" + String.format("%.4f", Math.max(0.1, 1.0 - gcRatio(node.sequence))))
                .toList();
        return "(" + String.join(",", labels) + ");";
    }

    private static String consensus(List<String> sequences) {
        if (sequences.isEmpty()) {
            return "";
        }
        int maxLength = sequences.stream().mapToInt(String::length).max().orElse(0);
        StringBuilder builder = new StringBuilder(maxLength);
        for (int i = 0; i < maxLength; i++) {
            int[] counts = new int[4];
            for (String sequence : sequences) {
                char base = i < sequence.length() ? sequence.charAt(i) : 'A';
                counts[baseIndex(base)]++;
            }
            builder.append("ACGT".charAt(maxIndex(counts)));
        }
        return builder.toString();
    }

    private static String translate(String dna) {
        StringBuilder protein = new StringBuilder();
        for (int i = 0; i + 2 < dna.length(); i += 3) {
            protein.append(CODON_TABLE.getOrDefault(dna.substring(i, i + 3), 'X'));
        }
        return protein.toString();
    }

    private static double gcRatio(String sequence) {
        long gc = sequence.chars().filter(ch -> ch == 'G' || ch == 'C').count();
        return sequence.isEmpty() ? 0.0 : (double) gc / sequence.length();
    }

    private static int baseIndex(char base) {
        return switch (base) {
            case 'C' -> 1;
            case 'G' -> 2;
            case 'T' -> 3;
            default -> 0;
        };
    }

    private static int maxIndex(int[] counts) {
        int index = 0;
        for (int i = 1; i < counts.length; i++) {
            if (counts[i] > counts[index]) {
                index = i;
            }
        }
        return index;
    }

    private static String signature(String sequence) {
        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i + 3 < sequence.length(); i += 2) {
            String kmer = sequence.substring(i, i + 4);
            counts.merge(kmer, 1, Integer::sum);
        }
        return counts.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed().thenComparing(Map.Entry::getKey))
                .limit(8)
                .map(Map.Entry::getKey)
                .collect(Collectors.joining("|"));
    }

    private static String signaturePrefix(String sequence, int length) {
        String signature = signature(sequence);
        return signature.length() <= length ? signature : signature.substring(0, length);
    }

    private static double jaccard(String left, String right) {
        List<String> a = List.of(left.split("\\|"));
        List<String> b = List.of(right.split("\\|"));
        long intersection = a.stream().filter(b::contains).count();
        long union = a.size() + b.size() - intersection;
        return union == 0 ? 0.0 : (double) intersection / union;
    }

    private static FastaRecord nextRecord(BufferedReader reader) throws IOException {
        String header = reader.readLine();
        if (header == null) {
            return null;
        }
        String sequence = reader.readLine();
        if (sequence == null) {
            throw new IOException("Invalid FASTA-like input; missing sequence line after header: " + header);
        }
        if (!header.startsWith(">")) {
            throw new IOException("Invalid FASTA-like input; expected '>' header but got: " + header);
        }
        String[] tokens = header.substring(1).split(" ");
        Map<String, String> metadata = new HashMap<>();
        for (int i = 1; i < tokens.length; i++) {
            String token = tokens[i];
            int separator = token.indexOf('=');
            if (separator > 0) {
                metadata.put(token.substring(0, separator), token.substring(separator + 1));
            }
        }
        return new FastaRecord(tokens[0], sequence, metadata);
    }

    private static BufferedReader bufferedReader(Path path, NodeProfile nodeProfile) throws IOException {
        return new BufferedReader(
                new InputStreamReader(Files.newInputStream(path), StandardCharsets.UTF_8),
                nodeProfile.effectiveReadBufferBytes());
    }

    private static Map<String, Character> buildCodonTable() {
        Map<String, Character> table = new HashMap<>();
        table.put("TTT", 'F');
        table.put("TTC", 'F');
        table.put("TTA", 'L');
        table.put("TTG", 'L');
        table.put("CTT", 'L');
        table.put("CTC", 'L');
        table.put("CTA", 'L');
        table.put("CTG", 'L');
        table.put("ATT", 'I');
        table.put("ATC", 'I');
        table.put("ATA", 'I');
        table.put("ATG", 'M');
        table.put("GTT", 'V');
        table.put("GTC", 'V');
        table.put("GTA", 'V');
        table.put("GTG", 'V');
        table.put("TCT", 'S');
        table.put("TCC", 'S');
        table.put("TCA", 'S');
        table.put("TCG", 'S');
        table.put("CCT", 'P');
        table.put("CCC", 'P');
        table.put("CCA", 'P');
        table.put("CCG", 'P');
        table.put("ACT", 'T');
        table.put("ACC", 'T');
        table.put("ACA", 'T');
        table.put("ACG", 'T');
        table.put("GCT", 'A');
        table.put("GCC", 'A');
        table.put("GCA", 'A');
        table.put("GCG", 'A');
        table.put("TAT", 'Y');
        table.put("TAC", 'Y');
        table.put("CAT", 'H');
        table.put("CAC", 'H');
        table.put("CAA", 'Q');
        table.put("CAG", 'Q');
        table.put("AAT", 'N');
        table.put("AAC", 'N');
        table.put("AAA", 'K');
        table.put("AAG", 'K');
        table.put("GAT", 'D');
        table.put("GAC", 'D');
        table.put("GAA", 'E');
        table.put("GAG", 'E');
        table.put("TGT", 'C');
        table.put("TGC", 'C');
        table.put("TGG", 'W');
        table.put("CGT", 'R');
        table.put("CGC", 'R');
        table.put("CGA", 'R');
        table.put("CGG", 'R');
        table.put("AGT", 'S');
        table.put("AGC", 'S');
        table.put("AGA", 'R');
        table.put("AGG", 'R');
        table.put("GGT", 'G');
        table.put("GGC", 'G');
        table.put("GGA", 'G');
        table.put("GGG", 'G');
        return table;
    }

    private record FastaRecord(String id, String sequence, Map<String, String> metadata) {
    }

    private record QueryProfile(String queryId, String motif, String signature) {
    }

    private record Hit(String queryId, String refId, double score, String organism, String function, String sequence) {
    }

    private record SequenceNode(String name, String sequence) {
    }
}
