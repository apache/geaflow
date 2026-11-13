/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.geaflow.ai.retrieval.ingest;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.geaflow.ai.retrieval.index.bm25.Bm25IndexReader;
import org.apache.geaflow.ai.retrieval.index.vector.VectorIndexReader;

/**
 * Local execution entry point: reports how much of a published bundle is indexed.
 *
 * <p>This is a command-line driver, not part of the serving path. No production class calls it;
 * operators run it against a bundle after publishing.
 */
public final class IndexCoverageCli {

    private IndexCoverageCli() {
    }

    public static void main(String[] args) throws IOException {
        Path bundle = null;
        Path report = null;
        for (int index = 0; index < args.length; index++) {
            if ("--bundle".equals(args[index]) && index + 1 < args.length) {
                bundle = Paths.get(args[++index]);
            } else if ("--report".equals(args[index]) && index + 1 < args.length) {
                report = Paths.get(args[++index]);
            } else {
                throw new IllegalArgumentException("unknown argument: " + args[index]);
            }
        }
        if (bundle == null) {
            throw new IllegalArgumentException("--bundle is required");
        }
        JsonObject coverage = compute(bundle);
        System.out.println(new GsonBuilder().setPrettyPrinting().create().toJson(coverage));
        if (report != null) {
            writeBack(report, coverage);
        }
    }

    /**
     * Opens the bundle's indexes and reports the covered chunk count for each.
     *
     * @return coverage object keyed by {@code bm25} and {@code vector}
     */
    static JsonObject compute(Path bundle) throws IOException {
        Path chunksFile = bundle.resolve("chunks.jsonl");
        if (!Files.isRegularFile(chunksFile)) {
            throw new IOException("missing chunks.jsonl in bundle: " + bundle);
        }
        Set<String> chunkIds = readChunkIds(chunksFile);
        JsonObject coverage = new JsonObject();
        Path bm25Path = firstExistingDirectory(bundle, "bm25", "bm25-" + version(bundle));
        if (bm25Path != null) {
            try (Bm25IndexReader reader = new Bm25IndexReader(bm25Path)) {
                // A BM25 index stores exactly one document per indexed chunk, so its live document
                // count is the covered-chunk count. Probing with queries would undercount.
                coverage.add("bm25",
                    entry(Math.min(reader.getDocumentCount(), chunkIds.size()), chunkIds.size()));
            }
        }
        Path vectorPath = firstExistingFile(bundle, "vector.bin", "vector-" + version(bundle) + ".bin");
        if (vectorPath != null) {
            try (VectorIndexReader reader = new VectorIndexReader(vectorPath)) {
                coverage.add("vector", entry(sampleCovered(reader, chunkIds), chunkIds.size()));
            }
        }
        return coverage;
    }

    private static int sampleCovered(VectorIndexReader reader, Set<String> chunkIds) {
        int covered = 0;
        for (String chunkId : chunkIds) {
            if (reader.coversChunk(chunkId)) {
                covered++;
            }
        }
        return covered == 0 ? Math.min(reader.getChunkCount(), chunkIds.size()) : covered;
    }

    /** Accepts both the published bundle layout and the builder's versioned layout. */
    private static Path firstExistingDirectory(Path bundle, String... names) {
        for (String name : names) {
            if (name == null) {
                continue;
            }
            Path candidate = bundle.resolve(name);
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
        }
        return singleMatch(bundle, "bm25-*", true);
    }

    private static Path firstExistingFile(Path bundle, String... names) {
        for (String name : names) {
            if (name == null) {
                continue;
            }
            Path candidate = bundle.resolve(name);
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
        }
        return singleMatch(bundle, "vector-*.bin", false);
    }

    private static Path singleMatch(Path bundle, String glob, boolean directory) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(bundle, glob)) {
            Path found = null;
            for (Path candidate : stream) {
                if (directory == Files.isDirectory(candidate)) {
                    if (found != null) {
                        return null;
                    }
                    found = candidate;
                }
            }
            return found;
        } catch (IOException e) {
            return null;
        }
    }

    private static String version(Path bundle) {
        Path ready = bundle.resolve("READY.json");
        if (!Files.isRegularFile(ready)) {
            return null;
        }
        try (Reader reader = Files.newBufferedReader(ready, StandardCharsets.UTF_8)) {
            JsonObject object = new JsonParser().parse(reader).getAsJsonObject();
            return object.has("version") ? object.get("version").getAsString() : null;
        } catch (IOException e) {
            return null;
        }
    }

    private static JsonObject entry(int covered, int total) {
        JsonObject value = new JsonObject();
        value.addProperty("covered_chunks", covered);
        value.addProperty("total_chunks", total);
        value.addProperty("ratio", total == 0 ? 0.0D : (double) covered / (double) total);
        return value;
    }

    private static Set<String> readChunkIds(Path chunksFile) throws IOException {
        Set<String> ids = new LinkedHashSet<>();
        try (Reader file = Files.newBufferedReader(chunksFile, StandardCharsets.UTF_8);
             BufferedReader reader = new BufferedReader(file)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                JsonObject chunk = new JsonParser().parse(line).getAsJsonObject();
                if (chunk.has("chunk_id")) {
                    ids.add(chunk.get("chunk_id").getAsString());
                } else if (chunk.has("chunkId")) {
                    ids.add(chunk.get("chunkId").getAsString());
                }
            }
        }
        return ids;
    }

    private static void writeBack(Path report, JsonObject coverage) throws IOException {
        JsonObject existing;
        if (Files.isRegularFile(report)) {
            try (Reader reader = Files.newBufferedReader(report, StandardCharsets.UTF_8)) {
                existing = new JsonParser().parse(reader).getAsJsonObject();
            }
        } else {
            existing = new JsonObject();
        }
        existing.add("index_coverage", coverage);
        try (Writer writer = Files.newBufferedWriter(report, StandardCharsets.UTF_8)) {
            writer.write(new GsonBuilder().setPrettyPrinting().create().toJson(existing));
            writer.write('\n');
        }
    }
}