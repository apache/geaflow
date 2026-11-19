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
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.ai.temporal.benchmark;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.geaflow.ai.temporal.candidate.EvidenceCandidate;
import org.apache.geaflow.ai.temporal.candidate.EvidenceRetriever;
import org.apache.geaflow.ai.temporal.candidate.RetrievalProfile;
import org.apache.geaflow.ai.temporal.candidate.TextNormalizer;
import org.apache.geaflow.ai.temporal.model.Evidence;

/** Prepares the fixed 200-candidate, title-and-body cross-encoder baseline. */
public final class RetrievalBaselinePreparation {

    private static final int DEPTH = 200;
    private static final Gson JSON = new GsonBuilder().disableHtmlEscaping().create();
    private static final String[] OUTPUT_FILES = {"subset.jsonl", "corpus.jsonl", "gold.jsonl",
        "source-mappings.jsonl", "titles.jsonl", "title-input.jsonl", "candidates.csv"};

    public JsonObject prepare(ClimateFeverDataset dataset, Path output, int queryLimit)
        throws IOException {
        if (queryLimit < 1 || queryLimit > ClimateFeverDataset.SUBSET_SIZE) {
            throw new IllegalArgumentException("queryLimit must be between 1 and 1000");
        }
        emptyDirectory(output);
        final int count = Math.min(queryLimit, dataset.getClaims().size());
        final Map<String, List<Evidence>> exact = new HashMap<>();
        for (Evidence evidence : dataset.getCorpus()) {
            exact.computeIfAbsent(TextNormalizer.normalize(evidence.getContent()),
                ignored -> new ArrayList<>()).add(evidence);
        }
        long pairCount = 0;
        try (EvidenceRetriever fusion = new EvidenceRetriever(dataset.getCorpus(),
                 RetrievalProfile.LEXICAL_FUSION);
             EvidenceRetriever english = new EvidenceRetriever(dataset.getCorpus(),
                 RetrievalProfile.ENGLISH);
             BufferedWriter input = writer(output.resolve("title-input.jsonl"));
             BufferedWriter candidates = writer(output.resolve("candidates.csv"))) {
            candidates.write("query_id,pool_rank,evidence_id,rule_exact\n");
            for (ClimateFeverDataset.Claim claim : dataset.getClaims().subList(0, count)) {
                final String normalizedQuery = TextNormalizer.normalize(claim.getText());
                final List<Evidence> pool = candidatePool(
                    exact.getOrDefault(normalizedQuery, Collections.emptyList()),
                    fusion.search(claim.getText(), 100), english.search(claim.getText(), 500));
                input.write(JSON.toJson(poolInput(claim.getId(), claim.getText(), pool)));
                input.write('\n');
                for (int index = 0; index < pool.size(); index++) {
                    final Evidence evidence = pool.get(index);
                    candidates.write(claim.getId() + "," + (index + 1) + "," + evidence.getId()
                        + "," + normalizedQuery.equals(TextNormalizer.normalize(evidence.getContent())) + "\n");
                }
                pairCount += pool.size();
            }
        }
        dataset.writeArtifacts(output);
        final JsonObject manifest = dataset.getManifest();
        manifest.addProperty("schema", "retrieval-baseline-inputs-v1");
        manifest.addProperty("profile", "CE_TITLE_BODY");
        manifest.addProperty("depth", DEPTH);
        manifest.addProperty("finalK", 20);
        manifest.addProperty("queryCount", count);
        manifest.addProperty("evaluatedQueryCount", count);
        manifest.addProperty("pairCount", pairCount);
        manifest.addProperty("poolPolicy", "Freeze RULE-first LEXICAL_FUSION top 100; append original "
            + "ENGLISH BM25 top-500 ranks, deduplicate evidence IDs and cap at 200");
        manifest.addProperty("titlePolicy", "Frozen global corpus.article, newline, original body; "
            + "independent of query and Gold");
        manifest.addProperty("rulePolicy", "TextNormalizer equality on original query and original body; "
            + "never on title-prefixed text");
        manifest.addProperty("goldPolicy", "Labels and Gold links affect metrics only, never candidates or model input");
        manifest.addProperty("querySelection", "First queryCount claims of the fixed numeric-ID subset");
        final JsonObject hashes = new JsonObject();
        for (String file : OUTPUT_FILES) {
            hashes.addProperty(file, sha256(output.resolve(file)));
        }
        manifest.add("outputSha256", hashes);
        try (BufferedWriter file = writer(output.resolve("manifest.json"))) {
            file.write(JSON.toJson(manifest));
            file.write('\n');
        }
        return manifest;
    }

    static List<Evidence> candidatePool(List<Evidence> rule, List<EvidenceCandidate> fusion,
                                        List<EvidenceCandidate> english) {
        final Map<String, Evidence> original = new LinkedHashMap<>();
        for (Evidence evidence : rule) {
            original.put(evidence.getId(), evidence);
        }
        for (EvidenceCandidate candidate : fusion) {
            original.put(candidate.getEvidence().getId(), candidate.getEvidence());
        }
        final List<Evidence> first = new ArrayList<>(original.values());
        final Map<String, Evidence> result = new LinkedHashMap<>();
        for (Evidence evidence : first.subList(0, Math.min(first.size(), 100))) {
            result.put(evidence.getId(), evidence);
        }
        for (EvidenceCandidate candidate : english) {
            if (result.size() >= DEPTH) {
                break;
            }
            result.putIfAbsent(candidate.getEvidence().getId(), candidate.getEvidence());
        }
        return new ArrayList<>(result.values());
    }

    private static JsonObject poolInput(String queryId, String queryText, List<Evidence> pool) {
        final JsonObject input = new JsonObject();
        input.addProperty("schemaVersion", 1);
        input.addProperty("queryId", queryId);
        input.addProperty("queryText", queryText);
        final JsonArray candidates = new JsonArray();
        for (Evidence evidence : pool) {
            final JsonObject candidate = new JsonObject();
            candidate.addProperty("evidenceId", evidence.getId());
            candidate.addProperty("text", evidence.getSource().getName() + "\n" + evidence.getContent());
            candidates.add(candidate);
        }
        input.add("candidates", candidates);
        input.addProperty("poolSha256", ClimateFeverDataset.sha256(
            JSON.toJson(input).getBytes(StandardCharsets.UTF_8)));
        return input;
    }

    private static String sha256(Path path) throws IOException {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(path)) {
                final byte[] buffer = new byte[8192];
                int count;
                while ((count = input.read(buffer)) != -1) {
                    digest.update(buffer, 0, count);
                }
            }
            final StringBuilder result = new StringBuilder();
            for (byte value : digest.digest()) {
                result.append(Character.forDigit((value & 0xff) >>> 4, 16));
                result.append(Character.forDigit(value & 0xf, 16));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is unavailable", error);
        }
    }

    private static BufferedWriter writer(Path path) throws IOException {
        return Files.newBufferedWriter(path, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
    }

    private static void emptyDirectory(Path output) throws IOException {
        if (Files.exists(output)) {
            if (!Files.isDirectory(output)) {
                throw new IllegalArgumentException("Output must be an empty directory");
            }
            try (Stream<Path> files = Files.list(output)) {
                if (files.findAny().isPresent()) {
                    throw new IllegalArgumentException("Output directory must be empty");
                }
            }
        }
        Files.createDirectories(output);
    }
}
