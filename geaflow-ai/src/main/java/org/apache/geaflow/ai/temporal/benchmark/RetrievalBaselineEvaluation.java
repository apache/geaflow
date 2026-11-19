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
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.apache.geaflow.ai.temporal.candidate.TextNormalizer;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;

/** Evaluates the fixed title/body baseline, retaining only one scored pool at a time. */
public final class RetrievalBaselineEvaluation {

    private static final String PROFILE = "CE_TITLE_BODY";
    private static final int DEPTH = 200;
    private static final int FINAL_K = 20;
    private static final Gson JSON = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();
    private static final TemporalJsonCodec CODEC = new TemporalJsonCodec();
    private static final String[] INPUT_FILES = {"subset.jsonl", "corpus.jsonl", "gold.jsonl",
        "source-mappings.jsonl", "titles.jsonl", "title-input.jsonl", "candidates.csv"};
    private static final String[] DATA_HASH_FIELDS = {"subsetSha256", "corpusSha256", "goldSha256",
        "mappingsSha256", "titlesSha256"};

    public JsonObject evaluate(Path preparedDirectory, Path scoresPath, Path metadataPath, Path output)
        throws IOException {
        final Path prepared = preparedDirectory.toRealPath();
        final Path scores = scoresPath.toRealPath();
        final Path metadataFile = metadataPath.toRealPath();
        final Path destination = output.toAbsolutePath().normalize();
        final String preparedHash = sha256(prepared.resolve("manifest.json"));
        final Prepared data = loadPrepared(prepared);
        final String metadataHash = sha256(metadataFile);
        final JsonObject metadata = readJson(metadataFile);
        validateMetadata(metadata, data.manifest);
        require(sha256(scores).equals(text(metadata, "scoresSha256")), "Scores SHA-256 mismatch");
        emptyDirectory(destination);
        final Metrics totals = new Metrics();
        long elapsedSum = 0;
        try (BufferedReader inputRows = reader(prepared.resolve("title-input.jsonl"));
             BufferedReader scoreRows = reader(scores);
             BufferedReader annotations = reader(prepared.resolve("candidates.csv"));
             BufferedWriter queryFile = writer(destination.resolve("query-results.csv"));
             BufferedWriter candidateFile = writer(destination.resolve("final-candidates.csv"))) {
            require("query_id,pool_rank,evidence_id,rule_exact".equals(annotations.readLine()),
                "Unexpected candidate CSV columns");
            csv(queryFile, "profile", "query_id", "claim_label", "gold_count", "actual_pool_count",
                "pool_hits", "pool_recall", "final_hits", "recall");
            csv(candidateFile, "profile", "query_id", "rank", "evidence_id", "cross_encoder_logit",
                "source_pool_rank", "rule_exact", "is_gold");
            for (Claim claim : data.claims) {
                final JsonObject query = nextJson(inputRows, "input query");
                validateQuery(query);
                require(text(query, "queryId").equals(claim.id)
                    && text(query, "queryText").equals(claim.text), "Input query order/text mismatch");
                final JsonArray candidates = array(query, "candidates");
                final boolean[] rules = validateCandidates(query, data.corpus, claim, annotations);
                final ScoredPool scored = scoreRow(nextJson(scoreRows, "score query"), query);
                elapsedSum = Math.addExact(elapsedSum, scored.elapsedNanos);
                final List<Integer> order = rankCandidates(candidates, scored.logits, rules);
                int poolHits = 0;
                int hits = 0;
                for (JsonElement candidate : candidates) {
                    if (claim.gold.contains(text(object(candidate, "candidate"), "evidenceId"))) {
                        poolHits++;
                    }
                }
                for (int index : order.subList(0, Math.min(FINAL_K, order.size()))) {
                    if (claim.gold.contains(text(candidates.get(index).getAsJsonObject(), "evidenceId"))) {
                        hits++;
                    }
                }
                totals.add(claim.gold.size(), hits, poolHits, candidates.size());
                csv(queryFile, PROFILE, claim.id, claim.label, claim.gold.size(), candidates.size(),
                    poolHits, fraction(poolHits, claim.gold.size()), hits, fraction(hits, claim.gold.size()));
                for (int rank = 0; rank < Math.min(FINAL_K, order.size()); rank++) {
                    final int index = order.get(rank);
                    final String evidenceId = text(candidates.get(index).getAsJsonObject(), "evidenceId");
                    csv(candidateFile, PROFILE, claim.id, rank + 1, evidenceId, scored.logits[index],
                        index + 1, rules[index], claim.gold.contains(evidenceId));
                }
            }
            exhausted(inputRows, "input query");
            exhausted(scoreRows, "score query");
            exhausted(annotations, "candidate annotation");
        }
        require(totals.candidatePairCount == integer(data.manifest, "pairCount", 0)
            && elapsedSum == integer(metadata, "inferenceNanos", 0),
            "Score pair/timing totals differ from metadata");
        verifyFiles(prepared, object(data.manifest.get("outputSha256"), "outputSha256"));
        require(sha256(prepared.resolve("manifest.json")).equals(preparedHash)
            && sha256(metadataFile).equals(metadataHash)
            && sha256(scores).equals(text(metadata, "scoresSha256")),
            "Input artifacts changed during evaluation");
        final JsonObject comparison = totals.summary();
        try (BufferedWriter file = writer(destination.resolve("comparison.csv"))) {
            final List<Object> names = new ArrayList<>();
            final List<Object> values = new ArrayList<>();
            for (Map.Entry<String, JsonElement> entry : comparison.entrySet()) {
                names.add(entry.getKey());
                values.add(entry.getValue().isJsonNull() ? null : entry.getValue().getAsString());
            }
            csv(file, names.toArray());
            csv(file, values.toArray());
        }
        final JsonObject result = result(data.manifest, metadata, comparison, prepared, scores,
            metadataFile, preparedHash, metadataHash, destination);
        // The manifest is the completion marker and is deliberately written last.
        try (BufferedWriter file = writer(destination.resolve("manifest.json"))) {
            file.write(JSON.toJson(result));
            file.write('\n');
        }
        return result;
    }

    static Prepared loadPrepared(Path directory) throws IOException {
        final JsonObject manifest = readJson(directory.resolve("manifest.json"));
        require("retrieval-baseline-inputs-v1".equals(text(manifest, "schema"))
            && PROFILE.equals(text(manifest, "profile")) && integer(manifest, "depth", 1) == DEPTH
            && integer(manifest, "finalK", 1) == FINAL_K, "Expected fixed retrieval baseline inputs");
        require("CLIMATE-FEVER".equals(text(manifest, "dataset"))
            && "1.0.1".equals(text(manifest, "release"))
            && ClimateFeverDataset.RELEASE_SHA256.equals(text(manifest, "rawSha256"))
            && bool(manifest, "officialReleaseBytes"), "Expected pinned CLIMATE-FEVER release");
        final JsonObject hashes = object(manifest.get("outputSha256"), "outputSha256");
        verifyFiles(directory, hashes);
        for (int index = 0; index < DATA_HASH_FIELDS.length; index++) {
            require(text(hashes, INPUT_FILES[index]).equals(fingerprint(manifest, DATA_HASH_FIELDS[index])),
                "Dataset hash reference mismatch: " + INPUT_FILES[index]);
        }
        final long count = integer(manifest, "queryCount", 1);
        require(count <= 1000 && count == integer(manifest, "evaluatedQueryCount", 1),
            "Invalid evaluated query count");
        integer(manifest, "pairCount", 0);
        final Map<String, Body> corpus = loadCorpus(directory, manifest);
        final List<Claim> claims = loadClaims(directory, manifest, corpus);
        return new Prepared(manifest, corpus, new ArrayList<>(claims.subList(0, (int) count)));
    }

    private static Map<String, Body> loadCorpus(Path directory, JsonObject manifest) throws IOException {
        final Map<String, Body> corpus = new TreeMap<>();
        try (BufferedReader rows = reader(directory.resolve("corpus.jsonl"))) {
            String line;
            while ((line = rows.readLine()) != null) {
                final JsonObject row = CODEC.parseObject(line);
                final String evidenceId = text(row, "evidence_id");
                final String body = text(row, "text");
                final String article = text(row, "article");
                require(evidenceId.equals("climate-evidence-" + hashText(body)),
                    "Corpus evidence ID differs from exact body hash");
                require(!corpus.containsKey(evidenceId)
                    && text(row, "source_id").equals("wikipedia-" + hashText(article)), "Invalid corpus identity");
                corpus.put(evidenceId, new Body(body, article));
            }
        }
        require(corpus.size() == integer(manifest, "corpusCount", 0), "Corpus count mismatch");
        return corpus;
    }

    private static List<Claim> loadClaims(Path directory, JsonObject manifest, Map<String, Body> corpus)
        throws IOException {
        final Map<String, Set<String>> titles = new HashMap<>();
        for (String evidenceId : corpus.keySet()) {
            titles.put(evidenceId, new TreeSet<>());
        }
        final List<Claim> claims = new ArrayList<>();
        final JsonArray selectedIds = new JsonArray();
        long pairCount = 0;
        long goldPairs = 0;
        long zeroGoldCount = 0;
        try (BufferedReader rows = reader(directory.resolve("subset.jsonl"));
             BufferedReader mappings = reader(directory.resolve("source-mappings.jsonl"));
             BufferedReader goldRows = reader(directory.resolve("gold.jsonl"))) {
            String line;
            while ((line = rows.readLine()) != null) {
                final JsonObject claim = CODEC.parseObject(line);
                final String id = text(claim, "claim_id");
                require(id.matches("0|[1-9][0-9]*"), "Invalid claim ID");
                require(claims.isEmpty() || new BigInteger(id).compareTo(
                    new BigInteger(claims.get(claims.size() - 1).id)) > 0, "Claims must follow numeric ID order");
                final String label = text(claim, "claim_label");
                require(Arrays.asList("SUPPORTS", "REFUTES", "NOT_ENOUGH_INFO", "DISPUTED").contains(label),
                    "Invalid claim label");
                final Set<String> gold = new HashSet<>();
                final Set<String> rawIds = new HashSet<>();
                for (JsonElement value : array(claim, "evidences")) {
                    final JsonObject evidence = object(value, "selected evidence");
                    final String evidenceId = text(evidence, "evidence_id");
                    final String article = text(evidence, "article");
                    final String rawId = text(evidence, "raw_evidence_id");
                    final String evidenceLabel = text(evidence, "evidence_label");
                    require(corpus.containsKey(evidenceId) && rawIds.add(rawId)
                        && Arrays.asList("SUPPORTS", "REFUTES", "NOT_ENOUGH_INFO").contains(evidenceLabel),
                        "Invalid selected evidence");
                    titles.get(evidenceId).add(article);
                    final JsonObject expected = new JsonObject();
                    expected.addProperty("claim_id", id);
                    expected.addProperty("raw_evidence_id", rawId);
                    expected.addProperty("evidence_id", evidenceId);
                    expected.addProperty("evidence_label", evidenceLabel);
                    expected.addProperty("source_id", "wikipedia-" + hashText(article));
                    expected.addProperty("article", article);
                    require(expected.equals(nextJson(mappings, "source mapping")),
                        "Source mapping differs from selected evidence");
                    pairCount++;
                    if (!"NOT_ENOUGH_INFO".equals(evidenceLabel)) {
                        gold.add(evidenceId);
                    }
                }
                final JsonObject goldRow = nextJson(goldRows, "Gold query");
                final Set<String> actualGold = new HashSet<>();
                for (JsonElement value : array(goldRow, "gold_evidence_ids")) {
                    require(actualGold.add(text(value, "gold evidence ID")), "Duplicate Gold evidence ID");
                }
                require(id.equals(text(goldRow, "claim_id")) && actualGold.equals(gold),
                    "Gold differs from selected evidence labels");
                claims.add(new Claim(id, text(claim, "claim"), label, gold));
                selectedIds.add(new JsonPrimitive(id));
                goldPairs += gold.size();
                if (gold.isEmpty()) {
                    zeroGoldCount++;
                }
            }
            exhausted(mappings, "source mapping");
            exhausted(goldRows, "Gold query");
        }
        require(claims.size() == 1000 && integer(manifest, "selectedClaimCount", 1) == 1000
            && integer(manifest, "rawClaimCount", 1) == 1535
            && selectedIds.equals(array(manifest, "selectedClaimIds")),
            "Selected claims differ from the pinned 1000-query subset");
        require(pairCount == integer(manifest, "selectedEvidencePairCount", 0)
            && goldPairs == integer(manifest, "positivePairCount", 0)
            && zeroGoldCount == integer(manifest, "zeroGoldClaimCount", 0),
            "Dataset counts differ from prepared manifest");
        try (BufferedReader rows = reader(directory.resolve("titles.jsonl"))) {
            for (Map.Entry<String, Body> entry : corpus.entrySet()) {
                final Set<String> sources = titles.get(entry.getKey());
                require(!sources.isEmpty() && sources.iterator().next().equals(entry.getValue().article),
                    "Corpus article must be the fixed lexicographically smallest source");
                final JsonObject expected = new JsonObject();
                expected.addProperty("evidence_id", entry.getKey());
                final JsonArray sourceTitles = new JsonArray();
                for (String title : sources) {
                    sourceTitles.add(new JsonPrimitive(title));
                }
                expected.add("titles", sourceTitles);
                require(expected.equals(nextJson(rows, "title row")), "Titles differ from original source mappings");
            }
            exhausted(rows, "title row");
        }
        return claims;
    }

    static void verifyFiles(Path directory, JsonObject hashes) throws IOException {
        fields(hashes, "Prepared hashes", INPUT_FILES);
        final Path root = directory.toRealPath();
        for (String name : INPUT_FILES) {
            final Path path = root.resolve(name).toRealPath();
            require(path.startsWith(root) && Files.isRegularFile(path), "Missing or unsafe artifact: " + name);
            require(sha256(path).equals(fingerprint(hashes, name)), "Artifact SHA-256 mismatch: " + name);
        }
    }

    static void validateMetadata(JsonObject metadata, JsonObject manifest) {
        validateMetadata(metadata, text(manifest.getAsJsonObject("outputSha256"), "title-input.jsonl"),
            integer(manifest, "queryCount", 1), integer(manifest, "pairCount", 0), DEPTH);
    }

    static void validateMetadata(JsonObject metadata, String inputSha256, long queryCount,
                                 long pairCount, int maxCandidates) {
        require(integer(metadata, "schemaVersion", 1) == 1, "Unsupported scoring metadata schema");
        require("cross-encoder/ms-marco-MiniLM-L6-v2".equals(text(metadata, "modelId"))
            && "233902d25c440f23af6f7d6e94d2946bac0bee0a".equals(text(metadata, "modelRevision")),
            "Expected pinned cross-encoder model and revision");
        final String device = text(metadata, "device");
        require("cpu".equals(device) || "cuda".equals(device), "Scoring device must be cpu or cuda");
        require(integer(metadata, "maxLength", 1) == 512
            && integer(metadata, "maxCandidates", 1) == maxCandidates
            && integer(metadata, "batchSize", 1) == 16 && integer(metadata, "threads", 1) == 4,
            "Baseline scoring configuration differs");
        require("raw_logit".equals(text(metadata, "scoreKind"))
            && "complete_pool".equals(text(metadata, "timingScope"))
            && (!metadata.has("reusedPairCount") || integer(metadata, "reusedPairCount", 0) == 0),
            "Expected complete-pool raw logits");
        require(text(metadata, "inputSha256").equals(inputSha256)
            && integer(metadata, "queryCount", 0) == queryCount
            && integer(metadata, "pairCount", 0) == pairCount,
            "Inference input/count references differ");
        integer(metadata, "inferenceNanos", 0);
        integer(metadata, "loadNanos", 0);
        fingerprint(metadata, "scoresSha256");
        require("2.8.0".equals(text(metadata, "torchVersion").split("\\+", 2)[0])
            && "4.57.1".equals(text(metadata, "transformersVersion")),
            "Scoring dependencies differ from the fixed baseline environment");
        if ("cuda".equals(device)) {
            text(metadata, "cudaVersion");
            text(metadata, "deviceName");
            integer(metadata, "deviceMemoryBytes", 1);
        }
    }

    static void validateQuery(JsonObject query) {
        validateQuery(query, DEPTH);
    }

    static void validateQuery(JsonObject query, int maxCandidates) {
        fields(query, "Query", "schemaVersion", "queryId", "queryText", "poolSha256", "candidates");
        require(integer(query, "schemaVersion", 1) == 1, "Unsupported query schema");
        text(query, "queryId");
        text(query, "queryText");
        fingerprint(query, "poolSha256");
        final JsonArray candidates = array(query, "candidates");
        require(candidates.size() <= maxCandidates, "Candidate pool exceeds " + maxCandidates);
        final Set<String> ids = new HashSet<>();
        final JsonArray canonicalCandidates = new JsonArray();
        for (JsonElement element : candidates) {
            final JsonObject candidate = object(element, "candidate");
            fields(candidate, "Candidate", "evidenceId", "text");
            final String id = text(candidate, "evidenceId");
            require(ids.add(id), "Duplicate evidence ID in input query");
            final JsonObject canonical = new JsonObject();
            canonical.addProperty("evidenceId", id);
            canonical.addProperty("text", text(candidate, "text"));
            canonicalCandidates.add(canonical);
        }
        final JsonObject canonical = new JsonObject();
        canonical.addProperty("schemaVersion", 1);
        canonical.addProperty("queryId", text(query, "queryId"));
        canonical.addProperty("queryText", text(query, "queryText"));
        canonical.add("candidates", canonicalCandidates);
        require(text(query, "poolSha256").equals(hashText(JSON.toJson(canonical))),
            "Pool fingerprint mismatch: " + text(query, "queryId"));
    }

    static boolean[] validateCandidates(JsonObject query, Map<String, Body> corpus, Claim claim,
                                                 BufferedReader annotations) throws IOException {
        final JsonArray candidates = array(query, "candidates");
        final boolean[] rules = new boolean[candidates.size()];
        final String normalized = TextNormalizer.normalize(claim.text);
        for (int index = 0; index < candidates.size(); index++) {
            final JsonObject candidate = candidates.get(index).getAsJsonObject();
            final String id = text(candidate, "evidenceId");
            final Body body = corpus.get(id);
            require(body != null && text(candidate, "text").equals(body.article + "\n" + body.text),
                "Candidate must preserve its original body and fixed article title");
            rules[index] = normalized.equals(TextNormalizer.normalize(body.text));
            final String expected = claim.id + "," + (index + 1) + "," + id + "," + rules[index];
            require(expected.equals(annotations.readLine()), "Candidate ID/order/RULE annotation mismatch");
        }
        return rules;
    }

    static ScoredPool scoreRow(JsonObject row, JsonObject query) {
        fields(row, "Score row", "schemaVersion", "queryId", "poolSha256", "elapsedNanos", "scores");
        require(integer(row, "schemaVersion", 1) == 1, "Unsupported score row schema");
        require(text(row, "queryId").equals(text(query, "queryId"))
            && text(row, "poolSha256").equals(text(query, "poolSha256")),
            "Score query/order/pool fingerprint mismatch");
        final long elapsed = integer(row, "elapsedNanos", 0);
        final JsonArray scores = array(row, "scores");
        final JsonArray candidates = array(query, "candidates");
        require(scores.size() == candidates.size(), "Scores must cover the complete candidate pool");
        final double[] logits = new double[scores.size()];
        for (int index = 0; index < scores.size(); index++) {
            final JsonObject value = object(scores.get(index), "Candidate score");
            fields(value, "Candidate score", "evidenceId", "logit");
            require(text(value, "evidenceId").equals(text(candidates.get(index).getAsJsonObject(), "evidenceId")),
                "Score candidate ID/order mismatch");
            final JsonElement logit = value.get("logit");
            require(logit != null && logit.isJsonPrimitive() && logit.getAsJsonPrimitive().isNumber(),
                "Invalid cross-encoder logit");
            logits[index] = logit.getAsDouble();
            require(Double.isFinite(logits[index]), "Invalid cross-encoder logit");
        }
        require(logits.length > 0 || elapsed == 0, "An empty pool must have zero inference time");
        return new ScoredPool(logits, elapsed);
    }

    static List<Integer> rankCandidates(JsonArray candidates, double[] logits, boolean[] rules) {
        require(candidates.size() == logits.length && logits.length == rules.length,
            "Candidate/score/RULE sizes differ");
        final List<Integer> order = new ArrayList<>();
        for (int index = 0; index < logits.length; index++) {
            require(Double.isFinite(logits[index]), "Invalid cross-encoder logit");
            order.add(index);
        }
        order.sort((left, right) -> {
            if (rules[left] != rules[right]) {
                return rules[left] ? -1 : 1;
            }
            if (rules[left]) {
                return Integer.compare(left, right);
            }
            // Numeric equality includes -0.0 == 0.0, as in the original Python sorter.
            final int scoreOrder = logits[left] == logits[right] ? 0 : Double.compare(logits[right], logits[left]);
            return scoreOrder != 0 ? scoreOrder : text(candidates.get(left).getAsJsonObject(), "evidenceId")
                .compareTo(text(candidates.get(right).getAsJsonObject(), "evidenceId"));
        });
        return order;
    }

    private static JsonObject result(JsonObject manifest, JsonObject metadata, JsonObject comparison,
                                     Path prepared, Path scores, Path metadataPath, String preparedHash,
                                     String metadataHash, Path output) throws IOException {
        final JsonObject result = new JsonObject();
        result.addProperty("schema", "retrieval-baseline-results-v1");
        result.addProperty("profile", PROFILE);
        result.addProperty("depth", DEPTH);
        result.addProperty("finalK", FINAL_K);
        result.add("queryCount", manifest.get("queryCount"));
        result.add("pairCount", manifest.get("pairCount"));
        result.addProperty("preparedDirectory", prepared.toString());
        result.addProperty("preparedManifestSha256", preparedHash);
        result.add("preparedOutputSha256", manifest.get("outputSha256"));
        result.addProperty("scoresPath", scores.toString());
        result.addProperty("metadataPath", metadataPath.toString());
        result.add("scoresSha256", metadata.get("scoresSha256"));
        result.addProperty("metadataSha256", metadataHash);
        result.add("inference", metadata);
        final JsonArray comparisons = new JsonArray();
        comparisons.add(comparison);
        result.add("comparison", comparisons);
        result.addProperty("rankingPolicy", "RULE exact matches first in original pool order; "
            + "other candidates by raw logit descending, evidence ID ascending");
        result.addProperty("goldPolicy", "SUPPORTS and REFUTES are positive; zero-Gold queries are "
            + "excluded from recall denominators; labels never affect ranking");
        final JsonObject hashes = new JsonObject();
        for (String file : new String[] {"comparison.csv", "query-results.csv", "final-candidates.csv"}) {
            hashes.addProperty(file, sha256(output.resolve(file)));
        }
        result.add("outputSha256", hashes);
        return result;
    }

    static final class Metrics {
        private long evaluatedQueryCount;
        private long positiveQueryCount;
        private long zeroGoldQueryCount;
        private long positivePairCount;
        private long candidatePairCount;
        private long hits;
        private long poolHits;
        private double recallSum;
        private double poolRecallSum;
        private long anyHits;
        private long allHits;

        void add(int goldCount, int finalHits, int candidateHits, int poolCount) {
            evaluatedQueryCount++;
            candidatePairCount += poolCount;
            if (goldCount == 0) {
                zeroGoldQueryCount++;
                return;
            }
            positiveQueryCount++;
            positivePairCount += goldCount;
            hits += finalHits;
            poolHits += candidateHits;
            recallSum += (double) finalHits / goldCount;
            poolRecallSum += (double) candidateHits / goldCount;
            if (finalHits > 0) {
                anyHits++;
            }
            if (finalHits == goldCount) {
                allHits++;
            }
        }

        JsonObject summary() {
            final JsonObject result = new JsonObject();
            result.addProperty("profile", PROFILE);
            result.addProperty("depth", DEPTH);
            result.addProperty("finalK", FINAL_K);
            number(result, "macroRecall", fraction(recallSum, positiveQueryCount));
            number(result, "microRecall", fraction(hits, positivePairCount));
            number(result, "anyHitRate", fraction(anyHits, positiveQueryCount));
            number(result, "allGoldHitRate", fraction(allHits, positiveQueryCount));
            number(result, "zeroHitRate", fraction(positiveQueryCount - anyHits, positiveQueryCount));
            number(result, "poolMacroRecall", fraction(poolRecallSum, positiveQueryCount));
            number(result, "poolMicroRecall", fraction(poolHits, positivePairCount));
            result.addProperty("evaluatedQueryCount", evaluatedQueryCount);
            result.addProperty("positiveQueryCount", positiveQueryCount);
            result.addProperty("zeroGoldQueryCount", zeroGoldQueryCount);
            result.addProperty("positivePairCount", positivePairCount);
            result.addProperty("candidatePairCount", candidatePairCount);
            result.addProperty("hits", hits);
            result.addProperty("poolHits", poolHits);
            return result;
        }
    }

    static final class ScoredPool {
        final double[] logits;
        final long elapsedNanos;

        private ScoredPool(double[] logits, long elapsedNanos) {
            this.logits = logits;
            this.elapsedNanos = elapsedNanos;
        }
    }

    static final class Prepared {
        final JsonObject manifest;
        final Map<String, Body> corpus;
        final List<Claim> claims;

        private Prepared(JsonObject manifest, Map<String, Body> corpus, List<Claim> claims) {
            this.manifest = manifest;
            this.corpus = corpus;
            this.claims = claims;
        }
    }

    static final class Body {
        final String text;
        final String article;

        Body(String text, String article) {
            this.text = text;
            this.article = article;
        }
    }

    static final class Claim {
        final String id;
        final String text;
        final String label;
        final Set<String> gold;

        private Claim(String id, String text, String label, Set<String> gold) {
            this.id = id;
            this.text = text;
            this.label = label;
            this.gold = gold;
        }
    }

    static String sha256(Path path) throws IOException {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(path)) {
                final byte[] buffer = new byte[8192];
                int count;
                while ((count = input.read(buffer)) != -1) {
                    digest.update(buffer, 0, count);
                }
            }
            final StringBuilder result = new StringBuilder(64);
            for (byte value : digest.digest()) {
                result.append(Character.forDigit((value & 0xff) >>> 4, 16));
                result.append(Character.forDigit(value & 0xf, 16));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is unavailable", error);
        }
    }

    static String hashText(String value) {
        return ClimateFeverDataset.sha256(value.getBytes(StandardCharsets.UTF_8));
    }

    static BufferedReader reader(Path path) throws IOException {
        return Files.newBufferedReader(path, StandardCharsets.UTF_8);
    }

    static BufferedWriter writer(Path path) throws IOException {
        return Files.newBufferedWriter(path, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
    }

    static JsonObject readJson(Path path) throws IOException {
        return CODEC.parseObject(new String(Files.readAllBytes(path), StandardCharsets.UTF_8));
    }

    static JsonObject nextJson(BufferedReader reader, String name) throws IOException {
        final String line = reader.readLine();
        require(line != null, "Missing " + name);
        return CODEC.parseObject(line);
    }

    static void exhausted(BufferedReader reader, String name) throws IOException {
        require(reader.readLine() == null, "Unexpected extra " + name);
    }

    static void emptyDirectory(Path directory) throws IOException {
        if (Files.exists(directory)) {
            require(Files.isDirectory(directory), "Output must be an empty directory");
            try (Stream<Path> files = Files.list(directory)) {
                require(!files.findAny().isPresent(), "Output directory must be empty");
            }
        }
        Files.createDirectories(directory);
    }

    static void csv(BufferedWriter writer, Object... values) throws IOException {
        for (int index = 0; index < values.length; index++) {
            if (index > 0) {
                writer.write(',');
            }
            final String value = values[index] == null ? "" : values[index].toString();
            if (value.indexOf(',') >= 0 || value.indexOf('"') >= 0
                || value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0) {
                writer.write('"');
                writer.write(value.replace("\"", "\"\""));
                writer.write('"');
            } else {
                writer.write(value);
            }
        }
        writer.write("\r\n");
    }

    static void fields(JsonObject value, String name, String... expected) {
        final Set<String> actual = new HashSet<>();
        for (Map.Entry<String, JsonElement> entry : value.entrySet()) {
            actual.add(entry.getKey());
        }
        require(actual.equals(new HashSet<>(Arrays.asList(expected))), name + " has missing or unexpected fields");
    }

    static JsonObject object(JsonElement value, String name) {
        require(value != null && value.isJsonObject(), name + " must be a JSON object");
        return value.getAsJsonObject();
    }

    static JsonArray array(JsonObject value, String name) {
        final JsonElement field = value.get(name);
        require(field != null && field.isJsonArray(), name + " must be an array");
        return field.getAsJsonArray();
    }

    static String text(JsonObject value, String name) {
        return text(value.get(name), name);
    }

    static String text(JsonElement value, String name) {
        require(value != null && value.isJsonPrimitive() && value.getAsJsonPrimitive().isString(),
            name + " must be a nonempty string");
        final String result = value.getAsString();
        require(!TextNormalizer.normalize(result).isEmpty(), name + " must be a nonempty string");
        return result;
    }

    private static boolean bool(JsonObject value, String name) {
        final JsonElement field = value.get(name);
        require(field != null && field.isJsonPrimitive() && field.getAsJsonPrimitive().isBoolean(),
            name + " must be boolean");
        return field.getAsBoolean();
    }

    static long integer(JsonObject value, String name, long minimum) {
        final JsonElement field = value.get(name);
        require(field != null && field.isJsonPrimitive() && field.getAsJsonPrimitive().isNumber()
            && field.getAsString().matches("0|[1-9][0-9]*"), name + " must be an integer >= " + minimum);
        try {
            final long result = Long.parseLong(field.getAsString());
            require(result >= minimum, name + " must be an integer >= " + minimum);
            return result;
        } catch (NumberFormatException error) {
            throw new IllegalArgumentException(name + " exceeds the supported integer range", error);
        }
    }

    static String fingerprint(JsonObject value, String name) {
        final String result = text(value, name);
        require(result.matches("[0-9a-f]{64}"), name + " must be a lowercase SHA-256");
        return result;
    }

    static Double fraction(double numerator, long denominator) {
        return denominator == 0 ? null : numerator / denominator;
    }

    private static void number(JsonObject target, String name, Double value) {
        target.add(name, value == null ? JsonNull.INSTANCE : new JsonPrimitive(value));
    }

    static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
