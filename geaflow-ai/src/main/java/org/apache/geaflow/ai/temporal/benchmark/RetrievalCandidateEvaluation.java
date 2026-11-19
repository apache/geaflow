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

import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.csv;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.emptyDirectory;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.fraction;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.require;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.sha256;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.writer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.ai.temporal.candidate.Bm25Index;
import org.apache.geaflow.ai.temporal.candidate.EvidenceCandidate;
import org.apache.geaflow.ai.temporal.candidate.EvidenceRetriever;
import org.apache.geaflow.ai.temporal.candidate.MinHashIndex;
import org.apache.geaflow.ai.temporal.candidate.RetrievalProfile;

/** Evaluates the four fixed candidate strategies without model inference or network access. */
public final class RetrievalCandidateEvaluation {

    private static final int MAX_CANDIDATES = 100;
    private static final int[] CUTOFFS = {1, 5, 10, 20, 50, 100};
    private static final RetrievalProfile[] PROFILES = {
        RetrievalProfile.RULE_ONLY, RetrievalProfile.RULE_BM25,
        RetrievalProfile.RULE_MINHASH, RetrievalProfile.RULE_ALL
    };
    private static final String[] OUTPUT_FILES = {
        "subset.jsonl", "corpus.jsonl", "source-mappings.jsonl", "gold.jsonl", "titles.jsonl",
        "comparison.csv", "query-results.csv", "final-candidates.csv"
    };
    private static final String[] COMPARISON_FIELDS = {
        "profile", "k", "macroRecall", "microRecall", "anyHitRate", "allGoldHitRate",
        "zeroHitRate", "poolMacroRecall", "poolMicroRecall", "evaluatedQueryCount",
        "positiveQueryCount", "zeroGoldQueryCount", "positivePairCount", "candidatePairCount",
        "hits", "poolHits"
    };
    private static final Gson JSON = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();

    public JsonObject evaluate(Path input, Path output) throws IOException {
        final Instant started = Instant.now();
        final long startedNanos = System.nanoTime();
        final Path source = input.toRealPath();
        final Path destination = RetrievalBaselineRunner.canonical(output);
        require(!source.startsWith(destination) && !destination.startsWith(source),
            "Output overlaps the raw dataset: " + source);
        final ClimateFeverDataset dataset = ClimateFeverDataset.load(source,
            ClimateFeverDataset.RELEASE_SHA256);
        require(dataset.getClaims().size() == ClimateFeverDataset.SUBSET_SIZE,
            "The fixed candidate experiment requires 1000 queries");
        return evaluate(dataset, destination, source, started, startedNanos);
    }

    JsonObject evaluate(ClimateFeverDataset dataset, Path output) throws IOException {
        return evaluate(Objects.requireNonNull(dataset, "dataset"),
            RetrievalBaselineRunner.canonical(output), null, Instant.now(), System.nanoTime());
    }

    private JsonObject evaluate(ClimateFeverDataset dataset, Path destination, Path source,
                                Instant started, long startedNanos) throws IOException {
        for (Path ancestor = destination; ancestor != null; ancestor = ancestor.getParent()) {
            require(!Files.isRegularFile(ancestor.resolve("manifest.json")),
                "Output must not be inside an existing experiment directory: " + ancestor);
        }
        emptyDirectory(destination);
        dataset.writeArtifacts(destination);
        final JsonArray comparisons = new JsonArray();
        final JsonArray timings = new JsonArray();
        try (BufferedWriter queries = writer(destination.resolve("query-results.csv"));
             BufferedWriter candidates = writer(destination.resolve("final-candidates.csv"))) {
            csv(queries, "profile", "k", "query_id", "claim_label", "gold_count",
                "actual_pool_count", "returned_count", "hits", "pool_hits", "recall");
            csv(candidates, "profile", "query_id", "rank", "evidence_id", "score",
                "channel_scores", "channel_ranks", "is_gold");
            for (RetrievalProfile profile : PROFILES) {
                evaluateProfile(dataset, profile, queries, candidates, comparisons, timings);
            }
        }
        try (BufferedWriter comparison = writer(destination.resolve("comparison.csv"))) {
            csv(comparison, (Object[]) COMPARISON_FIELDS);
            for (JsonElement item : comparisons) {
                final JsonObject row = item.getAsJsonObject();
                final Object[] values = new Object[COMPARISON_FIELDS.length];
                for (int index = 0; index < COMPARISON_FIELDS.length; index++) {
                    final JsonElement value = row.get(COMPARISON_FIELDS[index]);
                    values[index] = value.isJsonNull() ? null : value.getAsString();
                }
                csv(comparison, values);
            }
        }
        if (source != null) {
            require(ClimateFeverDataset.RELEASE_SHA256.equals(sha256(source)),
                "Raw dataset changed during candidate evaluation");
        }
        final JsonObject manifest = dataset.getManifest();
        manifest.addProperty("schema", "retrieval-candidates-v1");
        manifest.addProperty("status", "COMPLETED");
        manifest.addProperty("queryCount", dataset.getClaims().size());
        manifest.addProperty("corpusCount", dataset.getCorpus().size());
        manifest.addProperty("modelInference", false);
        manifest.addProperty("device", "cpu");
        manifest.addProperty("networkAccess", false);
        manifest.addProperty("maxCandidates", MAX_CANDIDATES);
        manifest.addProperty("retrievalInput", "Original query and original evidence body only; no title or Gold input");
        manifest.addProperty("goldPolicy", "Gold labels affect metrics and export annotations only; never candidate generation");
        manifest.addProperty("rankingPolicy", "One search(query,100) per profile and query; every K uses the same ranking prefix");
        manifest.addProperty("unjudgedPolicy", "Non-Gold candidates are unjudged, not asserted negative");
        manifest.addProperty("candidateExportPolicy", "All returned candidates up to 100, once per profile and query");
        if (source != null) {
            manifest.addProperty("inputPath", source.toString());
        }
        final JsonArray profiles = new JsonArray();
        for (RetrievalProfile profile : PROFILES) {
            profiles.add(new JsonPrimitive(profile.name()));
        }
        manifest.add("profiles", profiles);
        final JsonArray cutoffs = new JsonArray();
        for (int cutoff : CUTOFFS) {
            cutoffs.add(new JsonPrimitive(cutoff));
        }
        manifest.add("cutoffs", cutoffs);
        manifest.add("parameters", parameters());
        manifest.add("comparison", comparisons);
        manifest.add("profileTimings", timings);
        manifest.addProperty("startedAtUtc", started.toString());
        manifest.addProperty("javaVersion", System.getProperty("java.version"));
        manifest.addProperty("javaVendor", System.getProperty("java.vendor"));
        manifest.addProperty("osName", System.getProperty("os.name"));
        manifest.addProperty("osArch", System.getProperty("os.arch"));
        manifest.addProperty("availableProcessors", Runtime.getRuntime().availableProcessors());
        manifest.addProperty("maxHeapBytes", Runtime.getRuntime().maxMemory());
        final JsonObject hashes = new JsonObject();
        for (String file : OUTPUT_FILES) {
            hashes.addProperty(file, sha256(destination.resolve(file)));
        }
        manifest.add("outputSha256", hashes);
        manifest.addProperty("completedAtUtc", Instant.now().toString());
        manifest.addProperty("wallMillis", (System.nanoTime() - startedNanos) / 1_000_000.0);
        try (BufferedWriter file = writer(destination.resolve("manifest.json"))) {
            file.write(JSON.toJson(manifest));
            file.newLine();
        }
        return manifest;
    }

    private static void evaluateProfile(ClimateFeverDataset dataset, RetrievalProfile profile,
                                         BufferedWriter queries, BufferedWriter candidates,
                                         JsonArray comparisons, JsonArray timings) throws IOException {
        final long startedNanos = System.nanoTime();
        final RetrievalBaselineEvaluation.Metrics[] metrics =
            new RetrievalBaselineEvaluation.Metrics[CUTOFFS.length];
        for (int index = 0; index < metrics.length; index++) {
            metrics[index] = new RetrievalBaselineEvaluation.Metrics();
        }
        long searchNanos = 0;
        long candidatePairCount = 0;
        final long buildStartedNanos = System.nanoTime();
        final long buildNanos;
        try (EvidenceRetriever retriever = new EvidenceRetriever(dataset.getCorpus(), profile)) {
            buildNanos = System.nanoTime() - buildStartedNanos;
            for (ClimateFeverDataset.Claim claim : dataset.getClaims()) {
                final long searchStartedNanos = System.nanoTime();
                final List<EvidenceCandidate> ranking = retriever.search(claim.getText(), MAX_CANDIDATES);
                searchNanos += System.nanoTime() - searchStartedNanos;
                candidatePairCount += ranking.size();
                final int[] prefixHits = new int[ranking.size() + 1];
                for (int index = 0; index < ranking.size(); index++) {
                    final EvidenceCandidate candidate = ranking.get(index);
                    final String evidenceId = candidate.getEvidence().getId();
                    final boolean gold = claim.getGoldIds().contains(evidenceId);
                    prefixHits[index + 1] = prefixHits[index] + (gold ? 1 : 0);
                    csv(candidates, profile.name(), claim.getId(), index + 1, evidenceId,
                        candidate.getScore(), JSON.toJson(candidate.getChannelScores()),
                        JSON.toJson(candidate.getChannelRanks()), gold);
                }
                final int poolHits = prefixHits[ranking.size()];
                for (int index = 0; index < CUTOFFS.length; index++) {
                    final int returned = Math.min(CUTOFFS[index], ranking.size());
                    final int hits = prefixHits[returned];
                    metrics[index].add(claim.getGoldIds().size(), hits, poolHits, ranking.size());
                    csv(queries, profile.name(), CUTOFFS[index], claim.getId(), claim.getLabel(),
                        claim.getGoldIds().size(), ranking.size(), returned, hits, poolHits,
                        fraction(hits, claim.getGoldIds().size()));
                }
            }
        }
        for (int index = 0; index < CUTOFFS.length; index++) {
            final JsonObject summary = metrics[index].summary();
            summary.addProperty("profile", profile.name());
            summary.remove("depth");
            summary.remove("finalK");
            summary.addProperty("k", CUTOFFS[index]);
            comparisons.add(summary);
        }
        final JsonObject timing = new JsonObject();
        timing.addProperty("profile", profile.name());
        timing.addProperty("queryCount", dataset.getClaims().size());
        timing.addProperty("candidatePairCount", candidatePairCount);
        timing.addProperty("indexBuildMillis", buildNanos / 1_000_000.0);
        timing.addProperty("querySearchMillis", searchNanos / 1_000_000.0);
        timing.addProperty("wallMillis", (System.nanoTime() - startedNanos) / 1_000_000.0);
        timings.add(timing);
    }

    private static JsonObject parameters() {
        final JsonObject result = new JsonObject();
        result.addProperty("rule", "NFC, Locale.ROOT lowercase, collapsed Unicode whitespace, full-body equality");
        result.addProperty("bm25Analyzer", RetrievalProfile.RULE_BM25.getAnalyzerName());
        result.addProperty("bm25K1", Bm25Index.K1);
        result.addProperty("bm25B", Bm25Index.B);
        result.addProperty("indexedField", "evidence");
        result.addProperty("fieldWeight", 1.0);
        result.addProperty("candidatePoolSize", MAX_CANDIDATES);
        result.addProperty("rrfK", RetrievalProfile.RRF_K);
        result.addProperty("mergePolicy", "Rule candidates first; optional candidates by equal-weight RRF, then evidence ID");
        result.addProperty("minHashShingleSize", MinHashIndex.SHINGLE_SIZE);
        result.addProperty("minHashHashCount", MinHashIndex.HASH_COUNT);
        result.addProperty("minHashBands", MinHashIndex.BANDS);
        result.addProperty("minHashRowsPerBand", MinHashIndex.ROWS_PER_BAND);
        result.addProperty("minHashSeed", MinHashIndex.SEED);
        result.addProperty("minHashHashAlgorithm", MinHashIndex.HASH_ALGORITHM);
        result.addProperty("minHashTokenization", MinHashIndex.TOKENIZATION);
        result.addProperty("jaccardThreshold", MinHashIndex.JACCARD_THRESHOLD);
        result.addProperty("minHashPolicy", "LSH candidate recall, then exact Jaccard verification; not an exact all-pairs scan");
        return result;
    }
}
