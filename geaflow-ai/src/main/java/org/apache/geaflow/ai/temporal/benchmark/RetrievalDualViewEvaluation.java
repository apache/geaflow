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

import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.array;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.csv;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.exhausted;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.fraction;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.integer;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.nextJson;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.readJson;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.reader;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.require;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.sha256;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.text;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.writer;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Compares one fixed title/body RRF experiment with the unchanged title baseline. */
public final class RetrievalDualViewEvaluation {

    static final String PROFILE = "CE_TITLE_BODY_RRF_75_25";
    static final double TITLE_WEIGHT = 0.75;
    static final double BODY_WEIGHT = 0.25;
    static final int RRF_CONSTANT = 60;
    static final int DEPTH = 200;
    static final int FINAL_K = 20;

    public JsonObject evaluate(Path preparedDirectory, Path titleScoresPath, Path titleMetadataPath,
                               Path bodyInputPath, Path bodyScoresPath, Path bodyMetadataPath,
                               Path output) throws IOException {
        final Path prepared = preparedDirectory.toRealPath();
        final Path titleScores = titleScoresPath.toRealPath();
        final Path titleMetadataFile = titleMetadataPath.toRealPath();
        final Path destination = RetrievalBaselineRunner.canonical(output);
        for (Path input : new Path[] {prepared, titleScores, titleMetadataFile,
            bodyInputPath.toRealPath(), bodyScoresPath.toRealPath(), bodyMetadataPath.toRealPath()}) {
            require(!destination.startsWith(input) && !input.startsWith(destination),
                "Experiment output overlaps an input: " + input);
        }
        final String preparedHash = sha256(prepared.resolve("manifest.json"));
        final RetrievalBaselineEvaluation.Prepared data = RetrievalBaselineEvaluation.loadPrepared(prepared);
        final String metadataHash = sha256(titleMetadataFile);
        final JsonObject titleMetadata = readJson(titleMetadataFile);
        RetrievalBaselineEvaluation.validateMetadata(titleMetadata, data.manifest);
        require(sha256(titleScores).equals(text(titleMetadata, "scoresSha256")), "Title scores SHA-256 mismatch");
        final RetrievalBaselineEvaluation.Metrics baselineTotals = new RetrievalBaselineEvaluation.Metrics();
        final RetrievalBaselineEvaluation.Metrics fusionTotals = new RetrievalBaselineEvaluation.Metrics();
        final Map<String, RetrievalBaselineEvaluation.Metrics[]> groups = new TreeMap<>();
        long titleElapsed = 0;
        long pairCount = 0;
        long newGold = 0;
        long lostGold = 0;
        int improved = 0;
        int regressed = 0;
        int rescued = 0;
        int newlyZero = 0;
        final JsonObject bodySource;
        try (RetrievalBodyViewScores bodyScores = new RetrievalBodyViewScores(bodyInputPath, bodyScoresPath,
                 bodyMetadataPath, data.corpus, data.claims.size())) {
            RetrievalBaselineEvaluation.emptyDirectory(destination);
            try (BufferedReader inputs = reader(prepared.resolve("title-input.jsonl"));
                 BufferedReader titles = reader(titleScores);
                 BufferedReader annotations = reader(prepared.resolve("candidates.csv"));
                 BufferedWriter queries = writer(destination.resolve("query-results.csv"));
                 BufferedWriter candidatesFile = writer(destination.resolve("final-candidates.csv"))) {
                require("query_id,pool_rank,evidence_id,rule_exact".equals(annotations.readLine()),
                    "Unexpected candidate CSV columns");
                csv(queries, "profile", "query_id", "claim_label", "gold_count", "actual_pool_count",
                    "pool_hits", "pool_recall", "final_hits", "recall", "title_final_hits",
                    "delta_hits_vs_title", "new_gold_hits", "lost_gold_hits");
                csv(candidatesFile, "profile", "query_id", "rank", "evidence_id", "rule_exact", "is_gold",
                    "title_cross_encoder_logit", "body_cross_encoder_logit", "title_ce_rank", "body_ce_rank",
                    "rrf_score", "source_pool_rank", "title_final_rank", "selected_top20", "selected_title_top20");
                for (RetrievalBaselineEvaluation.Claim claim : data.claims) {
                    final JsonObject query = nextJson(inputs, "input query");
                    RetrievalBaselineEvaluation.validateQuery(query);
                    require(text(query, "queryId").equals(claim.id)
                        && text(query, "queryText").equals(claim.text), "Input query order/text mismatch");
                    final JsonArray candidates = array(query, "candidates");
                    final boolean[] rules = RetrievalBaselineEvaluation.validateCandidates(query,
                        data.corpus, claim, annotations);
                    final RetrievalBaselineEvaluation.ScoredPool title = RetrievalBaselineEvaluation.scoreRow(
                        nextJson(titles, "title score query"), query);
                    titleElapsed = Math.addExact(titleElapsed, title.elapsedNanos);
                    pairCount = Math.addExact(pairCount, candidates.size());
                    final double[] body = bodyScores.project(query);
                    final int[] titleRanks = ceRanks(candidates, title.logits);
                    final int[] bodyRanks = ceRanks(candidates, body);
                    final List<Integer> titleOrder = RetrievalBaselineEvaluation.rankCandidates(candidates,
                        title.logits, rules);
                    final List<Integer> order = rankFromRanks(candidates, titleRanks, bodyRanks, rules, TITLE_WEIGHT);
                    final int[] titleFinalRanks = ranks(titleOrder);
                    final int[] finalRanks = ranks(order);
                    int poolHits = 0;
                    int titleHits = 0;
                    int hits = 0;
                    int added = 0;
                    int dropped = 0;
                    // Gold is consulted only after both orders have been fixed.
                    for (int index = 0; index < candidates.size(); index++) {
                        if (claim.gold.contains(text(candidates.get(index).getAsJsonObject(), "evidenceId"))) {
                            poolHits++;
                            final boolean inTitle = titleFinalRanks[index] <= FINAL_K;
                            final boolean inFusion = finalRanks[index] <= FINAL_K;
                            titleHits += inTitle ? 1 : 0;
                            hits += inFusion ? 1 : 0;
                            added += inFusion && !inTitle ? 1 : 0;
                            dropped += inTitle && !inFusion ? 1 : 0;
                        }
                    }
                    baselineTotals.add(claim.gold.size(), titleHits, poolHits, candidates.size());
                    fusionTotals.add(claim.gold.size(), hits, poolHits, candidates.size());
                    final RetrievalBaselineEvaluation.Metrics[] group = groups.computeIfAbsent(claim.label,
                        ignored -> new RetrievalBaselineEvaluation.Metrics[] {
                            new RetrievalBaselineEvaluation.Metrics(), new RetrievalBaselineEvaluation.Metrics()});
                    group[0].add(claim.gold.size(), titleHits, poolHits, candidates.size());
                    group[1].add(claim.gold.size(), hits, poolHits, candidates.size());
                    newGold += added;
                    lostGold += dropped;
                    improved += hits > titleHits ? 1 : 0;
                    regressed += hits < titleHits ? 1 : 0;
                    rescued += titleHits == 0 && hits > 0 ? 1 : 0;
                    newlyZero += titleHits > 0 && hits == 0 ? 1 : 0;
                    csv(queries, PROFILE, claim.id, claim.label, claim.gold.size(), candidates.size(), poolHits,
                        fraction(poolHits, claim.gold.size()), hits, fraction(hits, claim.gold.size()),
                        titleHits, hits - titleHits, added, dropped);
                    for (int position = 0; position < order.size(); position++) {
                        final int index = order.get(position);
                        final String evidenceId = text(candidates.get(index).getAsJsonObject(), "evidenceId");
                        csv(candidatesFile, PROFILE, claim.id, position + 1, evidenceId, rules[index],
                            claim.gold.contains(evidenceId), title.logits[index], body[index], titleRanks[index],
                            bodyRanks[index], rrf(titleRanks[index], bodyRanks[index], TITLE_WEIGHT), index + 1,
                            titleFinalRanks[index], position < FINAL_K, titleFinalRanks[index] <= FINAL_K);
                    }
                }
                exhausted(inputs, "input query");
                exhausted(titles, "title score query");
                exhausted(annotations, "candidate annotation");
            }
            bodySource = bodyScores.finish();
        }
        require(pairCount == integer(data.manifest, "pairCount", 0)
            && titleElapsed == integer(titleMetadata, "inferenceNanos", 0), "Title score pair/timing totals differ");
        RetrievalBaselineEvaluation.verifyFiles(prepared, data.manifest.getAsJsonObject("outputSha256"));
        require(sha256(prepared.resolve("manifest.json")).equals(preparedHash)
            && sha256(titleMetadataFile).equals(metadataHash)
            && sha256(titleScores).equals(text(titleMetadata, "scoresSha256")), "Title/prepared inputs changed");
        final JsonArray comparisons = new JsonArray();
        comparisons.add(baselineTotals.summary());
        comparisons.add(summary(fusionTotals));
        writeComparison(destination.resolve("comparison.csv"), comparisons);
        final JsonObject result = new JsonObject();
        result.addProperty("schema", "retrieval-dual-view-results-v1");
        result.addProperty("profile", PROFILE);
        result.addProperty("titleWeight", TITLE_WEIGHT);
        result.addProperty("bodyWeight", BODY_WEIGHT);
        result.addProperty("rrfConstant", RRF_CONSTANT);
        result.addProperty("depth", DEPTH);
        result.addProperty("finalK", FINAL_K);
        result.addProperty("queryCount", data.claims.size());
        result.addProperty("pairCount", pairCount);
        result.addProperty("preparedDirectory", prepared.toString());
        result.addProperty("preparedManifestSha256", preparedHash);
        result.add("preparedOutputSha256", data.manifest.get("outputSha256"));
        result.addProperty("titleScoresPath", titleScores.toString());
        result.addProperty("titleMetadataPath", titleMetadataFile.toString());
        result.add("titleScoresSha256", titleMetadata.get("scoresSha256"));
        result.addProperty("titleMetadataSha256", metadataHash);
        result.add("titleInference", titleMetadata);
        result.add("bodySource", bodySource);
        result.add("comparison", comparisons);
        final JsonArray grouped = new JsonArray();
        for (Map.Entry<String, RetrievalBaselineEvaluation.Metrics[]> entry : groups.entrySet()) {
            final JsonObject baseline = entry.getValue()[0].summary();
            final JsonObject fusion = summary(entry.getValue()[1]);
            baseline.addProperty("claimLabel", entry.getKey());
            fusion.addProperty("claimLabel", entry.getKey());
            grouped.add(baseline);
            grouped.add(fusion);
        }
        result.add("groupComparison", grouped);
        final JsonObject changes = new JsonObject();
        changes.addProperty("improvedQueries", improved);
        changes.addProperty("regressedQueries", regressed);
        changes.addProperty("rescuedZeroQueries", rescued);
        changes.addProperty("newZeroQueries", newlyZero);
        changes.addProperty("newGoldPairs", newGold);
        changes.addProperty("lostGoldPairs", lostGold);
        result.add("changesVsTitle", changes);
        result.addProperty("rankingPolicy", "RULE first in original pool order; otherwise "
            + "0.75/(60+titleCErank)+0.25/(60+bodyCErank), descending; ties by titleCErank then evidence ID; "
            + "both CE ranks are recomputed within the same at-most-200 pool, logits descending and ID ascending");
        result.addProperty("goldPolicy", "SUPPORTS and REFUTES are positive; zero-Gold queries are excluded "
            + "from recall denominators; labels never affect ranks, weights or candidate selection");
        result.addProperty("candidateExportPolicy", "Full fixed pool, including unselected and negative-logit evidence");
        result.addProperty("latencyPolicy", "Body source metadata retains original pool timing; projected "
            + "500-pool timing is not a 200-pool or online dual-view latency measurement");
        final JsonObject hashes = new JsonObject();
        for (String name : new String[] {"comparison.csv", "query-results.csv", "final-candidates.csv"}) {
            hashes.addProperty(name, sha256(destination.resolve(name)));
        }
        result.add("outputSha256", hashes);
        RetrievalBaselineRunner.writeJson(destination.resolve("manifest.json"), result, true);
        return result;
    }

    static int[] ceRanks(JsonArray candidates, double[] logits) {
        require(candidates.size() <= DEPTH, "Candidate pool exceeds 200");
        return ranks(RetrievalBaselineEvaluation.rankCandidates(candidates, logits, new boolean[candidates.size()]));
    }

    static List<Integer> rankCandidates(JsonArray candidates, double[] title, double[] body, boolean[] rules) {
        return rankCandidates(candidates, title, body, rules, TITLE_WEIGHT);
    }

    static List<Integer> rankCandidates(JsonArray candidates, double[] title, double[] body,
                                        boolean[] rules, double titleWeight) {
        require(rules.length == candidates.size(), "Candidate/RULE sizes differ");
        require(Double.isFinite(titleWeight) && titleWeight >= 0 && titleWeight <= 1, "Invalid title weight");
        return rankFromRanks(candidates, ceRanks(candidates, title), ceRanks(candidates, body), rules, titleWeight);
    }

    private static List<Integer> rankFromRanks(JsonArray candidates, int[] titleRanks, int[] bodyRanks,
                                               boolean[] rules, double titleWeight) {
        final List<Integer> order = new ArrayList<>();
        for (int index = 0; index < candidates.size(); index++) {
            order.add(index);
        }
        order.sort((left, right) -> {
            if (rules[left] != rules[right]) {
                return rules[left] ? -1 : 1;
            }
            if (rules[left]) {
                return Integer.compare(left, right);
            }
            final int scoreOrder = Double.compare(rrf(titleRanks[right], bodyRanks[right], titleWeight),
                rrf(titleRanks[left], bodyRanks[left], titleWeight));
            if (scoreOrder != 0) {
                return scoreOrder;
            }
            final int titleOrder = Integer.compare(titleRanks[left], titleRanks[right]);
            return titleOrder != 0 ? titleOrder : text(candidates.get(left).getAsJsonObject(), "evidenceId")
                .compareTo(text(candidates.get(right).getAsJsonObject(), "evidenceId"));
        });
        return order;
    }

    private static double rrf(int titleRank, int bodyRank, double titleWeight) {
        return titleWeight / (RRF_CONSTANT + titleRank) + (1.0 - titleWeight) / (RRF_CONSTANT + bodyRank);
    }

    private static int[] ranks(List<Integer> order) {
        final int[] ranks = new int[order.size()];
        for (int index = 0; index < order.size(); index++) {
            ranks[order.get(index)] = index + 1;
        }
        return ranks;
    }

    private static JsonObject summary(RetrievalBaselineEvaluation.Metrics metrics) {
        final JsonObject result = metrics.summary();
        result.addProperty("profile", PROFILE);
        return result;
    }

    private static void writeComparison(Path output, JsonArray rows) throws IOException {
        try (BufferedWriter file = writer(output)) {
            final List<Object> names = new ArrayList<>();
            for (Map.Entry<String, JsonElement> entry : rows.get(0).getAsJsonObject().entrySet()) {
                names.add(entry.getKey());
            }
            csv(file, names.toArray());
            for (JsonElement row : rows) {
                final List<Object> values = new ArrayList<>();
                for (Object name : names) {
                    final JsonElement value = row.getAsJsonObject().get(name.toString());
                    values.add(value.isJsonNull() ? null : value.getAsString());
                }
                csv(file, values.toArray());
            }
        }
    }
}
