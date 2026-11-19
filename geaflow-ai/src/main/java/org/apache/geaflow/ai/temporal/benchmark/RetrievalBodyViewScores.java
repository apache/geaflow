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
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.exhausted;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.fingerprint;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.hashText;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.integer;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.nextJson;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.object;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.readJson;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.reader;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.require;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.scoreRow;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.sha256;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.text;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.validateQuery;
import static org.apache.geaflow.ai.temporal.benchmark.RetrievalBaselineEvaluation.writer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Validates complete body-view score sources before projecting logits onto the frozen pool. */
final class RetrievalBodyViewScores implements AutoCloseable {

    private static final Gson JSON = new GsonBuilder().disableHtmlEscaping().create();

    private final Path inputPath;
    private final Path scoresPath;
    private final Path metadataPath;
    private final String inputHash;
    private final String scoresHash;
    private final String metadataHash;
    private final JsonObject metadata;
    private final int maxCandidates;
    private final long expectedQueryCount;
    private final Map<String, RetrievalBaselineEvaluation.Body> corpus;
    private final BufferedReader inputs;
    private final BufferedReader scores;
    private final Set<String> queryIds = new HashSet<>();
    private long queryCount;
    private long pairCount;
    private long projectedPairCount;
    private long elapsedNanos;
    private boolean finished;

    static void prepare(Path preparedDirectory, Path bodyInput) throws IOException {
        final Path prepared = preparedDirectory.toRealPath();
        final String manifestHash = sha256(prepared.resolve("manifest.json"));
        final RetrievalBaselineEvaluation.Prepared data = RetrievalBaselineEvaluation.loadPrepared(prepared);
        final Path requested = bodyInput.toAbsolutePath().normalize();
        require(requested.getFileName() != null && requested.getParent() != null,
            "Body input must be a new file outside the prepared directory");
        final Path destination = requested.getParent().toRealPath().resolve(requested.getFileName());
        require(!destination.startsWith(prepared), "Body input must be outside the prepared directory");
        long pairs = 0;
        try (BufferedReader titleRows = reader(prepared.resolve("title-input.jsonl"));
             BufferedReader annotations = reader(prepared.resolve("candidates.csv"));
             BufferedWriter output = writer(destination)) {
            require("query_id,pool_rank,evidence_id,rule_exact".equals(annotations.readLine()),
                "Unexpected candidate CSV columns");
            for (RetrievalBaselineEvaluation.Claim claim : data.claims) {
                final JsonObject titleQuery = nextJson(titleRows, "title input query");
                validateQuery(titleQuery);
                require(claim.id.equals(text(titleQuery, "queryId"))
                    && claim.text.equals(text(titleQuery, "queryText")), "Input query order/text mismatch");
                RetrievalBaselineEvaluation.validateCandidates(titleQuery, data.corpus, claim, annotations);
                final JsonObject bodyQuery = new JsonObject();
                bodyQuery.addProperty("schemaVersion", 1);
                bodyQuery.addProperty("queryId", claim.id);
                bodyQuery.addProperty("queryText", claim.text);
                final JsonArray candidates = new JsonArray();
                for (JsonElement value : array(titleQuery, "candidates")) {
                    final String id = text(object(value, "candidate"), "evidenceId");
                    final JsonObject candidate = new JsonObject();
                    candidate.addProperty("evidenceId", id);
                    candidate.addProperty("text", data.corpus.get(id).text);
                    candidates.add(candidate);
                }
                bodyQuery.add("candidates", candidates);
                bodyQuery.addProperty("poolSha256", hashText(JSON.toJson(bodyQuery)));
                output.write(JSON.toJson(bodyQuery));
                output.write('\n');
                pairs = Math.addExact(pairs, candidates.size());
            }
            exhausted(titleRows, "title input query");
            exhausted(annotations, "candidate annotation");
        }
        require(pairs == integer(data.manifest, "pairCount", 0), "Prepared candidate pair count mismatch");
        RetrievalBaselineEvaluation.verifyFiles(prepared, object(data.manifest.get("outputSha256"), "outputSha256"));
        require(manifestHash.equals(sha256(prepared.resolve("manifest.json"))),
            "Prepared manifest changed while exporting body input");
    }

    RetrievalBodyViewScores(Path input, Path scoreFile, Path metadataFile,
                            Map<String, RetrievalBaselineEvaluation.Body> corpus,
                            long expectedQueryCount) throws IOException {
        require(expectedQueryCount > 0, "Expected at least one body-view query");
        inputPath = input.toRealPath();
        scoresPath = scoreFile.toRealPath();
        metadataPath = metadataFile.toRealPath();
        require(!inputPath.equals(scoresPath) && !inputPath.equals(metadataPath)
            && !scoresPath.equals(metadataPath), "Body-view source paths must differ");
        inputHash = sha256(inputPath);
        scoresHash = sha256(scoresPath);
        metadataHash = sha256(metadataPath);
        metadata = readJson(metadataPath);
        final long depth = integer(metadata, "maxCandidates", 1);
        require(depth == 200 || depth == 500, "Body-view source must have maxCandidates 200 or 500");
        maxCandidates = (int) depth;
        this.expectedQueryCount = expectedQueryCount;
        this.corpus = corpus;
        final long sourcePairs = integer(metadata, "pairCount", 0);
        require(sourcePairs <= Math.multiplyExact(expectedQueryCount, maxCandidates),
            "Body-view pair count exceeds the source pool limit");
        RetrievalBaselineEvaluation.validateMetadata(metadata, inputHash, expectedQueryCount,
            sourcePairs, maxCandidates);
        require(scoresHash.equals(fingerprint(metadata, "scoresSha256")), "Body-view scores SHA-256 mismatch");
        inputs = reader(inputPath);
        try {
            scores = reader(scoresPath);
        } catch (IOException | RuntimeException error) {
            inputs.close();
            throw error;
        }
    }

    double[] project(JsonObject currentTitleQuery) throws IOException {
        require(!finished && queryCount < expectedQueryCount, "Unexpected body-view query projection");
        validateQuery(currentTitleQuery);
        final JsonObject source = nextJson(inputs, "body-view input query");
        validateQuery(source, maxCandidates);
        require(queryIds.add(text(source, "queryId")), "Duplicate body-view query ID");
        require(text(source, "queryId").equals(text(currentTitleQuery, "queryId"))
            && text(source, "queryText").equals(text(currentTitleQuery, "queryText")),
            "Body-view source query order/text differs from the frozen title view");
        final RetrievalBaselineEvaluation.ScoredPool scored = scoreRow(nextJson(scores, "body-view score query"), source);
        final JsonArray candidates = array(source, "candidates");
        final Map<String, Double> byId = new HashMap<>();
        for (int index = 0; index < candidates.size(); index++) {
            final JsonObject candidate = object(candidates.get(index), "body candidate");
            final String id = text(candidate, "evidenceId");
            final RetrievalBaselineEvaluation.Body body = corpus.get(id);
            require(body != null && body.text.equals(text(candidate, "text")),
                "Body-view candidate must preserve the original corpus body");
            byId.put(id, scored.logits[index]);
        }
        final JsonArray current = array(currentTitleQuery, "candidates");
        final double[] result = new double[current.size()];
        for (int index = 0; index < current.size(); index++) {
            final JsonObject candidate = object(current.get(index), "title candidate");
            final String id = text(candidate, "evidenceId");
            final RetrievalBaselineEvaluation.Body body = corpus.get(id);
            require(body != null && (body.article + "\n" + body.text).equals(text(candidate, "text")),
                "Title-view candidate must preserve the original corpus title and body");
            final Double logit = byId.get(id);
            require(logit != null, "Body-view source does not cover frozen candidate: " + id);
            result[index] = logit;
        }
        queryCount++;
        pairCount = Math.addExact(pairCount, candidates.size());
        projectedPairCount = Math.addExact(projectedPairCount, current.size());
        elapsedNanos = Math.addExact(elapsedNanos, scored.elapsedNanos);
        return result;
    }

    JsonObject finish() throws IOException {
        require(!finished && queryCount == expectedQueryCount, "Body-view source query count mismatch");
        exhausted(inputs, "body-view input query");
        exhausted(scores, "body-view score query");
        require(pairCount == integer(metadata, "pairCount", 0)
            && elapsedNanos == integer(metadata, "inferenceNanos", 0),
            "Body-view complete-source pair/timing totals differ from metadata");
        require(inputHash.equals(sha256(inputPath)) && scoresHash.equals(sha256(scoresPath))
            && metadataHash.equals(sha256(metadataPath)), "Body-view source artifacts changed during evaluation");
        finished = true;
        final JsonObject result = new JsonObject();
        result.addProperty("sourceInputPath", inputPath.toString());
        result.addProperty("sourceScoresPath", scoresPath.toString());
        result.addProperty("sourceMetadataPath", metadataPath.toString());
        result.addProperty("inputSha256", inputHash);
        result.addProperty("scoresSha256", scoresHash);
        result.addProperty("metadataSha256", metadataHash);
        result.addProperty("sourceMaxCandidates", maxCandidates);
        result.addProperty("sourceQueryCount", queryCount);
        result.addProperty("sourcePairCount", pairCount);
        result.addProperty("sourceInferenceNanos", elapsedNanos);
        result.addProperty("projectedPairCount", projectedPairCount);
        result.addProperty("timingScope", "complete original source pool; no inferred timing for the projected 200 pool");
        result.add("originalMetadata", metadata);
        return result;
    }

    @Override
    public void close() throws IOException {
        try {
            inputs.close();
        } finally {
            scores.close();
        }
    }
}
