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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class RetrievalBaselineEvaluationTest {

    private static final Gson JSON = new GsonBuilder().disableHtmlEscaping().create();

    @TempDir
    Path directory;

    @Test
    public void testRulePriorityNegativeLogitsAndEvidenceIdTies() {
        final JsonArray candidates = candidates("z", "b", "a", "rule-a", "rule-z");
        Assertions.assertEquals(Arrays.asList(3, 4, 0, 2, 1),
            RetrievalBaselineEvaluation.rankCandidates(candidates,
                new double[] {-1, -2, -2, -99, -100}, new boolean[] {false, false, false, true, true}));
        Assertions.assertEquals(Arrays.asList(1, 0), RetrievalBaselineEvaluation.rankCandidates(
            candidates("z", "a"), new double[] {0.0, -0.0}, new boolean[] {false, false}));
        Assertions.assertThrows(IllegalArgumentException.class, () -> RetrievalBaselineEvaluation.rankCandidates(
            candidates("a"), new double[] {Double.NaN}, new boolean[] {false}));
    }

    @Test
    public void testZeroGoldExcludedFromEveryRecallDenominator() {
        final RetrievalBaselineEvaluation.Metrics totals = new RetrievalBaselineEvaluation.Metrics();
        totals.add(0, 0, 0, 200);
        final JsonObject empty = totals.summary();
        Assertions.assertTrue(empty.get("macroRecall").isJsonNull());
        Assertions.assertTrue(empty.get("microRecall").isJsonNull());
        Assertions.assertTrue(empty.get("zeroHitRate").isJsonNull());
        totals.add(2, 1, 2, 200);
        totals.add(1, 0, 0, 3);
        final JsonObject result = totals.summary();
        Assertions.assertEquals(3, result.get("evaluatedQueryCount").getAsInt());
        Assertions.assertEquals(2, result.get("positiveQueryCount").getAsInt());
        Assertions.assertEquals(1, result.get("zeroGoldQueryCount").getAsInt());
        Assertions.assertEquals(403, result.get("candidatePairCount").getAsInt());
        Assertions.assertEquals(0.25, result.get("macroRecall").getAsDouble());
        Assertions.assertEquals(1.0 / 3, result.get("microRecall").getAsDouble());
        Assertions.assertEquals(0.5, result.get("anyHitRate").getAsDouble());
        Assertions.assertEquals(0.0, result.get("allGoldHitRate").getAsDouble());
        Assertions.assertEquals(0.5, result.get("zeroHitRate").getAsDouble());
        Assertions.assertEquals(0.5, result.get("poolMacroRecall").getAsDouble());
        Assertions.assertEquals(2.0 / 3, result.get("poolMicroRecall").getAsDouble());
    }

    @Test
    public void testScoreRequiresCompleteOrderedPoolAndFiniteLogits() {
        final JsonObject query = query(candidates("a", "b"));
        final JsonObject score = score(query, -4.5, -8.0);
        Assertions.assertDoesNotThrow(() -> RetrievalBaselineEvaluation.scoreRow(score, query));
        score.getAsJsonArray("scores").get(0).getAsJsonObject().addProperty("evidenceId", "b");
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> RetrievalBaselineEvaluation.scoreRow(score, query));
        final JsonObject wrongPool = score(query, -4.5, -8.0);
        wrongPool.addProperty("poolSha256", repeat('0', 64));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> RetrievalBaselineEvaluation.scoreRow(wrongPool, query));
        final JsonObject missing = score(query, -4.5);
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> RetrievalBaselineEvaluation.scoreRow(missing, query));
        final JsonObject overflow = score(query, -4.5, -8.0);
        overflow.getAsJsonArray("scores").get(0).getAsJsonObject().addProperty("logit", Double.POSITIVE_INFINITY);
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> RetrievalBaselineEvaluation.scoreRow(overflow, query));
        final JsonObject wrongSchemaType = score(query, -4.5, -8.0);
        wrongSchemaType.addProperty("schemaVersion", true);
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> RetrievalBaselineEvaluation.scoreRow(wrongSchemaType, query));
    }

    @Test
    public void testPoolFingerprintAndDuplicateFieldsAreRejected() {
        final JsonObject query = query(candidates("a", "b"));
        Assertions.assertDoesNotThrow(() -> RetrievalBaselineEvaluation.validateQuery(query));
        query.getAsJsonArray("candidates").get(0).getAsJsonObject().addProperty("text", "changed");
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> RetrievalBaselineEvaluation.validateQuery(query));
        final JsonObject duplicateCandidate = query(candidates("a", "a"));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> RetrievalBaselineEvaluation.validateQuery(duplicateCandidate));
        final TemporalJsonCodec codec = new TemporalJsonCodec();
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> codec.parseObject("{\"queryId\":\"1\",\"queryId\":\"2\"}"));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> codec.parseObject("{\"logit\":NaN}"));
    }

    @Test
    public void testPreparedFileHashMismatchIsRejected() throws Exception {
        final JsonObject hashes = new JsonObject();
        for (String file : new String[] {"subset.jsonl", "corpus.jsonl", "gold.jsonl", "source-mappings.jsonl",
            "titles.jsonl", "title-input.jsonl", "candidates.csv"}) {
            final Path path = directory.resolve(file);
            Files.write(path, (file + "\n").getBytes(StandardCharsets.UTF_8));
            hashes.addProperty(file, RetrievalBaselineEvaluation.sha256(path));
        }
        Assertions.assertDoesNotThrow(() -> RetrievalBaselineEvaluation.verifyFiles(directory, hashes));
        Files.write(directory.resolve("title-input.jsonl"), "changed\n".getBytes(StandardCharsets.UTF_8));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> RetrievalBaselineEvaluation.verifyFiles(directory, hashes));
    }

    private static JsonArray candidates(String... ids) {
        final JsonArray result = new JsonArray();
        for (String id : ids) {
            final JsonObject candidate = new JsonObject();
            candidate.addProperty("evidenceId", id);
            candidate.addProperty("text", "Article\nOriginal body " + id + "\u2028");
            result.add(candidate);
        }
        return result;
    }

    private static JsonObject query(JsonArray candidates) {
        final JsonObject result = new JsonObject();
        result.addProperty("schemaVersion", 1);
        result.addProperty("queryId", "1");
        result.addProperty("queryText", "Claim");
        result.add("candidates", candidates);
        result.addProperty("poolSha256", ClimateFeverDataset.sha256(JSON.toJson(result).getBytes(StandardCharsets.UTF_8)));
        return result;
    }

    private static JsonObject score(JsonObject query, double... logits) {
        final JsonObject result = new JsonObject();
        result.addProperty("schemaVersion", 1);
        result.addProperty("queryId", "1");
        result.add("poolSha256", query.get("poolSha256"));
        result.addProperty("elapsedNanos", 1);
        final JsonArray scores = new JsonArray();
        for (int index = 0; index < logits.length; index++) {
            final JsonObject value = new JsonObject();
            value.add("evidenceId", query.getAsJsonArray("candidates").get(index).getAsJsonObject().get("evidenceId"));
            value.addProperty("logit", logits[index]);
            scores.add(value);
        }
        result.add("scores", scores);
        return result;
    }

    private static String repeat(char value, int count) {
        final char[] result = new char[count];
        Arrays.fill(result, value);
        return new String(result);
    }
}
