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
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class RetrievalBodyViewScoresTest {

    private static final Gson JSON = new GsonBuilder().disableHtmlEscaping().create();

    @TempDir
    Path directory;

    @Test
    public void testProjectionUsesIdsAndRetainsComplete500SourceMetadata() throws Exception {
        final Fixture fixture = fixture(500);
        try (RetrievalBodyViewScores source = fixture.open()) {
            Assertions.assertArrayEquals(new double[] {150, -280}, source.project(fixture.current));
            final JsonObject provenance = source.finish();
            Assertions.assertEquals(500, provenance.get("sourceMaxCandidates").getAsInt());
            Assertions.assertEquals(500, provenance.get("sourcePairCount").getAsInt());
            Assertions.assertEquals(2, provenance.get("projectedPairCount").getAsInt());
            Assertions.assertEquals(7, provenance.get("sourceInferenceNanos").getAsInt());
            Assertions.assertEquals(fixture.metadata, provenance.getAsJsonObject("originalMetadata"));
            Assertions.assertEquals(500,
                provenance.getAsJsonObject("originalMetadata").get("maxCandidates").getAsInt());
        }
    }

    @Test
    public void testMissingCoverageAndWrongBodyAreRejected() throws Exception {
        final Fixture fixture = fixture(200);
        fixture.corpus.put("absent", new RetrievalBaselineEvaluation.Body("Absent body", "Article"));
        fixture.current.getAsJsonArray("candidates").get(0).getAsJsonObject().addProperty("evidenceId", "absent");
        fixture.current.getAsJsonArray("candidates").get(0).getAsJsonObject()
            .addProperty("text", "Article\nAbsent body");
        sign(fixture.current);
        try (RetrievalBodyViewScores source = fixture.open()) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> source.project(fixture.current));
        }
        fixture.input.getAsJsonArray("candidates").get(0).getAsJsonObject().addProperty("text", "Wrong body");
        fixture.save();
        try (RetrievalBodyViewScores source = fixture.open()) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> source.project(fixture.current));
        }
    }

    @Test
    public void testUnselectedSourceScoreMustStillBeFiniteAndOrdered() throws Exception {
        final Fixture fixture = fixture(500);
        fixture.score.getAsJsonArray("scores").get(499).getAsJsonObject()
            .addProperty("logit", new BigDecimal("1e999"));
        fixture.save();
        try (RetrievalBodyViewScores source = fixture.open()) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> source.project(fixture.current));
        }
        fixture.score.getAsJsonArray("scores").get(499).getAsJsonObject().addProperty("logit", 199);
        fixture.score.getAsJsonArray("scores").get(499).getAsJsonObject().addProperty("evidenceId", "wrong-id");
        fixture.save();
        try (RetrievalBodyViewScores source = fixture.open()) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> source.project(fixture.current));
        }
    }

    @Test
    public void testSourceTotalsTrailingRowsAndMutationAreRejected() throws Exception {
        final Fixture fixture = fixture(200);
        fixture.metadata.addProperty("inferenceNanos", 8);
        fixture.save();
        try (RetrievalBodyViewScores source = fixture.open()) {
            source.project(fixture.current);
            Assertions.assertThrows(IllegalArgumentException.class, source::finish);
        }
        fixture.metadata.addProperty("inferenceNanos", 7);
        fixture.save();
        Files.write(fixture.scoresPath, (JSON.toJson(fixture.score) + "\n" + JSON.toJson(fixture.score) + "\n")
            .getBytes(StandardCharsets.UTF_8));
        fixture.metadata.addProperty("scoresSha256", RetrievalBaselineEvaluation.sha256(fixture.scoresPath));
        fixture.writeMetadata();
        try (RetrievalBodyViewScores source = fixture.open()) {
            source.project(fixture.current);
            Assertions.assertThrows(IllegalArgumentException.class, source::finish);
        }
        fixture.save();
        try (RetrievalBodyViewScores source = fixture.open()) {
            source.project(fixture.current);
            Files.write(fixture.metadataPath, (JSON.toJson(fixture.metadata) + "\n\n").getBytes(StandardCharsets.UTF_8));
            Assertions.assertThrows(IllegalArgumentException.class, source::finish);
        }
    }

    @Test
    public void testWrongInputHashQueryTextAndSourceDepthAreRejected() throws Exception {
        final Fixture fixture = fixture(200);
        fixture.metadata.addProperty("inputSha256", RetrievalBaselineEvaluation.hashText("different"));
        fixture.writeMetadata();
        Assertions.assertThrows(IllegalArgumentException.class, fixture::open);
        fixture.save();
        fixture.current.addProperty("queryText", "Different query");
        sign(fixture.current);
        try (RetrievalBodyViewScores source = fixture.open()) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> source.project(fixture.current));
        }
        fixture.metadata.addProperty("maxCandidates", 1000);
        fixture.writeMetadata();
        Assertions.assertThrows(IllegalArgumentException.class, fixture::open);
    }

    private Fixture fixture(int depth) throws Exception {
        return new Fixture(depth);
    }

    private static void sign(JsonObject query) {
        query.remove("poolSha256");
        query.addProperty("poolSha256", RetrievalBaselineEvaluation.hashText(JSON.toJson(query)));
    }

    private final class Fixture {
        private final Path inputPath = directory.resolve("body-input.jsonl");
        private final Path scoresPath = directory.resolve("scores.jsonl");
        private final Path metadataPath = directory.resolve("metadata.json");
        private final Map<String, RetrievalBaselineEvaluation.Body> corpus = new HashMap<>();
        private final JsonObject input = query();
        private final JsonObject current = query();
        private final JsonObject score = new JsonObject();
        private final JsonObject metadata = new JsonObject();

        private Fixture(int depth) throws Exception {
            final JsonArray candidates = new JsonArray();
            final JsonArray values = new JsonArray();
            for (int index = 0; index < depth; index++) {
                final String id = "evidence-" + index;
                final RetrievalBaselineEvaluation.Body body =
                    new RetrievalBaselineEvaluation.Body("Body " + index, "Article");
                corpus.put(id, body);
                candidates.add(candidate(id, body.text));
                final JsonObject value = new JsonObject();
                value.addProperty("evidenceId", id);
                value.addProperty("logit", (double) index - 300);
                values.add(value);
            }
            input.add("candidates", candidates);
            final JsonArray selected = new JsonArray();
            for (int index : new int[] {depth - 50, 20}) {
                final String id = "evidence-" + index;
                selected.add(candidate(id, "Article\n" + corpus.get(id).text));
            }
            current.add("candidates", selected);
            sign(current);
            score.addProperty("schemaVersion", 1);
            score.addProperty("queryId", "1");
            score.addProperty("elapsedNanos", 7);
            score.add("scores", values);
            metadata.addProperty("schemaVersion", 1);
            metadata.addProperty("modelId", "cross-encoder/ms-marco-MiniLM-L6-v2");
            metadata.addProperty("modelRevision", "233902d25c440f23af6f7d6e94d2946bac0bee0a");
            metadata.addProperty("device", "cpu");
            metadata.addProperty("maxLength", 512);
            metadata.addProperty("maxCandidates", depth);
            metadata.addProperty("batchSize", 16);
            metadata.addProperty("threads", 4);
            metadata.addProperty("scoreKind", "raw_logit");
            metadata.addProperty("timingScope", "complete_pool");
            metadata.addProperty("queryCount", 1);
            metadata.addProperty("pairCount", depth);
            metadata.addProperty("loadNanos", 3);
            metadata.addProperty("inferenceNanos", 7);
            metadata.addProperty("torchVersion", "2.8.0");
            metadata.addProperty("transformersVersion", "4.57.1");
            save();
        }

        private void save() throws Exception {
            sign(input);
            score.add("poolSha256", input.get("poolSha256"));
            Files.write(inputPath, (JSON.toJson(input) + "\n").getBytes(StandardCharsets.UTF_8));
            Files.write(scoresPath, (JSON.toJson(score) + "\n").getBytes(StandardCharsets.UTF_8));
            metadata.addProperty("inputSha256", RetrievalBaselineEvaluation.sha256(inputPath));
            metadata.addProperty("scoresSha256", RetrievalBaselineEvaluation.sha256(scoresPath));
            writeMetadata();
        }

        private void writeMetadata() throws Exception {
            Files.write(metadataPath, (JSON.toJson(metadata) + "\n").getBytes(StandardCharsets.UTF_8));
        }

        private RetrievalBodyViewScores open() throws Exception {
            return new RetrievalBodyViewScores(inputPath, scoresPath, metadataPath, corpus, 1);
        }

        private JsonObject query() {
            final JsonObject result = new JsonObject();
            result.addProperty("schemaVersion", 1);
            result.addProperty("queryId", "1");
            result.addProperty("queryText", "Claim");
            return result;
        }

        private JsonObject candidate(String id, String body) {
            final JsonObject result = new JsonObject();
            result.addProperty("evidenceId", id);
            result.addProperty("text", body);
            return result;
        }
    }
}
