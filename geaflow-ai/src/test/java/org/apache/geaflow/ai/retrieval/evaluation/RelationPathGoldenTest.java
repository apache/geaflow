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
package org.apache.geaflow.ai.retrieval.evaluation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Pins {@link RankingMetrics#relationPathHit} against a fixture shared with the Python evaluator.
 *
 * <p>The metric is implemented and unit tested in Java and the gold annotations are produced in
 * Python, so neither side alone proves the two agree. Without a shared fixture a divergence in how
 * relation labels are compared would only surface as an unexplained difference between the Java and
 * Python reports. The cases cover both fixed datasets: explicit {@code 2WikiMultiHopQA evidences}
 * annotations, and HotpotQA, which carries no relation annotations and therefore falls back to its
 * supporting-fact titles.
 */
class RelationPathGoldenTest {

    @Test
    void matchesTheSharedRelationPathFixture() {
        JsonObject golden = load();
        JsonArray cases = golden.getAsJsonArray("cases");
        assertEquals(7, cases.size(), "the golden fixture must keep its reviewed cases");

        for (int i = 0; i < cases.size(); i++) {
            JsonObject testCase = cases.get(i).getAsJsonObject();
            String id = testCase.get("id").getAsString();
            Set<String> targets = strings(testCase.getAsJsonArray("targets"));
            Set<String> predicted = strings(testCase.getAsJsonArray("predictedPath"));
            double expected = testCase.get("javaRelationPathHit").getAsDouble();

            assertEquals(expected, RankingMetrics.relationPathHit(predicted, targets), 1.0e-12,
                "relation path hit disagrees with the shared fixture for case " + id);
        }
    }

    @Test
    void fixtureRecordsWhereJavaAndPythonDisagreeOnGranularity() {
        // Java scores whole-label membership in a predicted label set; Python requires the full gold
        // sequence to appear contiguously inside one predicted path. The difference is real, so the
        // fixture records both values instead of implying the two implementations are equivalent.
        JsonArray cases = load().getAsJsonArray("cases");
        int divergences = 0;
        for (int i = 0; i < cases.size(); i++) {
            JsonObject testCase = cases.get(i).getAsJsonObject();
            double javaScore = testCase.get("javaRelationPathHit").getAsDouble();
            int pythonScore = testCase.get("pythonRelationPathHit").getAsInt();
            if (javaScore != pythonScore) {
                divergences++;
            }
        }
        assertTrue(divergences > 0,
            "the fixture must keep at least one case that exposes the granularity difference");
    }

    @Test
    void emptyTargetsYieldZeroSoUnalignedDatasetsAreNotScored() {
        // A single-paragraph answer has no inter-entity hop; scoring it as a miss would deflate the
        // metric on questions the dataset cannot express as a relation path.
        assertEquals(0.0D, RankingMetrics.relationPathHit(
            new HashSet<>(java.util.Collections.singletonList("anything")),
            new HashSet<>()), 1.0e-12);
        assertEquals(0.0D, RankingMetrics.relationPathHit(null,
            new HashSet<>(java.util.Collections.singletonList("a"))), 1.0e-12);
        assertEquals(0.0D, RankingMetrics.relationPathHit(
            new HashSet<>(java.util.Collections.singletonList("a")), null), 1.0e-12);
    }

    @Test
    void fixtureDocumentsBothSupportedGoldSources() {
        JsonArray cases = load().getAsJsonArray("cases");
        Set<String> sources = new HashSet<>();
        for (int i = 0; i < cases.size(); i++) {
            sources.add(cases.get(i).getAsJsonObject().get("source").getAsString());
        }
        assertTrue(sources.contains("2WikiMultiHopQA evidences"),
            "the fixture must cover the explicitly annotated dataset");
        assertTrue(sources.contains("HotpotQA supporting_facts titles"),
            "the fixture must cover the title-fallback dataset");
    }

    private static Set<String> strings(JsonArray array) {
        Set<String> values = new HashSet<>();
        for (int i = 0; i < array.size(); i++) {
            values.add(array.get(i).getAsString());
        }
        return values;
    }

    private static JsonObject load() {
        try (InputStreamReader reader = new InputStreamReader(
            RelationPathGoldenTest.class.getResourceAsStream(
                "/retrieval/evaluation/relation-path-golden.json"), StandardCharsets.UTF_8)) {
            JsonObject golden = new JsonParser().parse(reader).getAsJsonObject();
            assertNotNull(golden);
            return golden;
        } catch (Exception e) {
            throw new IllegalStateException("relation path golden fixture is unreadable", e);
        }
    }
}