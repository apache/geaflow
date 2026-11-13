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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class RetrievalCandidateEvaluationTest {

    private static final Gson GSON = new Gson();
    private static final List<String> ARTIFACTS = Arrays.asList("subset.jsonl", "corpus.jsonl",
        "source-mappings.jsonl", "gold.jsonl", "titles.jsonl", "comparison.csv",
        "query-results.csv", "final-candidates.csv");
    @TempDir
    Path directory;

    @Test
    public void testFourProfilesSixCutoffsAndGoldDenominators() throws Exception {
        final ClimateFeverDataset dataset = load("fixture.jsonl", ClimateFeverDatasetTest.fixture());
        final Path output = directory.resolve("result");
        final JsonObject result = new RetrievalCandidateEvaluation().evaluate(dataset, output);
        Assertions.assertEquals("retrieval-candidates-v1", result.get("schema").getAsString());
        Assertions.assertEquals("COMPLETED", result.get("status").getAsString());
        Assertions.assertEquals("cpu", result.get("device").getAsString());
        Assertions.assertFalse(result.get("modelInference").getAsBoolean());
        Assertions.assertEquals(4, result.get("queryCount").getAsInt());
        Assertions.assertEquals(3, result.get("corpusCount").getAsInt());
        Assertions.assertEquals(100, result.get("maxCandidates").getAsInt());
        Assertions.assertEquals(24, result.getAsJsonArray("comparison").size());
        final Set<String> combinations = new HashSet<>();
        for (JsonElement item : result.getAsJsonArray("comparison")) {
            final JsonObject summary = item.getAsJsonObject();
            Assertions.assertEquals(4, summary.get("evaluatedQueryCount").getAsInt());
            Assertions.assertEquals(3, summary.get("positiveQueryCount").getAsInt());
            Assertions.assertEquals(1, summary.get("zeroGoldQueryCount").getAsInt());
            Assertions.assertEquals(5, summary.get("positivePairCount").getAsInt());
            Assertions.assertTrue(combinations.add(summary.get("profile").getAsString()
                + ":" + summary.get("k").getAsInt()));
        }
        for (String profile : Arrays.asList("RULE_ONLY", "RULE_BM25", "RULE_MINHASH", "RULE_ALL")) {
            for (int k : new int[] {1, 5, 10, 20, 50, 100}) {
                Assertions.assertTrue(combinations.contains(profile + ":" + k));
            }
        }
        assertMetrics(summary(result, "RULE_ONLY", 1), 2, 0.5, 0.4, 2.0 / 3, 1.0 / 3, 1.0 / 3);
        assertMetrics(summary(result, "RULE_ONLY", 100), 2, 0.5, 0.4, 2.0 / 3, 1.0 / 3, 1.0 / 3);
        assertMetrics(summary(result, "RULE_BM25", 1), 3, 2.0 / 3, 0.6, 1, 1.0 / 3, 0);
        assertMetrics(summary(result, "RULE_BM25", 100), 4, 5.0 / 6, 0.8, 1, 2.0 / 3, 0);
        final List<String> candidates = Files.readAllLines(output.resolve("final-candidates.csv"),
            StandardCharsets.UTF_8);
        Assertions.assertEquals(Arrays.asList("RULE_ONLY,2,1," + evidenceId("alpha"),
            "RULE_ONLY,3,1," + evidenceId("beta"), "RULE_ONLY,10,1," + evidenceId("gamma")), candidates.stream()
            .filter(line -> line.startsWith("RULE_ONLY,"))
            .map(line -> String.join(",", Arrays.copyOf(line.split(",", 5), 4)))
            .collect(Collectors.toList()));
        Assertions.assertEquals(Arrays.asList("RULE_BM25,2,1," + evidenceId("alpha"),
            "RULE_BM25,3,1," + evidenceId("beta"), "RULE_BM25,4,1," + evidenceId("alpha"),
            "RULE_BM25,4,2," + evidenceId("beta"), "RULE_BM25,10,1," + evidenceId("gamma")), candidates.stream()
            .filter(line -> line.startsWith("RULE_BM25,"))
            .map(line -> String.join(",", Arrays.copyOf(line.split(",", 5), 4)))
            .collect(Collectors.toList()));
        final String queries = read(output.resolve("query-results.csv"));
        Assertions.assertTrue(queries.contains("RULE_ONLY,1,10,NOT_ENOUGH_INFO,0,1,1,0,0,"));
        Assertions.assertEquals(25, Files.readAllLines(output.resolve("comparison.csv")).size());
        Assertions.assertEquals(97, Files.readAllLines(output.resolve("query-results.csv")).size());
        assertOutputHashes(result, output);
        // Compare the persisted JSON; float primitives and parsed decimals have different equality semantics.
        Assertions.assertEquals(new TemporalJsonCodec().toJson(result),
            read(output.resolve("manifest.json")).trim());
    }

    @Test
    public void testLabelsCannotChangeCandidateInputsOrRankingAndRerunsAreDeterministic() throws Exception {
        final List<JsonObject> records = ClimateFeverDatasetTest.fixture();
        final ClimateFeverDataset original = load("original.jsonl", records);
        final RetrievalCandidateEvaluation evaluation = new RetrievalCandidateEvaluation();
        final Path first = directory.resolve("first");
        final Path repeated = directory.resolve("repeated");
        evaluation.evaluate(original, first);
        evaluation.evaluate(original, repeated);
        for (String name : ARTIFACTS) {
            Assertions.assertArrayEquals(Files.readAllBytes(first.resolve(name)),
                Files.readAllBytes(repeated.resolve(name)), name);
        }
        for (JsonObject record : records) {
            record.addProperty("claim_label", "NOT_ENOUGH_INFO");
            for (JsonElement item : record.getAsJsonArray("evidences")) {
                final JsonObject evidence = item.getAsJsonObject();
                final String label = evidence.get("evidence_label").getAsString();
                evidence.addProperty("evidence_label", "SUPPORTS".equals(label) ? "REFUTES" : "SUPPORTS");
            }
        }
        final Path relabeled = directory.resolve("relabeled");
        evaluation.evaluate(load("relabeled.jsonl", records), relabeled);
        for (String name : Arrays.asList("corpus.jsonl", "titles.jsonl")) {
            Assertions.assertArrayEquals(Files.readAllBytes(first.resolve(name)),
                Files.readAllBytes(relabeled.resolve(name)), name);
        }
        Assertions.assertEquals(rankings(first), rankings(relabeled));
    }

    @Test
    public void testCandidateBudgetIsOneHundredEvenWithMoreMatchingGold() throws Exception {
        final JsonObject[] evidence = new JsonObject[101];
        for (int index = 0; index < evidence.length; index++) {
            evidence[index] = ClimateFeverDatasetTest.evidence("item-" + index,
                "shared token" + index, "SUPPORTS", "Article");
        }
        final ClimateFeverDataset dataset = load("large.jsonl", Collections.singletonList(
            ClimateFeverDatasetTest.record("0", "shared", "SUPPORTS", evidence)));
        final Path output = directory.resolve("large");
        final JsonObject result = new RetrievalCandidateEvaluation().evaluate(dataset, output);
        final JsonObject summary = summary(result, "RULE_BM25", 100);
        Assertions.assertEquals(100, summary.get("candidatePairCount").getAsInt());
        Assertions.assertEquals(100, summary.get("hits").getAsInt());
        Assertions.assertEquals(100.0 / 101, summary.get("microRecall").getAsDouble(), 1e-12);
        final List<String> rows = Files.readAllLines(output.resolve("final-candidates.csv"));
        for (String profile : Arrays.asList("RULE_ONLY", "RULE_BM25", "RULE_MINHASH", "RULE_ALL")) {
            final List<String> candidates = rows.stream().filter(line -> line.startsWith(profile + ","))
                .collect(Collectors.toList());
            Assertions.assertTrue(candidates.size() <= 100);
            for (int rank = 0; rank < candidates.size(); rank++) {
                Assertions.assertEquals(rank + 1, Integer.parseInt(candidates.get(rank).split(",", 5)[2]));
            }
        }
    }

    @Test
    public void testExistingOutputIsNeverOverwritten() throws Exception {
        final ClimateFeverDataset dataset = load("fixture.jsonl", ClimateFeverDatasetTest.fixture());
        final Path output = directory.resolve("existing");
        Files.createDirectory(output);
        final Path sentinel = output.resolve("keep.txt");
        Files.write(sentinel, "keep".getBytes(StandardCharsets.UTF_8));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new RetrievalCandidateEvaluation().evaluate(dataset, output));
        Assertions.assertEquals("keep", read(sentinel));
        try (java.util.stream.Stream<Path> entries = Files.list(output)) {
            Assertions.assertEquals(1, entries.count());
        }
    }

    private ClimateFeverDataset load(String name, List<JsonObject> records) throws Exception {
        final Path input = directory.resolve(name);
        final StringBuilder contents = new StringBuilder();
        for (JsonObject record : records) {
            contents.append(GSON.toJson(record)).append('\n');
        }
        Files.write(input, contents.toString().getBytes(StandardCharsets.UTF_8));
        return ClimateFeverDataset.load(input, ClimateFeverDataset.sha256(Files.readAllBytes(input)));
    }

    private static JsonObject summary(JsonObject result, String profile, int k) {
        for (JsonElement item : result.getAsJsonArray("comparison")) {
            final JsonObject row = item.getAsJsonObject();
            if (profile.equals(row.get("profile").getAsString()) && row.get("k").getAsInt() == k) {
                return row;
            }
        }
        throw new AssertionError("Missing profile/cutoff: " + profile + "/" + k);
    }

    private static void assertMetrics(JsonObject summary, int hits, double macro, double micro,
                                      double any, double all, double zero) {
        Assertions.assertEquals(hits, summary.get("hits").getAsInt());
        Assertions.assertEquals(macro, summary.get("macroRecall").getAsDouble(), 1e-12);
        Assertions.assertEquals(micro, summary.get("microRecall").getAsDouble(), 1e-12);
        Assertions.assertEquals(any, summary.get("anyHitRate").getAsDouble(), 1e-12);
        Assertions.assertEquals(all, summary.get("allGoldHitRate").getAsDouble(), 1e-12);
        Assertions.assertEquals(zero, summary.get("zeroHitRate").getAsDouble(), 1e-12);
    }

    private static void assertOutputHashes(JsonObject result, Path output) throws Exception {
        final JsonObject hashes = result.getAsJsonObject("outputSha256");
        Assertions.assertEquals(new HashSet<>(ARTIFACTS), hashes.entrySet().stream()
            .map(Map.Entry::getKey).collect(Collectors.toSet()));
        for (String name : ARTIFACTS) {
            Assertions.assertEquals(ClimateFeverDataset.sha256(Files.readAllBytes(output.resolve(name))),
                hashes.get(name).getAsString(), name);
        }
    }

    private static List<String> rankings(Path output) throws Exception {
        final List<String> result = new ArrayList<>();
        for (String line : Files.readAllLines(output.resolve("final-candidates.csv"))) {
            result.add(line.substring(0, line.lastIndexOf(',')));
        }
        return result;
    }

    private static String evidenceId(String text) {
        return "climate-evidence-" + ClimateFeverDataset.sha256(text.getBytes(StandardCharsets.UTF_8));
    }

    private static String read(Path path) throws Exception {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }
}
