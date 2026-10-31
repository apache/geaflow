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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.apache.geaflow.ai.temporal.model.VersionRelation;
import org.apache.geaflow.ai.temporal.model.VersionRelationType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class FactGoldEvaluationTest {

    private static final TemporalJsonCodec CODEC = new TemporalJsonCodec();

    @TempDir
    Path directory;

    @Test
    public void testFullGoldRunsAllProductionStrategiesAndPublishesDiagnostics() throws Exception {
        final Path output = directory.resolve("full");
        final JsonObject manifest = new FactGoldEvaluation().evaluate(gold(), output);
        Assertions.assertTrue(manifest.get("passed").getAsBoolean());
        Assertions.assertEquals(12, manifest.get("fixtureCount").getAsInt());
        Assertions.assertEquals(48, manifest.get("caseCount").getAsInt());
        Assertions.assertEquals(1, manifest.get("topK").getAsInt());
        Assertions.assertEquals(12, manifest.getAsJsonObject("inputSha256").entrySet().size());
        for (JsonElement item : manifest.getAsJsonArray("profiles")) {
            final JsonObject result = item.getAsJsonObject();
            Assertions.assertEquals(12, result.get("caseCount").getAsInt());
            Assertions.assertEquals(12, result.get("passedCaseCount").getAsInt());
            Assertions.assertEquals(3, result.get("conflictTP").getAsInt());
            Assertions.assertEquals(0, result.get("conflictFP").getAsInt());
            Assertions.assertEquals(0, result.get("conflictFN").getAsInt());
            Assertions.assertEquals(1.0, result.get("conflictF1").getAsDouble(), 0.0);
            Assertions.assertEquals(10, result.get("correctNegativeCaseCount").getAsInt());
            Assertions.assertEquals(5, result.get("rejectedBatchCount").getAsInt());
            for (String category : Arrays.asList("semantic", "graph", "query", "decision", "oracle")) {
                Assertions.assertEquals(12, result.get(category + "PassedCount").getAsInt());
            }
        }
        for (JsonElement item : manifest.getAsJsonArray("cases")) {
            final JsonObject result = item.getAsJsonObject();
            if (result.get("negativeCaseCount").getAsInt() == 1) {
                Assertions.assertTrue(result.get("conflictPrecision").isJsonNull());
                Assertions.assertTrue(result.get("conflictRecall").isJsonNull());
                Assertions.assertTrue(result.get("conflictF1").isJsonNull());
            }
            final Path diagnostics = output.resolve("diagnostics")
                .resolve(result.get("profile").getAsString()).resolve(result.get("fixture").getAsString());
            for (String file : Arrays.asList("actual-snapshot.json", "expected-snapshot.json",
                "checks.json", "decision-traces.jsonl", "query-traces.jsonl")) {
                Assertions.assertTrue(Files.isRegularFile(diagnostics.resolve(file)), file);
            }
        }
        Assertions.assertEquals(5, Files.readAllLines(output.resolve("comparison.csv")).size());
        Assertions.assertEquals(49, Files.readAllLines(output.resolve("cases.csv")).size());
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new FactGoldEvaluation().evaluate(gold(), output));
    }

    @Test
    public void testMetricsCountRealFalsePositivesAndFalseNegativesAndCanonicalEdges() {
        final VersionRelation common = conflict("a", "b");
        final JsonObject result = FactGoldEvaluation.conflictMetrics(
            Arrays.asList(common, conflict("b", "c")),
            Arrays.asList(common, conflict("d", "c"), conflict("b", "a"),
                new VersionRelation(VersionRelationType.SUPERSEDES, "x", "y")));
        Assertions.assertEquals(1, result.get("conflictTP").getAsInt());
        Assertions.assertEquals(1, result.get("conflictFP").getAsInt());
        Assertions.assertEquals(1, result.get("conflictFN").getAsInt());
        Assertions.assertEquals(0.5, result.get("conflictF1").getAsDouble(), 0.0);
        final JsonObject empty = FactGoldEvaluation.conflictMetrics(
            Collections.emptyList(), Collections.emptyList());
        Assertions.assertTrue(empty.get("conflictF1").isJsonNull());
        Assertions.assertTrue(empty.get("conflictPrecision").isJsonNull());
        Assertions.assertTrue(empty.get("conflictRecall").isJsonNull());
        final JsonObject missed = FactGoldEvaluation.conflictMetrics(
            Collections.singletonList(common), Collections.emptyList());
        Assertions.assertEquals(1, missed.get("conflictFN").getAsInt());
        Assertions.assertEquals(0.0, missed.get("conflictF1").getAsDouble(), 0.0);
    }

    @Test
    public void testChangedManualGoldProducesRealFpFnAndNonzeroCliWithoutRewritingGold() throws Exception {
        final Path input = copyGold("changed-gold");
        final Path positive = input.resolve("05-conflict-coexistence.json");
        final JsonObject removed = read(positive);
        removed.getAsJsonObject("expected").add("relations", new JsonArray());
        write(positive, removed);
        final Path negative = input.resolve("06-adjacent-intervals.json");
        final JsonObject added = read(negative);
        final JsonObject edge = new JsonObject();
        edge.addProperty("type", "CONFLICTS_WITH");
        edge.addProperty("from", "left-version");
        edge.addProperty("to", "right-version");
        added.getAsJsonObject("expected").getAsJsonArray("relations").add(edge);
        write(negative, added);
        final byte[] before = Files.readAllBytes(negative);
        final Path output = directory.resolve("mismatch");

        Assertions.assertEquals(1, cli(input, output, new ByteArrayOutputStream()));

        Assertions.assertArrayEquals(before, Files.readAllBytes(negative));
        final JsonObject manifest = read(output.resolve("manifest.json"));
        Assertions.assertFalse(manifest.get("passed").getAsBoolean());
        Assertions.assertEquals(48, manifest.getAsJsonArray("cases").size());
        for (JsonElement item : manifest.getAsJsonArray("profiles")) {
            Assertions.assertEquals(1, item.getAsJsonObject().get("conflictFP").getAsInt());
            Assertions.assertEquals(1, item.getAsJsonObject().get("conflictFN").getAsInt());
            Assertions.assertEquals(10, item.getAsJsonObject().get("passedCaseCount").getAsInt());
        }
    }

    @Test
    public void testCliRejectsUnknownAliasNonemptyOutputAndUnknownOptions() throws Exception {
        final Path input = copyGold("unknown-alias");
        final Path file = input.resolve("01-assert-entity-reference.json");
        final JsonObject fixture = read(file);
        final JsonArray unknown = new JsonArray();
        unknown.add(new JsonPrimitive("unknown-version"));
        fixture.getAsJsonObject("expected").getAsJsonArray("queries").get(0)
            .getAsJsonObject().add("versions", unknown);
        write(file, fixture);
        final ByteArrayOutputStream errors = new ByteArrayOutputStream();
        Assertions.assertEquals(2, cli(input, directory.resolve("bad"), errors));
        Assertions.assertTrue(new String(errors.toByteArray(), StandardCharsets.UTF_8)
            .contains("Unknown Gold query version alias"));
        final Path occupied = Files.createDirectory(directory.resolve("occupied"));
        final Path sentinel = occupied.resolve("keep.txt");
        Files.write(sentinel, Collections.singletonList("keep"), StandardCharsets.UTF_8);
        Assertions.assertEquals(2, cli(gold(), occupied, new ByteArrayOutputStream()));
        Assertions.assertEquals(Collections.singletonList("keep"), Files.readAllLines(sentinel));
        try (PrintStream sink = new PrintStream(new ByteArrayOutputStream())) {
            Assertions.assertEquals(2, new TemporalFactGoldCli().run(
                new String[] {"--gold-directory", "x", "--unknown", "x"}, sink, sink));
        }
    }

    private int cli(Path input, Path output, ByteArrayOutputStream errors) throws Exception {
        try (PrintStream out = new PrintStream(new ByteArrayOutputStream(), true, "UTF-8");
             PrintStream err = new PrintStream(errors, true, "UTF-8")) {
            return new TemporalFactGoldCli().run(new String[] {
                "--gold-directory", input.toString(), "--output", output.toString()}, out, err);
        }
    }

    private Path copyGold(String name) throws Exception {
        final Path copy = Files.createDirectory(directory.resolve(name));
        try (Stream<Path> paths = Files.list(gold())) {
            final List<Path> files = paths.collect(Collectors.toList());
            for (Path file : files) {
                Files.copy(file, copy.resolve(file.getFileName()));
            }
        }
        return copy;
    }

    private Path gold() throws Exception {
        return Paths.get(getClass().getResource("/temporal/gold/01-assert-entity-reference.json")
            .toURI()).getParent();
    }

    private static VersionRelation conflict(String from, String to) {
        return new VersionRelation(VersionRelationType.CONFLICTS_WITH, from, to);
    }

    private static JsonObject read(Path path) throws Exception {
        return CODEC.parseObject(new String(Files.readAllBytes(path), StandardCharsets.UTF_8));
    }

    private static void write(Path path, JsonObject value) throws Exception {
        Files.write(path, CODEC.toJson(value).getBytes(StandardCharsets.UTF_8));
    }
}
