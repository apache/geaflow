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
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ClimateFeverDatasetTest {

    private static final Gson GSON = new Gson();
    @TempDir
    Path directory;

    @Test
    public void testNumericOrderTextDeduplicationGoldAndSourceMappings() throws Exception {
        ClimateFeverDataset dataset = load("fixture.jsonl", fixture());
        Assertions.assertEquals(Arrays.asList("2", "3", "4", "10"), dataset.getClaims().stream()
            .map(ClimateFeverDataset.Claim::getId).collect(Collectors.toList()));
        Assertions.assertEquals(3, dataset.getCorpus().size());
        Assertions.assertEquals(2, dataset.getClaims().get(0).getGoldIds().size());
        Assertions.assertEquals(1, dataset.getClaims().get(1).getGoldIds().size());
        Assertions.assertEquals(2, dataset.getClaims().get(2).getGoldIds().size());
        Assertions.assertTrue(dataset.getClaims().get(3).getGoldIds().isEmpty());
        JsonObject manifest = dataset.getManifest();
        Assertions.assertEquals(5, manifest.get("positivePairCount").getAsInt());
        Assertions.assertEquals(1, manifest.get("zeroGoldClaimCount").getAsInt());
        Assertions.assertEquals(1, manifest.get("notEnoughInfoClaimCount").getAsInt());
        Assertions.assertEquals("unknown", manifest.get("license").getAsString());
        Assertions.assertFalse(manifest.get("officialReleaseBytes").getAsBoolean());
        dataset.writeArtifacts(directory);
        String mappings = read(directory.resolve("source-mappings.jsonl"));
        Assertions.assertEquals(7, mappings.trim().split("\n").length);
        Assertions.assertTrue(mappings.contains("alpha-alias"));
        Assertions.assertTrue(mappings.contains("Other article"));
        for (String entry : new String[] {"subset", "corpus", "gold"}) {
            Assertions.assertEquals(manifest.get(entry + "Sha256").getAsString(),
                ClimateFeverDataset.sha256(Files.readAllBytes(directory.resolve(entry + ".jsonl"))));
        }
        List<JsonObject> reversed = fixture();
        Collections.reverse(reversed);
        ClimateFeverDataset reordered = load("reversed.jsonl", reversed);
        for (String name : new String[] {"subsetSha256", "corpusSha256", "mappingsSha256", "goldSha256"}) {
            Assertions.assertEquals(manifest.get(name), reordered.getManifest().get(name));
        }
        manifest.addProperty("license", "changed");
        Assertions.assertEquals("unknown", dataset.getManifest().get("license").getAsString());
    }

    @Test
    public void testFixedFirstThousandUseNumericClaimOrder() throws Exception {
        List<JsonObject> records = new ArrayList<>();
        for (int id = 1004; id >= 0; id--) {
            records.add(record(Integer.toString(id), "query", "NOT_ENOUGH_INFO"));
        }
        ClimateFeverDataset dataset = load("prefix.jsonl", records);
        Assertions.assertEquals(1000, dataset.getClaims().size());
        Assertions.assertEquals("0", dataset.getClaims().get(0).getId());
        Assertions.assertEquals("999", dataset.getClaims().get(999).getId());
        Assertions.assertEquals(1005, dataset.getManifest().get("rawClaimCount").getAsInt());
        Assertions.assertEquals(1000, dataset.getManifest().get("zeroGoldClaimCount").getAsInt());
    }

    @Test
    public void testHashMismatchDuplicateIdsAndInvalidBoundaryData() throws Exception {
        Path path = write("valid.jsonl", fixture());
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> ClimateFeverDataset.load(path, String.join("", Collections.nCopies(64, "0"))));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> ClimateFeverDataset.load(path, "wrong"));
        List<List<JsonObject>> invalid = Arrays.asList(
            Arrays.asList(record("1", "query", "SUPPORTS"), record("1", "other", "REFUTES")),
            Collections.singletonList(record("01", "query", "NOT_ENOUGH_INFO")),
            Collections.singletonList(record("-1", "query", "NOT_ENOUGH_INFO")),
            Collections.singletonList(record("1", " ", "NOT_ENOUGH_INFO")),
            Collections.singletonList(record("1", "query", "unknown")),
            Collections.singletonList(record("1", "query", "SUPPORTS",
                evidence("id", "alpha", "SUPPORTS", "article"),
                evidence("id", "alpha", "REFUTES", "article"))),
            Arrays.asList(record("1", "query", "SUPPORTS", evidence("id", "alpha", "SUPPORTS", "article")),
                record("2", "query", "SUPPORTS", evidence("id", "different", "SUPPORTS", "article"))),
            Collections.singletonList(record("1", "query", "SUPPORTS",
                evidence("id", "alpha", "DISPUTED", "article"))));
        for (int index = 0; index < invalid.size(); index++) {
            List<JsonObject> records = invalid.get(index);
            String name = "invalid-" + index + ".jsonl";
            Assertions.assertThrows(IllegalArgumentException.class, () -> load(name, records), name);
        }
        JsonObject numericId = record("1", "query", "NOT_ENOUGH_INFO");
        numericId.addProperty("claim_id", 1);
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> load("numeric-id.jsonl", Collections.singletonList(numericId)));
        Path malformed = directory.resolve("malformed.jsonl");
        Files.write(malformed, "{broken}\n".getBytes(StandardCharsets.UTF_8));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ClimateFeverDataset.load(malformed,
            ClimateFeverDataset.sha256(Files.readAllBytes(malformed))));
    }

    @Test
    public void testCaseSensitiveIdentityRemainsSeparateFromRetrievalNormalization() throws Exception {
        ClimateFeverDataset dataset = load("case.jsonl", Collections.singletonList(record("0", "alpha",
            "SUPPORTS", evidence("lower", "alpha", "SUPPORTS", "A"),
            evidence("upper", "ALPHA", "SUPPORTS", "A"))));
        Assertions.assertEquals(2, dataset.getCorpus().size());
        Assertions.assertEquals(2, dataset.getClaims().get(0).getGoldIds().size());
    }

    static List<JsonObject> fixture() {
        return new ArrayList<>(Arrays.asList(
            record("10", "gamma", "NOT_ENOUGH_INFO", evidence("g", "gamma", "NOT_ENOUGH_INFO", "Gamma")),
            record("4", "alpha beta", "DISPUTED", evidence("a", "alpha", "SUPPORTS", "Alpha"),
                evidence("b", "beta", "REFUTES", "Beta")),
            record("2", "alpha", "DISPUTED", evidence("a", "alpha", "SUPPORTS", "Alpha"),
                evidence("alpha-alias", "alpha", "SUPPORTS", "Other article"),
                evidence("b", "beta", "REFUTES", "Beta")),
            record("3", "beta", "REFUTES", evidence("b", "beta", "REFUTES", "Beta"))));
    }

    static JsonObject record(String id, String text, String label, JsonObject... evidences) {
        JsonObject record = new JsonObject();
        record.addProperty("claim_id", id);
        record.addProperty("claim", text);
        record.addProperty("claim_label", label);
        JsonArray array = new JsonArray();
        for (JsonObject evidence : evidences) {
            array.add(evidence);
        }
        record.add("evidences", array);
        return record;
    }

    static JsonObject evidence(String id, String text, String label, String article) {
        JsonObject evidence = new JsonObject();
        evidence.addProperty("evidence_id", id);
        evidence.addProperty("evidence", text);
        evidence.addProperty("evidence_label", label);
        evidence.addProperty("article", article);
        return evidence;
    }

    private ClimateFeverDataset load(String name, List<JsonObject> records) throws Exception {
        Path path = write(name, records);
        return ClimateFeverDataset.load(path, ClimateFeverDataset.sha256(Files.readAllBytes(path)));
    }

    private Path write(String name, List<JsonObject> records) throws Exception {
        Path path = directory.resolve(name);
        StringBuilder content = new StringBuilder();
        for (JsonObject record : records) {
            content.append(GSON.toJson(record)).append('\n');
        }
        Files.write(path, content.toString().getBytes(StandardCharsets.UTF_8));
        return path;
    }

    private static String read(Path path) throws Exception {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }
}
