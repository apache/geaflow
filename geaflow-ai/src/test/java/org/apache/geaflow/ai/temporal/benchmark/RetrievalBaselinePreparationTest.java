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
import com.google.gson.JsonParser;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.temporal.candidate.EvidenceCandidate;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.Source;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class RetrievalBaselinePreparationTest {

    @TempDir
    Path directory;

    @Test
    public void testRuleFirstOriginalHundredAndDeduplicatedEnglishAppend() {
        final List<EvidenceCandidate> english = new ArrayList<>();
        for (int index = 0; index < 220; index++) {
            final Evidence evidence = new Evidence("e" + index, new Source("source", "article"),
                "text " + index);
            english.add(new EvidenceCandidate(evidence, Collections.emptyList(), 1.0, index + 1));
        }
        final Evidence rule = english.get(219).getEvidence();
        final List<Evidence> pool = RetrievalBaselinePreparation.candidatePool(
            Collections.singletonList(rule), english.subList(0, 100), english);
        Assertions.assertEquals(200, pool.size());
        Assertions.assertSame(rule, pool.get(0));
        Assertions.assertEquals("e98", pool.get(99).getId());
        Assertions.assertEquals("e99", pool.get(100).getId());
        Assertions.assertEquals("e198", pool.get(199).getId());
        Assertions.assertEquals(200, pool.stream().map(Evidence::getId).distinct().count());
    }

    @Test
    public void testGlobalTitleBodyIdentityRuleAndInputsIndependentOfGold() throws Exception {
        final RetrievalBaselinePreparation preparation = new RetrievalBaselinePreparation();
        final Path first = directory.resolve("first");
        final Path relabeled = directory.resolve("relabeled");
        final JsonObject manifest = preparation.prepare(dataset("SUPPORTS"), first, 1000);
        preparation.prepare(dataset("NOT_ENOUGH_INFO"), relabeled, 1000);
        for (String file : new String[] {"title-input.jsonl", "candidates.csv"}) {
            Assertions.assertArrayEquals(Files.readAllBytes(first.resolve(file)),
                Files.readAllBytes(relabeled.resolve(file)));
        }
        final JsonObject query = new JsonParser().parse(new String(
            Files.readAllBytes(first.resolve("title-input.jsonl")), StandardCharsets.UTF_8).trim())
            .getAsJsonObject();
        final JsonObject exact = query.getAsJsonArray("candidates").get(0).getAsJsonObject();
        final String bodyId = "climate-evidence-" + ClimateFeverDataset.sha256(
            "alpha climate".getBytes(StandardCharsets.UTF_8));
        Assertions.assertEquals(bodyId, exact.get("evidenceId").getAsString());
        Assertions.assertEquals("A article\nalpha climate", exact.get("text").getAsString());
        Assertions.assertEquals(5, query.entrySet().size());
        Assertions.assertEquals(2, exact.entrySet().size());
        Assertions.assertEquals("0,1," + bodyId + ",true",
            Files.readAllLines(first.resolve("candidates.csv"), StandardCharsets.UTF_8).get(1));
        Assertions.assertEquals("retrieval-baseline-inputs-v1", manifest.get("schema").getAsString());
        Assertions.assertEquals("CE_TITLE_BODY", manifest.get("profile").getAsString());
        Assertions.assertEquals(7, manifest.getAsJsonObject("outputSha256").entrySet().size());
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> preparation.prepare(dataset("SUPPORTS"), first, 1000));
        final Path rejected = directory.resolve("rejected");
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> preparation.prepare(dataset("SUPPORTS"), rejected, 0));
        Assertions.assertFalse(Files.exists(rejected.resolve("manifest.json")));
    }

    private ClimateFeverDataset dataset(String label) throws Exception {
        final JsonObject claim = new JsonObject();
        claim.addProperty("claim_id", "0");
        claim.addProperty("claim", "ALPHA   climate");
        claim.addProperty("claim_label", label);
        final JsonArray evidences = new JsonArray();
        for (int index = 0; index < 3; index++) {
            final JsonObject evidence = new JsonObject();
            evidence.addProperty("evidence_id", "raw-" + index);
            evidence.addProperty("evidence", index == 2 ? "climate changes" : "alpha climate");
            evidence.addProperty("article", index == 0 ? "Z article" : "A article");
            evidence.addProperty("evidence_label", label);
            evidences.add(evidence);
        }
        claim.add("evidences", evidences);
        final byte[] bytes = (new Gson().toJson(claim) + "\n").getBytes(StandardCharsets.UTF_8);
        final Path raw = directory.resolve(label + ".jsonl");
        Files.write(raw, bytes);
        return ClimateFeverDataset.load(raw, ClimateFeverDataset.sha256(bytes));
    }
}
