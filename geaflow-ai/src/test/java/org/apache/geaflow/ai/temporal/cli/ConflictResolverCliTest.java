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

package org.apache.geaflow.ai.temporal.cli;

import com.google.gson.JsonObject;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.temporal.api.ReplayResult;
import org.apache.geaflow.ai.temporal.api.TemporalMemoryFacade;
import org.apache.geaflow.ai.temporal.candidate.RetrievalProfile;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.oracle.FullReplayOracle;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ConflictResolverCliTest {

    private static final Instant BASE = Instant.parse("2024-01-01T00:00:00Z");
    private static final FactKey KEY = new FactKey("alice", "city", "profile");
    private final TemporalJsonCodec codec = new TemporalJsonCodec();

    @TempDir
    Path directory;

    @Test
    public void testFourProfilesPublishSameOracleStateAndPersistTheirActualConfiguration() throws Exception {
        List<NormalizedMemoryEvent> events = events();
        CanonicalSnapshot expected = new FullReplayOracle().replayToSnapshot(events);
        Path input = input(events);
        for (RetrievalProfile profile : Arrays.asList(RetrievalProfile.RULE_ONLY,
            RetrievalProfile.RULE_BM25, RetrievalProfile.RULE_MINHASH, RetrievalProfile.RULE_ALL)) {
            Path run = directory.resolve(profile.name() + ".json");
            ByteArrayOutputStream errors = new ByteArrayOutputStream();
            int code = invoke(errors, "replay", "--input", input.toString(), "--output", run.toString(),
                "--candidate-profile", profile.name(), "--candidate-top-k", "1");
            Assertions.assertEquals(0, code, errors.toString("UTF-8"));
            byte[] before = Files.readAllBytes(run);
            TemporalMemoryFacade loaded = new TemporalRunStore().load(run);
            Assertions.assertEquals(expected, loaded.getSnapshot());
            Assertions.assertEquals(profile.name(), loaded.getOperations().get(0)
                .getAsJsonObject().get("candidateProfile").getAsString());
            for (org.apache.geaflow.ai.temporal.trace.DecisionTrace trace : loaded.getDecisionTraces()) {
                Assertions.assertEquals("COMPLETED", trace.toJson().get("retrievalStatus").getAsString());
                Assertions.assertTrue(trace.toJson().get("candidateScores").isJsonObject());
            }
            ReplayResult duplicate = loaded.applyBatch(Collections.singletonList(events.get(0)), profile, 1);
            Assertions.assertEquals("DUPLICATE_NOOP", duplicate.getDecisionTraces().get(0).getCode());
            Assertions.assertTrue(duplicate.getDecisionTraces().get(0).toJson()
                .get("candidateScores").isJsonNull());
            Assertions.assertEquals(expected, loaded.getSnapshot());
            Assertions.assertArrayEquals(before, Files.readAllBytes(run));
            Path updated = directory.resolve(profile.name() + "-updated.json");
            new TemporalRunStore().save(updated, loaded);
            Assertions.assertEquals(loaded.getOperations(), new TemporalRunStore().load(updated).getOperations());
        }
    }

    @Test
    public void testLegacyRunAndNewTraceTamperingRemainStrictlyVerified() throws Exception {
        TemporalMemoryFacade legacy = new TemporalMemoryFacade();
        legacy.replay(events());
        Path oldRun = directory.resolve("legacy.json");
        new TemporalRunStore().save(oldRun, legacy);
        Assertions.assertEquals(legacy.getSnapshot(), new TemporalRunStore().load(oldRun).getSnapshot());
        Assertions.assertFalse(legacy.getOperations().get(0).getAsJsonObject().has("candidateProfile"));
        Assertions.assertEquals("NOT_APPLICABLE_WEEK6", legacy.getDecisionTraces().get(0)
            .toJson().get("retrievalStatus").getAsString());

        TemporalMemoryFacade current = new TemporalMemoryFacade();
        current.replay(events(), RetrievalProfile.RULE_ALL, 1);
        Path run = directory.resolve("new.json");
        new TemporalRunStore().save(run, current);
        JsonObject stored = codec.parseObject(new String(Files.readAllBytes(run), StandardCharsets.UTF_8));
        JsonObject scores = stored.getAsJsonArray("decisionTraces").get(1).getAsJsonObject()
            .getAsJsonObject("candidateScores");
        scores.addProperty("invented-version", 1.0);
        Files.write(run, codec.toJson(stored).getBytes(StandardCharsets.UTF_8));
        Assertions.assertThrows(TemporalValidationException.class, () -> new TemporalRunStore().load(run));
    }

    @Test
    public void testInvalidCandidateOptionsNeverCreateOutput() throws Exception {
        Path output = directory.resolve("must-not-exist.json");
        for (String[] options : Arrays.asList(new String[] {"--candidate-profile", "LEXICAL_FUSION"},
            new String[] {"--candidate-profile", "RULE_ALL", "--candidate-top-k", "0"},
            new String[] {"--candidate-profile", "RULE_ALL", "--candidate-top-k", "1.5"},
            new String[] {"--candidate-top-k", "1"})) {
            List<String> arguments = new java.util.ArrayList<>(Arrays.asList("replay", "--input", "unused",
                "--output", output.toString()));
            arguments.addAll(Arrays.asList(options));
            Assertions.assertEquals(2, invoke(new ByteArrayOutputStream(), arguments.toArray(new String[0])));
            Assertions.assertFalse(Files.exists(output));
        }
    }

    private Path input(List<NormalizedMemoryEvent> events) throws Exception {
        Path path = directory.resolve("input with spaces.jsonl");
        StringBuilder json = new StringBuilder();
        for (NormalizedMemoryEvent event : events) {
            json.append(codec.toJson(codec.eventToJson(event))).append('\n');
        }
        Files.write(path, json.toString().getBytes(StandardCharsets.UTF_8));
        return path;
    }

    private static List<NormalizedMemoryEvent> events() {
        return Arrays.asList(add("other", new FactKey("bob", "city", "profile"), "Rome", 5),
            add("first", KEY, "Paris", 10), add("conflict", KEY, "Berlin", 20),
            add("duplicate", KEY, "Paris", 30));
    }

    private static NormalizedMemoryEvent add(String id, FactKey key, String city, int time) {
        MemoryFact fact = MemoryFact.attribute("fact-" + id, new MemoryEntity(key.getSubjectId(), "person"),
            key.getPredicate(), city);
        return new EventNormalizer().normalize(MemoryEvent.add(id, fact,
            TimeInterval.unboundedFrom(BASE), BASE.plusSeconds(time), Collections.singletonList(
                new Evidence("e-" + id, new Source("registry", "Registry"),
                    "City registry records the current location"))), key);
    }

    private static int invoke(ByteArrayOutputStream errors, String... args) throws Exception {
        return new TemporalMemoryCli().run(args, new PrintStream(new ByteArrayOutputStream(), true, "UTF-8"),
            new PrintStream(errors, true, "UTF-8"));
    }
}
