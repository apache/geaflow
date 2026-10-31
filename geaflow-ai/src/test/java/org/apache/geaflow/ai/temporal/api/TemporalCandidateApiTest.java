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

package org.apache.geaflow.ai.temporal.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import org.apache.geaflow.ai.temporal.candidate.EvidenceCandidate;
import org.apache.geaflow.ai.temporal.candidate.RetrievalProfile;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalErrorCode;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TemporalCandidateApiTest {

    private static final Instant BASE = Instant.parse("2024-01-01T00:00:00Z");
    private static final FactKey KEY = new FactKey("alice", "city", "profile");

    @Test
    public void testReadOnlyStableResultIncludesRuleScoresAndEvidenceSources() throws Exception {
        TemporalMemoryFacade facade = populated();
        facade.query(QueryMode.CURRENT, KEY, at(50), null);
        CanonicalSnapshot before = facade.getSnapshot();
        JsonArray operations = facade.getOperations();
        int decisions = facade.getDecisionTraces().size();
        int queries = facade.getQueryTraces().size();

        CandidateResult result = facade.candidates(QueryMode.CURRENT, KEY, "renewable",
            at(50), null, 1);
        Assertions.assertEquals(2, result.getFacts().size());
        Assertions.assertTrue(result.getFacts().get(0).isMandatory());
        Assertions.assertEquals(KEY, result.getFacts().get(0).getFactKey());
        Assertions.assertEquals(1.0, result.getFacts().get(0).getScores().get("RULE"));
        Assertions.assertTrue(result.getFacts().get(0).getScores().get("BM25") > 0);
        Assertions.assertFalse(result.getFacts().get(1).isMandatory());
        Assertions.assertEquals(1, result.getEvidence().size());
        JsonObject encoded = result.toJson();
        JsonObject evidence = encoded.getAsJsonArray("evidence").get(0).getAsJsonObject();
        Assertions.assertEquals("registry", evidence.getAsJsonObject("evidence")
            .getAsJsonObject("source").get("id").getAsString());
        Assertions.assertTrue(evidence.getAsJsonObject("scores").get("BM25").getAsDouble() > 0);
        Assertions.assertEquals(1, evidence.getAsJsonArray("versionIds").size());
        Assertions.assertEquals(encoded, facade.candidates(QueryMode.CURRENT, KEY,
            "renewable", at(50), null, 1).toJson());
        encoded.remove("facts");
        Assertions.assertTrue(result.toJson().has("facts"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> result.getFacts().clear());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> result.getEvidence().clear());
        Assertions.assertSame(before, facade.getSnapshot());
        Assertions.assertEquals(operations, facade.getOperations());
        Assertions.assertEquals(decisions, facade.getDecisionTraces().size());
        Assertions.assertEquals(queries, facade.getQueryTraces().size());
    }

    @Test
    public void testScopeAndTimeSlicesExcludeInvisibleVersions() throws Exception {
        TemporalMemoryFacade facade = populated();
        CandidateResult past = facade.candidates(QueryMode.AS_OF, KEY, "renewable",
            at(50), at(14), 20);
        Assertions.assertEquals(1, past.getFacts().size());
        Assertions.assertEquals("e-public", past.getEvidence().get(0).getEvidence().getId());
        CandidateResult future = facade.candidates(QueryMode.AS_OF, KEY, "renewable",
            at(50), at(9), 20);
        Assertions.assertTrue(future.getFacts().isEmpty());
        Assertions.assertTrue(future.getEvidence().isEmpty());
        CandidateResult otherScope = facade.candidates(QueryMode.CURRENT,
            new FactKey("alice", "city", "missing"), "renewable", at(50), null, 20);
        Assertions.assertTrue(otherScope.getFacts().isEmpty());
        Assertions.assertTrue(otherScope.getEvidence().isEmpty());
        for (QueryMode mode : QueryMode.values()) {
            CandidateResult atEnd = facade.candidates(mode, KEY, "renewable", at(100),
                mode == QueryMode.AS_OF ? at(30) : null, 20);
            Assertions.assertTrue(atEnd.getFacts().isEmpty());
            Assertions.assertTrue(atEnd.getEvidence().isEmpty());
        }
        Assertions.assertTrue(facade.candidates(QueryMode.CURRENT, KEY, "secret", at(50),
            null, 20).getEvidence().isEmpty());
    }

    @Test
    public void testEmptyTextKeepsMandatoryFactsAndNoEvidenceHits() throws Exception {
        TemporalMemoryFacade facade = populated();
        CandidateResult result = facade.candidates(QueryMode.CURRENT, KEY, "", at(50), null, 20);
        Assertions.assertEquals(1, result.getFacts().size());
        Assertions.assertTrue(result.getFacts().get(0).isMandatory());
        Assertions.assertEquals(Collections.singletonMap("RULE", 1.0),
            result.getFacts().get(0).getScores());
        Assertions.assertTrue(result.getEvidence().isEmpty());
        Assertions.assertEquals("", result.toJson().get("text").getAsString());
    }

    @Test
    public void testInvalidRequestsDoNotChangeJournal() {
        TemporalMemoryFacade facade = populated();
        JsonArray before = facade.getOperations();
        Assertions.assertEquals(TemporalErrorCode.REJECT_INPUT, Assertions.assertThrows(
            TemporalValidationException.class,
            () -> facade.candidates(QueryMode.CURRENT, KEY, "wind", at(50), null, 0)).getCode());
        Assertions.assertThrows(TemporalValidationException.class,
            () -> facade.candidates(QueryMode.AS_OF, KEY, "wind", at(50), null, 20));
        Assertions.assertThrows(TemporalValidationException.class,
            () -> facade.candidates(QueryMode.CURRENT, KEY, "wind", at(50), at(10), 20));
        Assertions.assertThrows(TemporalValidationException.class,
            () -> facade.candidates(null, KEY, "wind", at(50), null, 20));
        Assertions.assertThrows(TemporalValidationException.class,
            () -> facade.candidates(QueryMode.CURRENT, null, "wind", at(50), null, 20));
        Assertions.assertThrows(TemporalValidationException.class,
            () -> facade.candidates(QueryMode.CURRENT, KEY, null, at(50), null, 20));
        Assertions.assertThrows(TemporalValidationException.class,
            () -> facade.candidates(QueryMode.CURRENT, KEY, "wind", null, null, 20));
        Assertions.assertEquals(before, facade.getOperations());
    }

    @Test
    public void testLexicalProfilesExposeRealScoresWithoutChangingMemory() throws Exception {
        TemporalMemoryFacade facade = populated();
        CanonicalSnapshot snapshot = facade.getSnapshot();
        JsonArray operations = facade.getOperations();
        final RetrievalProfile profile = RetrievalProfile.LEXICAL_FUSION;
        CandidateResult result = facade.candidates(QueryMode.CURRENT, KEY, "renewable",
            at(50), null, 20, profile);
        EvidenceCandidate candidate = result.getEvidence().get(0);
        JsonObject encoded = result.toJson().getAsJsonArray("evidence").get(0)
            .getAsJsonObject();
        Assertions.assertEquals(candidate.getScore(), encoded.get("score").getAsDouble());
        Assertions.assertEquals(candidate.getChannelScores().get("BM25").doubleValue(),
            encoded.getAsJsonObject("scores").get("BM25").getAsDouble());
        Assertions.assertTrue(encoded.getAsJsonObject("scores").has("ALIAS_BM25"));
        Assertions.assertTrue(encoded.getAsJsonObject("channelRanks").has("RRF"));
        Assertions.assertTrue(result.getFacts().get(0).isMandatory());
        Assertions.assertTrue(result.getFacts().get(0).getScores().containsKey("RRF"));
        Assertions.assertEquals(result.toJson(), facade.candidates(QueryMode.CURRENT, KEY,
            "renewable", at(50), null, 20, profile).toJson());
        JsonObject baseline = facade.candidates(QueryMode.CURRENT, KEY, "renewable",
            at(50), null, 20).toJson().getAsJsonArray("evidence").get(0).getAsJsonObject();
        Assertions.assertFalse(baseline.has("score"));
        Assertions.assertFalse(baseline.has("channelRanks"));
        Assertions.assertSame(snapshot, facade.getSnapshot());
        Assertions.assertEquals(operations, facade.getOperations());
    }

    private static TemporalMemoryFacade populated() {
        TemporalMemoryFacade facade = new TemporalMemoryFacade();
        facade.replay(Arrays.asList(add("public", KEY, "Beijing", "renewable wind", 10),
            add("other", new FactKey("bob", "city", "profile"), "Shanghai", "renewable solar", 15),
            add("private", new FactKey("alice", "city", "private"), "Rome", "secret renewable", 20)));
        return facade;
    }

    @Test
    public void testFourCandidateStrategiesExposeChannelsAndKeepSnapshotReadOnly() throws Exception {
        TemporalMemoryFacade facade = new TemporalMemoryFacade();
        String text = "renewable wind powers cities";
        facade.replay(Arrays.asList(add("visible", KEY, "Beijing", text, 10),
            add("hidden", new FactKey("alice", "city", "private"), "Rome", text, 20)));
        CanonicalSnapshot snapshot = facade.getSnapshot();
        JsonArray operations = facade.getOperations();
        for (RetrievalProfile profile : Arrays.asList(RetrievalProfile.RULE_ONLY,
            RetrievalProfile.RULE_BM25, RetrievalProfile.RULE_MINHASH, RetrievalProfile.RULE_ALL)) {
            CandidateResult result = facade.candidates(QueryMode.CURRENT, KEY, text,
                at(50), null, 1, profile);
            Assertions.assertEquals(1, result.getFacts().size());
            Assertions.assertTrue(result.getFacts().get(0).isMandatory());
            Assertions.assertEquals(1, result.getEvidence().size());
            JsonObject encoded = result.toJson();
            JsonObject fact = encoded.getAsJsonArray("facts").get(0).getAsJsonObject();
            JsonObject evidence = encoded.getAsJsonArray("evidence").get(0).getAsJsonObject();
            Assertions.assertTrue(fact.has("score"));
            Assertions.assertTrue(fact.getAsJsonObject("channelRanks").has("RULE"));
            Assertions.assertTrue(evidence.getAsJsonObject("scores").has("RULE"));
            Assertions.assertEquals(profile.usesBm25(), evidence.getAsJsonObject("scores").has("BM25"));
            Assertions.assertEquals(profile.usesMinHash(), evidence.getAsJsonObject("scores").has("MINHASH"));
            Assertions.assertEquals("e-visible", result.getEvidence().get(0).getEvidence().getId());
            Assertions.assertEquals(1, result.getEvidence().get(0).getVersionIds().size());
            Assertions.assertTrue(facade.candidates(QueryMode.AS_OF, KEY, text, at(50),
                at(9), 1, profile).getEvidence().isEmpty());
            Assertions.assertSame(snapshot, facade.getSnapshot());
            Assertions.assertEquals(operations, facade.getOperations());
        }
        CandidateResult punctuation = facade.candidates(QueryMode.CURRENT, KEY,
            "RENEWABLE, wind powers cities!", at(50), null, 1, RetrievalProfile.RULE_MINHASH);
        Assertions.assertEquals(1, punctuation.getEvidence().size());
        Assertions.assertEquals(1.0, punctuation.getEvidence().get(0).getChannelScores().get("MINHASH"));
        Assertions.assertFalse(punctuation.getEvidence().get(0).getChannelScores().containsKey("RULE"));
    }

    private static NormalizedMemoryEvent add(String id, FactKey key, String city, String text,
                                              long recordedAt) {
        MemoryFact fact = MemoryFact.attribute("fact-" + id,
            new MemoryEntity(key.getSubjectId(), "person"), key.getPredicate(), city);
        return new EventNormalizer().normalize(MemoryEvent.add(id, fact,
            new TimeInterval(BASE, at(100)), at(recordedAt), Collections.singletonList(
                new Evidence("e-" + id, new Source("registry", "Registry"), text))), key);
    }

    private static Instant at(long nanos) {
        return BASE.plusNanos(nanos);
    }
}
