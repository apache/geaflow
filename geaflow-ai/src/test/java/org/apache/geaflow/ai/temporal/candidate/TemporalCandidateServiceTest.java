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

package org.apache.geaflow.ai.temporal.candidate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.temporal.api.QueryMode;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TemporalCandidateServiceTest {

    private static final Instant BASE = Instant.parse("2024-01-01T00:00:00Z");
    private static final FactKey KEY = new FactKey("person:alice", "city", "profile");
    private final TemporalCandidateService service = new TemporalCandidateService();

    @Test
    public void testMandatoryCandidatesAreCompleteAndSoftCandidatesHaveOwnLimit() throws Exception {
        List<MemoryFactVersion> mandatory = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            mandatory.add(version(String.format("v-%02d", i), KEY, open(0), open(0),
                MemoryFactVersionStatus.ACTIVE, evidence("e-" + i, "Shanghai city")));
        }
        Map<FactKey, List<MemoryFactVersion>> groups = new TreeMap<>();
        groups.put(KEY, mandatory);
        groups.put(key("bob", "profile"), Arrays.asList(
            active("soft-b", key("bob", "profile"), "Shanghai city"),
            active("soft-a", key("bob", "profile"), "Shanghai city")));
        CanonicalSnapshot snapshot = snapshot(groups);

        List<FactCandidate> facts = service.facts(snapshot, QueryMode.CURRENT,
            KEY, at(0), null, "Shanghai", 1);
        Assertions.assertEquals(31, facts.size());
        for (int i = 0; i < 30; i++) {
            Assertions.assertTrue(facts.get(i).isMandatory());
            Assertions.assertEquals(KEY, facts.get(i).getFactKey());
            Assertions.assertEquals(1.0, facts.get(i).getScores().get("RULE"));
            Assertions.assertTrue(facts.get(i).getScores().get("BM25") > 0);
            Assertions.assertEquals(i + 1, facts.get(i).getRank());
        }
        Assertions.assertEquals("soft-a", facts.get(30).getVersion().getId());
        Assertions.assertFalse(facts.get(30).isMandatory());
        Assertions.assertEquals(Collections.singleton("BM25"),
            facts.get(30).getScores().keySet());
        Assertions.assertEquals(30, service.facts(snapshot, QueryMode.CURRENT,
            KEY, at(0), null, "", 1).size());
    }

    @Test
    public void testHalfOpenVisibilityAndTombstonesFollowQueryModes() throws Exception {
        MemoryFactVersion old = version("old", KEY, interval(10, 20), interval(0, 30),
            MemoryFactVersionStatus.ACTIVE, evidence("old-e", "Shanghai"));
        MemoryFactVersion tombstone = version("tombstone", KEY, interval(10, 20), open(30),
            MemoryFactVersionStatus.RETRACTED, evidence("retract-e", "Shanghai withdrawn"));
        CanonicalSnapshot snapshot = snapshot(Collections.singletonMap(KEY,
            Arrays.asList(tombstone, old)));

        Assertions.assertEquals(Collections.singletonList("old"), factIds(service.facts(
            snapshot, QueryMode.AS_OF, KEY, at(10), at(29), "Shanghai", 20)));
        Assertions.assertTrue(service.facts(snapshot, QueryMode.AS_OF,
            KEY, at(10), at(30), "Shanghai", 20).isEmpty());
        Assertions.assertTrue(service.facts(snapshot, QueryMode.CURRENT,
            KEY, at(10), null, "Shanghai", 20).isEmpty());
        Assertions.assertEquals(Arrays.asList("old", "tombstone"), factIds(service.facts(
            snapshot, QueryMode.HISTORICAL, KEY, at(19), null, "Shanghai", 20)));
        Assertions.assertTrue(service.facts(snapshot, QueryMode.HISTORICAL,
            KEY, at(20), null, "Shanghai", 20).isEmpty());
        Assertions.assertTrue(service.facts(snapshot, QueryMode.AS_OF,
            KEY, at(9), at(29), "Shanghai", 20).isEmpty());
        Assertions.assertEquals(Collections.singletonList("old-e"), evidenceIds(service.evidence(
            snapshot, QueryMode.AS_OF, KEY, at(10), at(29), "Shanghai", 20)));
        Assertions.assertTrue(service.evidence(snapshot, QueryMode.CURRENT,
            KEY, at(10), null, "Shanghai", 20).isEmpty());
        Assertions.assertEquals(2, service.evidence(snapshot, QueryMode.HISTORICAL,
            KEY, at(10), null, "Shanghai", 20).size());
    }

    @Test
    public void testMandatoryRankingUsesBm25ThenVersionIdAndBlankUsesVersionId() throws Exception {
        CanonicalSnapshot snapshot = snapshot(Collections.singletonMap(KEY, Arrays.asList(
            active("a-low", KEY, "Beijing"), active("z-high", KEY, "Shanghai Shanghai"))));

        List<FactCandidate> ranked = service.facts(snapshot, QueryMode.CURRENT,
            KEY, at(0), null, "Shanghai", 1);
        Assertions.assertEquals(Arrays.asList("z-high", "a-low"), factIds(ranked));
        Assertions.assertTrue(ranked.get(0).getScores().get("BM25")
            > ranked.get(1).getScores().get("BM25"));
        Assertions.assertEquals(Arrays.asList("a-low", "z-high"), factIds(service.facts(
            snapshot, QueryMode.CURRENT, KEY, at(0), null, "", 1)));
    }

    @Test
    public void testScopeIsExcludedFromBothCandidatesAndCorpusStatistics() throws Exception {
        CanonicalSnapshot baseline = snapshot(Collections.singletonMap(KEY,
            Collections.singletonList(active("v", KEY, "Shanghai city"))));
        Map<FactKey, List<MemoryFactVersion>> groups = new TreeMap<>(
            baseline.getState().getVersionsByFactKey());
        FactKey hidden = key("alice", "secret");
        groups.put(hidden, Collections.singletonList(active("secret-v", hidden,
            "Shanghai Shanghai Shanghai city confidential")));
        CanonicalSnapshot withSecret = snapshot(groups);

        List<EvidenceCandidate> expected = service.evidence(baseline, QueryMode.CURRENT,
            KEY, at(1), null, "Shanghai", 20);
        List<EvidenceCandidate> actual = service.evidence(withSecret, QueryMode.CURRENT,
            KEY, at(1), null, "Shanghai", 20);
        Assertions.assertEquals(evidenceIds(expected), evidenceIds(actual));
        Assertions.assertEquals(expected.get(0).getScore(), actual.get(0).getScore());
        List<FactCandidate> facts = service.facts(withSecret, QueryMode.CURRENT,
            KEY, at(1), null, "Shanghai", 20);
        Assertions.assertEquals(Collections.singletonList("v"), factIds(facts));
        Assertions.assertEquals(service.facts(baseline, QueryMode.CURRENT,
            KEY, at(1), null, "Shanghai", 20).get(0).getScores(), facts.get(0).getScores());
    }

    @Test
    public void testFutureEvidenceAndCorpusCannotChangePastRankingOrScores() throws Exception {
        Evidence original = evidence("e-original", "Shanghai city");
        MemoryFactVersion initial = version("old", KEY, open(0), open(0),
            MemoryFactVersionStatus.ACTIVE, original);
        CanonicalSnapshot before = snapshot(Collections.singletonMap(KEY,
            Collections.singletonList(initial)));
        MemoryFactVersion old = version("old", KEY, open(0), interval(0, 10),
            MemoryFactVersionStatus.ACTIVE, original);
        MemoryFactVersion updated = version("new", KEY, open(0), open(10),
            MemoryFactVersionStatus.ACTIVE, original, evidence("e-future", "Shanghai confirmed"));
        Map<FactKey, List<MemoryFactVersion>> groups = new TreeMap<>();
        groups.put(KEY, Arrays.asList(updated, old));
        FactKey futureKey = key("future", "profile");
        groups.put(futureKey, Collections.singletonList(version("future", futureKey,
            open(0), open(10), MemoryFactVersionStatus.ACTIVE,
            evidence("future-corpus", "Shanghai Shanghai Shanghai"))));
        CanonicalSnapshot after = snapshot(groups);

        List<EvidenceCandidate> expected = service.evidence(before, QueryMode.AS_OF,
            KEY, at(5), at(5), "Shanghai", 20);
        List<EvidenceCandidate> actual = service.evidence(after, QueryMode.AS_OF,
            KEY, at(5), at(5), "Shanghai", 20);
        Assertions.assertEquals(Collections.singletonList("e-original"), evidenceIds(actual));
        Assertions.assertEquals(expected.get(0).getScore(), actual.get(0).getScore());
        Assertions.assertEquals(Collections.singletonList("old"), actual.get(0).getVersionIds());
        List<FactCandidate> facts = service.facts(after, QueryMode.AS_OF,
            KEY, at(5), at(5), "Shanghai", 20);
        Assertions.assertEquals(Collections.singletonList("old"), factIds(facts));
        Assertions.assertEquals(service.facts(before, QueryMode.AS_OF,
            KEY, at(5), at(5), "Shanghai", 20).get(0).getScores(), facts.get(0).getScores());
    }

    @Test
    public void testEvidenceDeduplicatesVisibleReferencesAndStableTies() throws Exception {
        Evidence shared = evidence("e-a", "Shanghai city");
        MemoryFactVersion first = version("v-z", KEY, open(0), open(0),
            MemoryFactVersionStatus.ACTIVE, shared, evidence("e-z", "Shanghai city"));
        FactKey other = key("bob", "profile");
        MemoryFactVersion second = version("v-a", other, open(0), open(0),
            MemoryFactVersionStatus.ACTIVE, shared);
        Map<FactKey, List<MemoryFactVersion>> groups = new TreeMap<>();
        groups.put(other, Collections.singletonList(second));
        groups.put(KEY, Collections.singletonList(first));
        CanonicalSnapshot snapshot = snapshot(groups);

        List<EvidenceCandidate> candidates = service.evidence(snapshot, QueryMode.CURRENT,
            KEY, at(1), null, "Shanghai", 20);
        Assertions.assertEquals(Arrays.asList("e-a", "e-z"), evidenceIds(candidates));
        Assertions.assertEquals(Arrays.asList("v-a", "v-z"), candidates.get(0).getVersionIds());
        Assertions.assertEquals(1, candidates.get(0).getRank());
        Assertions.assertEquals(2, candidates.get(1).getRank());
        Assertions.assertEquals(candidates.get(0).getScore(), candidates.get(1).getScore());
        Assertions.assertEquals(Collections.singletonList("v-z"),
            candidates.get(1).getVersionIds());
    }

    @Test
    public void testEventCandidatesUseIntervalOverlapRatherThanOnlyStartPoint() {
        MemoryFactVersion first = version("first", KEY, interval(0, 10), open(0),
            MemoryFactVersionStatus.ACTIVE, evidence("first-e", "first"));
        MemoryFactVersion later = version("later", KEY, interval(10, 20), open(0),
            MemoryFactVersionStatus.ACTIVE, evidence("later-e", "later"));
        MemoryFactVersion boundary = version("boundary", KEY, interval(20, 30), open(0),
            MemoryFactVersionStatus.ACTIVE, evidence("boundary-e", "boundary"));
        MemoryFactVersion future = version("future", KEY, open(0), open(30),
            MemoryFactVersionStatus.ACTIVE, evidence("future-e", "future"));
        MemoryFactVersion tombstone = version("tombstone", KEY, open(0), open(0),
            MemoryFactVersionStatus.RETRACTED, evidence("tombstone-e", "withdrawn"));
        CanonicalSnapshot snapshot = snapshot(Collections.singletonMap(KEY,
            Arrays.asList(first, later, boundary, future, tombstone)));
        MemoryEvent event = MemoryEvent.add("incoming", first.getFact(), interval(5, 20),
            at(1), first.getEvidence());

        Assertions.assertEquals(Arrays.asList("first", "later"), factIds(service.eventFacts(
            snapshot, new EventNormalizer().normalize(event, KEY))));
    }

    @Test
    public void testInvalidInputsAndEvidenceCollisionDoNotChangeSnapshot() throws Exception {
        MemoryFactVersion first = active("first", KEY, "Shanghai city");
        CanonicalSnapshot good = snapshot(Collections.singletonMap(KEY,
            Collections.singletonList(first)));
        int originalHash = good.hashCode();
        Assertions.assertThrows(IllegalArgumentException.class, () -> service.facts(
            good, QueryMode.CURRENT, KEY, at(0), null, "Shanghai", 0));
        Assertions.assertThrows(NullPointerException.class, () -> service.facts(
            good, QueryMode.AS_OF, KEY, at(0), null, "Shanghai", 20));
        Assertions.assertThrows(IllegalArgumentException.class, () -> service.evidence(
            good, QueryMode.CURRENT, KEY, at(0), at(0), "Shanghai", 20));
        Assertions.assertEquals(originalHash, good.hashCode());
        Assertions.assertEquals(Collections.singletonList(first), good.getState().getVersions());

        MemoryFactVersion left = version("left", KEY, open(0), open(0),
            MemoryFactVersionStatus.ACTIVE, evidence("same-id", "Shanghai"));
        MemoryFactVersion right = version("right", KEY, open(0), open(0),
            MemoryFactVersionStatus.ACTIVE, evidence("same-id", "Beijing"));
        CanonicalSnapshot collision = snapshot(Collections.singletonMap(KEY,
            Arrays.asList(left, right)));
        int collisionHash = collision.hashCode();
        Assertions.assertThrows(IllegalArgumentException.class, () -> service.evidence(
            collision, QueryMode.CURRENT, KEY, at(0), null, "Shanghai", 20));
        Assertions.assertEquals(collisionHash, collision.hashCode());
        Assertions.assertEquals(Arrays.asList(left, right), collision.getState().getVersions());
    }

    @Test
    public void testCandidateAndResultCollectionsAreImmutable() throws Exception {
        MemoryFactVersion version = active("v", KEY, "Shanghai");
        Map<String, Double> scores = new HashMap<>();
        scores.put("RULE", 1.0);
        FactCandidate fact = new FactCandidate(KEY, version, true, scores, 1);
        scores.clear();
        Assertions.assertEquals(1.0, fact.getScores().get("RULE"));
        Assertions.assertThrows(UnsupportedOperationException.class,
            () -> fact.getScores().clear());
        List<String> ids = new ArrayList<>(Arrays.asList("v-z", "v-a", "v-a"));
        EvidenceCandidate evidence = new EvidenceCandidate(version.getEvidence().get(0), ids, 1, 1);
        ids.clear();
        Assertions.assertEquals(Arrays.asList("v-a", "v-z"), evidence.getVersionIds());
        Assertions.assertThrows(UnsupportedOperationException.class,
            () -> evidence.getVersionIds().clear());
        Map<String, Double> channelScores = new HashMap<>();
        channelScores.put("ALIAS_BM25", 2.0);
        Map<String, Integer> channelRanks = new HashMap<>();
        channelRanks.put("ALIAS_BM25", 1);
        EvidenceCandidate traced = new EvidenceCandidate(evidence.getEvidence(), ids, 0.02, 1,
            channelScores, channelRanks);
        channelScores.clear();
        channelRanks.clear();
        Assertions.assertEquals(Collections.singletonMap("ALIAS_BM25", 2.0),
            traced.getChannelScores());
        Assertions.assertEquals(Collections.singletonMap("ALIAS_BM25", 1),
            traced.getChannelRanks());
        Assertions.assertThrows(UnsupportedOperationException.class,
            () -> traced.getChannelScores().clear());
        Assertions.assertThrows(UnsupportedOperationException.class,
            () -> traced.getChannelRanks().clear());
        Assertions.assertThrows(IllegalArgumentException.class, () -> new EvidenceCandidate(
            evidence.getEvidence(), ids, 0.02, 1, Collections.emptyMap(),
            Collections.singletonMap("missing", 1)));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new EvidenceCandidate(evidence.getEvidence(), ids, Double.NaN, 1));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new FactCandidate(KEY, version, true, Collections.emptyMap(), 0));
        CanonicalSnapshot snapshot = snapshot(Collections.singletonMap(KEY,
            Collections.singletonList(version)));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> service.facts(
            snapshot, QueryMode.CURRENT, KEY, at(0), null, "Shanghai", 20).clear());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> service.evidence(
            snapshot, QueryMode.CURRENT, KEY, at(0), null, "Shanghai", 20).clear());
    }

    @Test
    public void testLexicalProfilesKeepMandatoryFactsBeyondPoolAndOnlyVisibleEvidence()
        throws Exception {
        List<MemoryFactVersion> mandatory = new ArrayList<>();
        for (int i = 0; i < 105; i++) {
            mandatory.add(active(String.format("v-%03d", i), KEY, "carbon dioxide atmosphere"));
        }
        Map<FactKey, List<MemoryFactVersion>> visible = new TreeMap<>();
        visible.put(KEY, mandatory);
        FactKey other = key("bob", "profile");
        visible.put(other, Collections.singletonList(active("soft", other, "carbon dioxide")));
        CanonicalSnapshot before = snapshot(visible);
        Map<FactKey, List<MemoryFactVersion>> all = new TreeMap<>(visible);
        FactKey hidden = key("alice", "private");
        all.put(hidden, Collections.singletonList(active("secret", hidden, "CO2 secret")));
        FactKey future = key("future", "profile");
        all.put(future, Collections.singletonList(version("future", future, open(0), open(10),
            MemoryFactVersionStatus.ACTIVE, evidence("e-future", "CO2 future"))));
        CanonicalSnapshot after = snapshot(all);
        final RetrievalProfile profile = RetrievalProfile.LEXICAL_FUSION;
        TemporalCandidateService lexical = new TemporalCandidateService(profile);
        List<FactCandidate> facts = lexical.facts(after, QueryMode.AS_OF,
            KEY, at(5), at(5), "CO2", 1);
        Assertions.assertEquals(106, facts.size());
        for (int i = 0; i < mandatory.size(); i++) {
            Assertions.assertTrue(facts.get(i).isMandatory());
            Assertions.assertEquals(KEY, facts.get(i).getFactKey());
            Assertions.assertEquals(1.0, facts.get(i).getScores().get("RULE"));
        }
        Assertions.assertEquals("soft", facts.get(105).getVersion().getId());
        List<EvidenceCandidate> expected = lexical.evidence(before, QueryMode.AS_OF,
            KEY, at(5), at(5), "CO2", 20);
        List<EvidenceCandidate> actual = lexical.evidence(after, QueryMode.AS_OF,
            KEY, at(5), at(5), "CO2", 20);
        Assertions.assertEquals(evidenceIds(expected), evidenceIds(actual));
        for (int i = 0; i < expected.size(); i++) {
            Assertions.assertEquals(expected.get(i).getScore(), actual.get(i).getScore());
            Assertions.assertEquals(expected.get(i).getChannelScores(),
                actual.get(i).getChannelScores());
            Assertions.assertEquals(expected.get(i).getChannelRanks(),
                actual.get(i).getChannelRanks());
            Assertions.assertEquals(expected.get(i).getVersionIds(),
                actual.get(i).getVersionIds());
        }
        Assertions.assertEquals(105, lexical.facts(after, QueryMode.AS_OF,
            KEY, at(5), at(5), "", 1).size());
        Assertions.assertTrue(lexical.evidence(after, QueryMode.AS_OF,
            KEY, at(5), at(5), "", 20).isEmpty());
    }

    @Test
    public void testAllCandidateStrategiesKeepMandatoryCompleteAndHonorTemporalScope()
        throws Exception {
        final String text = "the earth climate temperature is rising across the planet each year";
        final List<MemoryFactVersion> mandatory = new ArrayList<>();
        for (int i = 0; i < 105; i++) {
            mandatory.add(active(String.format("v-%03d", i), KEY, text));
        }
        final Map<FactKey, List<MemoryFactVersion>> groups = new TreeMap<>();
        groups.put(KEY, mandatory);
        FactKey soft = key("bob", "profile");
        groups.put(soft, Collections.singletonList(active("soft", soft, text)));
        final CanonicalSnapshot before = snapshot(groups);
        final Map<FactKey, List<MemoryFactVersion>> withHidden = new TreeMap<>(groups);
        FactKey secret = key("alice", "secret");
        withHidden.put(secret, Collections.singletonList(active("secret", secret, text)));
        FactKey future = key("future", "profile");
        withHidden.put(future, Collections.singletonList(version("future", future, open(0),
            open(10), MemoryFactVersionStatus.ACTIVE, evidence("future-e", text))));
        final CanonicalSnapshot after = snapshot(withHidden);
        final int snapshotHash = after.hashCode();
        for (RetrievalProfile profile : Arrays.asList(RetrievalProfile.RULE_ONLY,
            RetrievalProfile.RULE_BM25, RetrievalProfile.RULE_MINHASH, RetrievalProfile.RULE_ALL)) {
            TemporalCandidateService strategy = new TemporalCandidateService(profile);
            List<FactCandidate> expectedFacts = strategy.facts(before, QueryMode.AS_OF,
                KEY, at(5), at(5), text, 1);
            List<FactCandidate> actualFacts = strategy.facts(after, QueryMode.AS_OF,
                KEY, at(5), at(5), text, 1);
            Assertions.assertEquals(factIds(expectedFacts), factIds(actualFacts));
            Assertions.assertTrue(actualFacts.size() >= 105 && actualFacts.size() <= 106);
            for (int i = 0; i < 105; i++) {
                FactCandidate fact = actualFacts.get(i);
                Assertions.assertTrue(fact.isMandatory());
                Assertions.assertEquals(KEY, fact.getFactKey());
                Assertions.assertEquals(1.0, fact.getScores().get("RULE"));
                Assertions.assertTrue(fact.getChannelRanks().containsKey("RULE"));
                Assertions.assertEquals(fact.getScore(), fact.getScores().get("RRF"));
                Assertions.assertEquals(expectedFacts.get(i).getScores(), fact.getScores());
                Assertions.assertEquals(expectedFacts.get(i).getChannelRanks(),
                    fact.getChannelRanks());
            }
            if (profile.usesBm25()) {
                Assertions.assertEquals(106, actualFacts.size());
                Assertions.assertEquals("soft", actualFacts.get(105).getVersion().getId());
                Assertions.assertFalse(actualFacts.get(105).isMandatory());
            } else if (profile == RetrievalProfile.RULE_ONLY) {
                Assertions.assertEquals(105, actualFacts.size());
            }
            Assertions.assertEquals(105, strategy.facts(after, QueryMode.AS_OF,
                KEY, at(5), at(5), "", 1).size());
            List<EvidenceCandidate> expected = strategy.evidence(before, QueryMode.AS_OF,
                KEY, at(5), at(5), text, 20);
            List<EvidenceCandidate> actual = strategy.evidence(after, QueryMode.AS_OF,
                KEY, at(5), at(5), text, 20);
            Assertions.assertEquals(evidenceIds(expected), evidenceIds(actual));
            Assertions.assertEquals(20, actual.size());
            for (int i = 0; i < expected.size(); i++) {
                Assertions.assertEquals(expected.get(i).getChannelScores(),
                    actual.get(i).getChannelScores());
                Assertions.assertEquals(expected.get(i).getChannelRanks(),
                    actual.get(i).getChannelRanks());
                Assertions.assertEquals(expected.get(i).getVersionIds(), actual.get(i).getVersionIds());
            }
        }
        Assertions.assertEquals(snapshotHash, after.hashCode());
        Assertions.assertEquals(108, after.getEvents().size());
        Assertions.assertTrue(after.getState().getRelations().isEmpty());
    }

    @Test
    public void testFactChannelRankValidationAndImmutableTrace() {
        final MemoryFactVersion version = active("v", KEY, "near duplicate");
        Map<String, Double> scores = new HashMap<>();
        scores.put("MINHASH", 0.75);
        Map<String, Integer> ranks = new HashMap<>();
        ranks.put("MINHASH", 2);
        FactCandidate candidate = new FactCandidate(KEY, version, false, 0.02, 1, scores, ranks);
        scores.clear();
        ranks.clear();
        Assertions.assertEquals(0.02, candidate.getScore());
        Assertions.assertEquals(Collections.singletonMap("MINHASH", 2), candidate.getChannelRanks());
        Assertions.assertThrows(UnsupportedOperationException.class,
            () -> candidate.getChannelRanks().clear());
        Assertions.assertThrows(IllegalArgumentException.class, () -> new FactCandidate(KEY,
            version, false, 0.02, 1, Collections.emptyMap(),
            Collections.singletonMap("MINHASH", 1)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new FactCandidate(KEY,
            version, false, Double.NaN, 1, Collections.emptyMap(), Collections.emptyMap()));
    }

    @Test
    public void testMinHashOnlyFindsSoftFactsAndPreservesVisibleEvidenceReferences() throws Exception {
        final String text = "the earth climate temperature is rising across the planet each year";
        final FactKey required = new FactKey("alice", "city", "profile");
        final FactKey other = new FactKey("bob", "city", "profile");
        final Evidence shared = evidence("shared", text);
        Map<FactKey, List<MemoryFactVersion>> groups = new TreeMap<>();
        groups.put(required, Collections.singletonList(version("required", required, open(0), open(0),
            MemoryFactVersionStatus.ACTIVE, shared)));
        groups.put(other, Collections.singletonList(version("other", other, open(0), open(0),
            MemoryFactVersionStatus.ACTIVE, shared)));
        final CanonicalSnapshot snapshot = snapshot(groups);
        final TemporalCandidateService minHash = new TemporalCandidateService(
            RetrievalProfile.RULE_MINHASH);
        List<FactCandidate> facts = minHash.facts(snapshot, QueryMode.CURRENT, required,
            at(1), null, text, 1);
        Assertions.assertEquals(Arrays.asList("required", "other"), factIds(facts));
        Assertions.assertEquals(1.0, facts.get(0).getScores().get("MINHASH"));
        Assertions.assertEquals(1.0, facts.get(1).getScores().get("MINHASH"));
        Assertions.assertFalse(facts.get(1).isMandatory());
        Assertions.assertFalse(facts.get(1).getScores().containsKey("BM25"));
        List<EvidenceCandidate> evidence = minHash.evidence(snapshot, QueryMode.CURRENT, required,
            at(1), null, text, 1);
        Assertions.assertEquals(Collections.singletonList("shared"), evidenceIds(evidence));
        Assertions.assertEquals(Arrays.asList("other", "required"), evidence.get(0).getVersionIds());
        Assertions.assertTrue(snapshot.getState().getRelations().isEmpty());
        Assertions.assertEquals(2, snapshot.getEvents().size());
    }

    private static CanonicalSnapshot snapshot(Map<FactKey, List<MemoryFactVersion>> groups) {
        List<NormalizedMemoryEvent> events = new ArrayList<>();
        Map<String, String> origins = new TreeMap<>();
        for (Map.Entry<FactKey, List<MemoryFactVersion>> entry : groups.entrySet()) {
            for (MemoryFactVersion version : entry.getValue()) {
                String eventId = "event-" + version.getId();
                MemoryEvent event = MemoryEvent.add(eventId, version.getFact(),
                    version.getValidTime(), version.getTransactionTime().getStart(),
                    version.getEvidence());
                events.add(new EventNormalizer().normalize(event, entry.getKey()));
                origins.put(version.getId(), eventId);
            }
        }
        return new CanonicalSnapshot(new TemporalState(groups, Collections.emptyList()),
            events, origins);
    }

    private static MemoryFactVersion active(String id, FactKey key, String content) {
        return version(id, key, open(0), open(0), MemoryFactVersionStatus.ACTIVE,
            evidence("e-" + id, content));
    }

    private static MemoryFactVersion version(String id, FactKey key, TimeInterval valid,
                                             TimeInterval system, MemoryFactVersionStatus status,
                                             Evidence... evidence) {
        MemoryFact fact = MemoryFact.attribute("fact-" + id,
            new MemoryEntity(key.getSubjectId(), "person"), key.getPredicate(), "Shanghai");
        return new MemoryFactVersion(id, fact, status, valid, system, Arrays.asList(evidence));
    }

    private static Evidence evidence(String id, String content) {
        return new Evidence(id, new Source("fixture", "test source"), content);
    }

    private static FactKey key(String name, String scope) {
        return new FactKey("person:" + name, "city", scope);
    }

    private static Instant at(long nanos) {
        return BASE.plusNanos(nanos);
    }

    private static TimeInterval interval(long start, long end) {
        return new TimeInterval(at(start), at(end));
    }

    private static TimeInterval open(long start) {
        return TimeInterval.unboundedFrom(at(start));
    }

    private static List<String> factIds(List<FactCandidate> facts) {
        return facts.stream().map(candidate -> candidate.getVersion().getId())
            .collect(Collectors.toList());
    }

    private static List<String> evidenceIds(List<EvidenceCandidate> evidence) {
        return evidence.stream().map(candidate -> candidate.getEvidence().getId())
            .collect(Collectors.toList());
    }
}
