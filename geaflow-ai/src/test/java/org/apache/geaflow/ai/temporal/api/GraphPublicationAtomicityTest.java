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

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.ai.temporal.adapter.MemoryGraphAdapter;
import org.apache.geaflow.ai.temporal.candidate.RetrievalProfile;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.model.VersionRelation;
import org.apache.geaflow.ai.temporal.model.VersionRelationType;
import org.apache.geaflow.ai.temporal.oracle.FullReplayOracle;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalErrorCode;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** A graph identity collision cannot publish staged events or consume their ledger IDs. */
public class GraphPublicationAtomicityTest {

    private static final FactKey KEY = new FactKey("alice", "city", "profile");
    private static final Source SOURCE = new Source("source", "Registry");
    private static final Instant VALID_FROM = Instant.parse("2024-01-01T00:00:00Z");

    @Test
    public void testConflictingEvidenceIdRollsBackWholeBatchAndLedger() {
        assertPublicationRollback(new Evidence("proof-base", SOURCE, "Changed content"),
            new Evidence("proof-repaired", SOURCE, "Changed content"));
    }

    @Test
    public void testConflictingSourceIdRollsBackWholeBatchAndLedger() {
        assertPublicationRollback(new Evidence("proof-second",
            new Source("source", "Different registry name"), "Second evidence"),
            new Evidence("proof-second", SOURCE, "Second evidence"));
    }

    @Test
    public void testEveryCandidateStrategyRollsBackGraphAndLedgerBeforeRetry() {
        for (RetrievalProfile profile : candidateProfiles()) {
            assertPublicationRollback(new Evidence("proof-base", SOURCE, "Changed content"),
                new Evidence("proof-repaired", SOURCE, "Changed content"), profile);
            assertPublicationRollback(new Evidence("proof-second",
                new Source("source", "Different registry name"), "Second evidence"),
                new Evidence("proof-second", SOURCE, "Second evidence"), profile);
        }
    }

    @Test
    public void testOutOfOrderReplayPublishesResidualConflictWithoutCrossScopeEdges() {
        final TimeInterval whole = interval("2024-01-01T00:00:00Z", "2024-10-01T00:00:00Z");
        final NormalizedMemoryEvent first = assertion("first", "Paris", "2024-02-01T00:00:00Z",
            new Evidence("first-proof", SOURCE, "Paris"), KEY, whole);
        final NormalizedMemoryEvent conflict = assertion("conflict", "Rome", "2024-03-01T00:00:00Z",
            new Evidence("conflict-proof", SOURCE, "Rome"), KEY,
            interval("2024-05-01T00:00:00Z", "2024-12-01T00:00:00Z"));
        final String target = versionId(new FullReplayOracle().replayToSnapshot(
            Collections.singletonList(first)), "first", "Paris");
        final TimeInterval corrected = interval("2024-01-01T00:00:00Z", "2024-05-01T00:00:00Z");
        final NormalizedMemoryEvent correction = new EventNormalizer().normalize(MemoryEvent.correct(
            "correction", MemoryFact.attribute("fact-correction", new MemoryEntity("alice", "person"),
                "city", "Berlin"), corrected, Instant.parse("2024-04-01T00:00:00Z"),
            Collections.singletonList(new Evidence("correction-proof", SOURCE, "Berlin")))
            .targeting(target), KEY);
        NormalizedMemoryEvent otherScope = assertion("other-scope", "Tokyo", "2024-03-01T00:00:00Z",
            new Evidence("other-proof", SOURCE, "Tokyo"), new FactKey("alice", "city", "travel"), whole);
        final List<NormalizedMemoryEvent> shuffled = Arrays.asList(conflict, correction, otherScope, first);
        final CanonicalSnapshot expected = new FullReplayOracle().replayToSnapshot(shuffled);

        for (RetrievalProfile profile : candidateProfiles()) {
            TemporalMemoryFacade facade = new TemporalMemoryFacade();
            CanonicalSnapshot actual = facade.replay(shuffled, profile, 1).getSnapshot();
            Assertions.assertEquals(expected, actual, profile.name());
            final String residual = versionId(actual, "correction", "Paris");
            final String replacement = versionId(actual, "correction", "Berlin");
            final String conflicting = versionId(actual, "conflict", "Rome");
            Set<VersionRelation> gold = new HashSet<>(Arrays.asList(
                new VersionRelation(VersionRelationType.CONFLICTS_WITH, target, conflicting),
                new VersionRelation(VersionRelationType.CONFLICTS_WITH, residual, conflicting),
                new VersionRelation(VersionRelationType.SUPERSEDES, residual, target),
                new VersionRelation(VersionRelationType.SUPERSEDES, replacement, target)));
            Assertions.assertEquals(gold, new HashSet<>(actual.getState().getRelations()), profile.name());
            assertPublishedRelations(actual);
        }
    }

    private static void assertPublicationRollback(Evidence invalidEvidence, Evidence repairedEvidence) {
        assertPublicationRollback(invalidEvidence, repairedEvidence, null);
    }

    private static void assertPublicationRollback(Evidence invalidEvidence, Evidence repairedEvidence,
                                                  RetrievalProfile profile) {
        TemporalMemoryFacade facade = new TemporalMemoryFacade();
        NormalizedMemoryEvent base = assertion("base", "Paris", "2024-02-01T00:00:00Z",
            new Evidence("proof-base", SOURCE, "Original content"));
        apply(facade, Collections.singletonList(base), profile);
        final CanonicalSnapshot committed = facade.getSnapshot();
        NormalizedMemoryEvent stagedGood = assertion("staged-good", "Rome", "2024-03-01T00:00:00Z",
            new Evidence("proof-good", SOURCE, "Good evidence"));
        NormalizedMemoryEvent stagedBad = assertion("staged-second", "Berlin",
            "2024-04-01T00:00:00Z", invalidEvidence);

        TemporalValidationException failure = Assertions.assertThrows(TemporalValidationException.class,
            () -> apply(facade, Arrays.asList(stagedBad, stagedGood), profile));
        Assertions.assertEquals(TemporalErrorCode.REJECT_REFERENCE_INTEGRITY, failure.getCode());
        Assertions.assertEquals(committed, facade.getSnapshot());
        Assertions.assertEquals(Collections.singletonList(base), facade.getSnapshot().getEvents());

        NormalizedMemoryEvent repaired = assertion("staged-second", "Berlin",
            "2024-04-01T00:00:00Z", repairedEvidence);
        ReplayResult retry = apply(facade, Arrays.asList(repaired, stagedGood), profile);
        Assertions.assertEquals(Integer.valueOf(2), retry.getStatistics().get("acceptedCount"));
        Assertions.assertEquals(Integer.valueOf(0), retry.getStatistics().get("duplicateCount"));
        Assertions.assertEquals(3, retry.getSnapshot().getEvents().size());
        Assertions.assertEquals("ACCEPTED", retry.getDecisionTraces().get(0).getCode());
        Assertions.assertEquals("ACCEPTED", retry.getDecisionTraces().get(1).getCode());
        final CanonicalSnapshot retried = retry.getSnapshot();
        final String baseId = versionId(retried, "base", "Paris");
        final String goodId = versionId(retried, "staged-good", "Rome");
        final String repairedId = versionId(retried, "staged-second", "Berlin");
        Assertions.assertEquals(new HashSet<>(Arrays.asList(
            new VersionRelation(VersionRelationType.CONFLICTS_WITH, baseId, goodId),
            new VersionRelation(VersionRelationType.CONFLICTS_WITH, baseId, repairedId),
            new VersionRelation(VersionRelationType.CONFLICTS_WITH, goodId, repairedId))),
            new HashSet<>(retried.getState().getRelations()));
        assertPublishedRelations(retried);
        Assertions.assertTrue(facade.validateInvariants().isValid());
    }

    private static ReplayResult apply(TemporalMemoryFacade facade, List<NormalizedMemoryEvent> events,
                                       RetrievalProfile profile) {
        return profile == null ? facade.applyBatch(events) : facade.applyBatch(events, profile, 1);
    }

    private static List<RetrievalProfile> candidateProfiles() {
        return Arrays.asList(RetrievalProfile.RULE_ONLY, RetrievalProfile.RULE_BM25,
            RetrievalProfile.RULE_MINHASH, RetrievalProfile.RULE_ALL);
    }

    private static String versionId(CanonicalSnapshot snapshot, String eventId, String value) {
        return snapshot.getState().getVersions().stream()
            .filter(version -> eventId.equals(snapshot.getGeneratingEventIds().get(version.getId()))
                && value.equals(version.getFact().getLiteralValue().orElse(null)))
            .map(MemoryFactVersion::getId).findFirst().orElseThrow(AssertionError::new);
    }

    private static void assertPublishedRelations(CanonicalSnapshot snapshot) {
        final Map<String, FactKey> keys = new HashMap<>();
        final Map<String, MemoryFactVersion> versions = new HashMap<>();
        for (Map.Entry<FactKey, List<MemoryFactVersion>> entry
            : snapshot.getState().getVersionsByFactKey().entrySet()) {
            for (MemoryFactVersion version : entry.getValue()) {
                keys.put(version.getId(), entry.getKey());
                versions.put(version.getId(), version);
            }
        }
        List<VersionRelation> relations = snapshot.getState().getRelations();
        Assertions.assertEquals(relations.size(), new HashSet<>(relations).size());
        for (VersionRelation relation : relations) {
            MemoryFactVersion from = versions.get(relation.getFromVersionId());
            MemoryFactVersion to = versions.get(relation.getToVersionId());
            Assertions.assertNotNull(from);
            Assertions.assertNotNull(to);
            Assertions.assertEquals(keys.get(from.getId()), keys.get(to.getId()));
            if (relation.getType() == VersionRelationType.CONFLICTS_WITH) {
                Assertions.assertNotEquals(from.getFact().getLiteralValue(), to.getFact().getLiteralValue());
                Assertions.assertTrue(from.getValidTime().overlaps(to.getValidTime()));
            }
        }
        MemoryGraphAdapter adapter = new MemoryGraphAdapter();
        Assertions.assertEquals(snapshot, adapter.fromGraph(adapter.toGraph(snapshot)));
        Assertions.assertEquals(snapshot, new FullReplayOracle().replayToSnapshot(snapshot.getEvents()));
    }

    private static NormalizedMemoryEvent assertion(String id, String value,
                                                    String recordedAt, Evidence evidence) {
        return assertion(id, value, recordedAt, evidence, KEY, TimeInterval.unboundedFrom(VALID_FROM));
    }

    private static NormalizedMemoryEvent assertion(String id, String value,
                                                    String recordedAt, Evidence evidence,
                                                    FactKey key, TimeInterval validTime) {
        MemoryFact fact = MemoryFact.attribute("fact-" + id,
            new MemoryEntity("alice", "person"), "city", value);
        return new EventNormalizer().normalize(MemoryEvent.add(id, fact,
            validTime, Instant.parse(recordedAt), Collections.singletonList(evidence)), key);
    }

    private static TimeInterval interval(String start, String end) {
        return new TimeInterval(Instant.parse(start), Instant.parse(end));
    }
}
