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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EventCandidateServiceTest {

    private static final Instant BASE = Instant.parse("2024-01-01T00:00:00Z");
    private static final FactKey KEY = new FactKey("alice", "city", "profile");
    private static final String CONTENT = "climate temperature increases across the planet each year";

    @Test
    public void testEffectiveIntervalKeepsAllMandatoryAndTopKOnlyLimitsSoftCandidates()
        throws Exception {
        final List<MemoryFactVersion> mandatory = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            mandatory.add(version("v-" + i, KEY, interval(0, 40), interval(0, null),
                MemoryFactVersionStatus.ACTIVE));
        }
        final Map<FactKey, List<MemoryFactVersion>> groups = new TreeMap<>();
        groups.put(KEY, mandatory);
        FactKey other = new FactKey("bob", "city", "profile");
        groups.put(other, Arrays.asList(
            version("soft-a", other, interval(0, 40), interval(0, null),
                MemoryFactVersionStatus.ACTIVE),
            version("soft-b", other, interval(0, 40), interval(0, null),
                MemoryFactVersionStatus.ACTIVE)));
        TemporalState state = new TemporalState(groups, Collections.emptyList());
        NormalizedMemoryEvent event = event(interval(50, 60));
        for (RetrievalProfile profile : Arrays.asList(RetrievalProfile.RULE_ONLY,
            RetrievalProfile.RULE_BM25, RetrievalProfile.RULE_MINHASH, RetrievalProfile.RULE_ALL)) {
            List<FactCandidate> candidates = new TemporalCandidateService(profile)
                .eventFacts(state, event, interval(0, 60), 1);
            Assertions.assertEquals(30, candidates.stream().filter(FactCandidate::isMandatory)
                .count());
            Assertions.assertTrue(candidates.size() >= 30 && candidates.size() <= 31);
            for (int i = 0; i < 30; i++) {
                Assertions.assertTrue(candidates.get(i).isMandatory());
                Assertions.assertEquals(KEY, candidates.get(i).getFactKey());
                Assertions.assertEquals(1.0, candidates.get(i).getScores().get("RULE"));
            }
            if (profile.usesBm25()) {
                Assertions.assertEquals(31, candidates.size());
                Assertions.assertFalse(candidates.get(30).isMandatory());
            }
        }
        Assertions.assertEquals(32, state.getVersions().size());
        Assertions.assertTrue(state.getRelations().isEmpty());
    }

    @Test
    public void testHiddenFutureClosedBoundaryAndRetractedVersionsCannotChangeScores()
        throws Exception {
        final Map<FactKey, List<MemoryFactVersion>> baseline = new TreeMap<>();
        baseline.put(KEY, Collections.singletonList(version("visible", KEY, interval(0, 20),
            interval(0, null), MemoryFactVersionStatus.ACTIVE)));
        final Map<FactKey, List<MemoryFactVersion>> all = new TreeMap<>(baseline);
        FactKey other = new FactKey("bob", "city", "profile");
        all.put(other, Arrays.asList(
            version("future", other, interval(0, 20), interval(11, null),
                MemoryFactVersionStatus.ACTIVE),
            version("closed", other, interval(0, 20), interval(0, 10),
                MemoryFactVersionStatus.ACTIVE),
            version("boundary", other, interval(20, 30), interval(0, null),
                MemoryFactVersionStatus.ACTIVE),
            version("retracted", other, interval(0, 20), interval(0, null),
                MemoryFactVersionStatus.RETRACTED)));
        FactKey secret = new FactKey("alice", "city", "secret");
        all.put(secret, Collections.singletonList(version("secret", secret, interval(0, 20),
            interval(0, null), MemoryFactVersionStatus.ACTIVE)));
        final NormalizedMemoryEvent event = event(interval(5, 20));
        for (RetrievalProfile profile : Arrays.asList(RetrievalProfile.RULE_ONLY,
            RetrievalProfile.RULE_BM25, RetrievalProfile.RULE_MINHASH, RetrievalProfile.RULE_ALL)) {
            TemporalCandidateService service = new TemporalCandidateService(profile);
            List<FactCandidate> expected = service.eventFacts(new TemporalState(baseline,
                Collections.emptyList()), event, event.getValidTime(), 20);
            List<FactCandidate> actual = service.eventFacts(new TemporalState(all,
                Collections.emptyList()), event, event.getValidTime(), 20);
            Assertions.assertEquals(Collections.singletonList("visible"), actual.stream()
                .map(candidate -> candidate.getVersion().getId()).collect(Collectors.toList()));
            Assertions.assertEquals(expected.get(0).getScores(), actual.get(0).getScores());
            Assertions.assertEquals(expected.get(0).getChannelRanks(), actual.get(0).getChannelRanks());
        }
    }

    private static NormalizedMemoryEvent event(TimeInterval valid) {
        MemoryFact fact = MemoryFact.attribute("incoming-fact", new MemoryEntity("alice", "person"),
            "city", "Shanghai");
        return new EventNormalizer().normalize(MemoryEvent.add("incoming", fact, valid,
            BASE.plusSeconds(10), Collections.singletonList(evidence("incoming-e"))), KEY);
    }

    private static MemoryFactVersion version(String id, FactKey key, TimeInterval valid,
                                             TimeInterval system, MemoryFactVersionStatus status) {
        MemoryFact fact = MemoryFact.attribute("fact-" + id,
            new MemoryEntity(key.getSubjectId(), "person"), key.getPredicate(), "Shanghai");
        return new MemoryFactVersion(id, fact, status, valid, system,
            Collections.singletonList(evidence("e-" + id)));
    }

    private static Evidence evidence(String id) {
        return new Evidence(id, new Source("source", "Registry"), CONTENT);
    }

    private static TimeInterval interval(long from, Integer to) {
        return new TimeInterval(BASE.plusSeconds(from), to == null ? null : BASE.plusSeconds(to));
    }
}
