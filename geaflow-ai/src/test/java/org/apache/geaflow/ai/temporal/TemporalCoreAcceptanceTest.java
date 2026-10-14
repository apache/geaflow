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

package org.apache.geaflow.ai.temporal;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.temporal.integration.IncrementalTemporalIntegrator;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.model.VersionRelationType;
import org.apache.geaflow.ai.temporal.oracle.FullReplayOracle;
import org.apache.geaflow.ai.temporal.query.BitemporalQuery;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * A fixed, end-to-end acceptance example for the Phase 1 temporal core.
 */
public class TemporalCoreAcceptanceTest {

    private static final MemoryEntity ALICE =
        new MemoryEntity("alice", "person");
    private static final FactKey CITY_KEY =
        new FactKey("alice", "city", "profile");
    private static final Source PROFILE =
        new Source("source-profile", "profile");

    private final EventNormalizer normalizer = new EventNormalizer();
    private final BitemporalQuery query = new BitemporalQuery();

    @Test
    public void testCorrectionRetractionAndReplayAreIndependentlyVerifiable() {
        NormalizedMemoryEvent added = normalize(MemoryEvent.add(
            "event-1",
            cityFact("fact-beijing", "Beijing"),
            TimeInterval.unboundedFrom(time("2024-01-01T00:00:00Z")),
            time("2024-02-01T00:00:00Z"),
            evidence("evidence-1", "Alice lived in Beijing.")));
        NormalizedMemoryEvent corrected = normalize(MemoryEvent.correct(
            "event-2",
            cityFact("fact-shanghai", "Shanghai"),
            interval("2024-04-01T00:00:00Z", "2024-07-01T00:00:00Z"),
            time("2024-05-01T00:00:00Z"),
            evidence("evidence-2", "Alice moved to Shanghai.")));
        NormalizedMemoryEvent retracted = normalize(MemoryEvent.retract(
            "event-3",
            "fact-shanghai",
            interval("2024-06-01T00:00:00Z", "2024-07-01T00:00:00Z"),
            time("2024-06-15T00:00:00Z"),
            evidence("evidence-3", "The later Shanghai claim was withdrawn.")));
        List<NormalizedMemoryEvent> events =
            Arrays.asList(added, corrected, retracted);

        TemporalState oracleState =
            new FullReplayOracle().replayNormalized(events);
        IncrementalTemporalIntegrator incremental =
            new IncrementalTemporalIntegrator();
        for (NormalizedMemoryEvent event : events) {
            incremental.apply(event);
        }
        TemporalState actual = incremental.stateSnapshot();

        Assertions.assertEquals(oracleState, actual);
        Assertions.assertEquals(6, actual.getVersions().size());
        Assertions.assertEquals(1, countStatus(
            actual, MemoryFactVersionStatus.RETRACTED));
        Assertions.assertEquals(5, actual.getRelations().size());
        Assertions.assertTrue(actual.getRelations().stream().allMatch(
            relation -> relation.getType() == VersionRelationType.SUPERSEDES));

        MemoryFactVersion tombstone = actual.getVersions().stream()
            .filter(version -> version.getStatus()
                == MemoryFactVersionStatus.RETRACTED)
            .findFirst()
            .get();
        Assertions.assertEquals(
            time("2024-06-01T00:00:00Z"),
            tombstone.getValidTime().getStart());
        Assertions.assertEquals(
            time("2024-07-01T00:00:00Z"),
            tombstone.getValidTime().getEnd().get());
        Assertions.assertEquals(
            time("2024-06-15T00:00:00Z"),
            tombstone.getTransactionTime().getStart());
        Assertions.assertFalse(
            tombstone.getTransactionTime().getEnd().isPresent());

        assertValues(actual, "2024-04-15T00:00:00Z",
            "2024-04-30T00:00:00Z", "Beijing");
        assertValues(actual, "2024-04-15T00:00:00Z",
            "2024-05-15T00:00:00Z", "Shanghai");
        assertValues(actual, "2024-03-01T00:00:00Z",
            "2024-06-20T00:00:00Z", "Beijing");
        assertValues(actual, "2024-06-20T00:00:00Z",
            "2024-06-01T00:00:00Z", "Shanghai");
        assertValues(actual, "2024-06-20T00:00:00Z",
            "2024-06-20T00:00:00Z");
        assertValues(actual, "2024-07-01T00:00:00Z",
            "2024-06-20T00:00:00Z", "Beijing");
        assertValues(actual, "2024-06-01T00:00:00Z",
            "2024-06-15T00:00:00Z");

        MemoryFactVersion retainedBeijing = query.query(
            actual.getVersions(),
            time("2024-03-01T00:00:00Z"),
            time("2024-06-20T00:00:00Z")).get(0);
        Assertions.assertEquals(
            "evidence-1", retainedBeijing.getEvidence().get(0).getId());
        Assertions.assertEquals(
            "source-profile",
            retainedBeijing.getEvidence().get(0).getSource().getId());

        TemporalState beforeDuplicate = incremental.stateSnapshot();
        incremental.apply(corrected);
        Assertions.assertEquals(beforeDuplicate, incremental.stateSnapshot());

        NormalizedMemoryEvent reusedId = normalize(MemoryEvent.correct(
            "event-2",
            cityFact("fact-tianjin", "Tianjin"),
            interval("2024-04-01T00:00:00Z", "2024-07-01T00:00:00Z"),
            time("2024-05-01T00:00:00Z"),
            evidence("evidence-2", "Alice moved to Tianjin.")));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> incremental.apply(reusedId));
        Assertions.assertEquals(beforeDuplicate, incremental.stateSnapshot());
    }

    private NormalizedMemoryEvent normalize(MemoryEvent event) {
        return normalizer.normalize(event, CITY_KEY);
    }

    private void assertValues(
        TemporalState state,
        String validAt,
        String transactionAt,
        String... expected) {
        List<String> actual = query.query(
            state.getVersions(),
            time(validAt),
            time(transactionAt)).stream()
            .map(version -> version.getFact().getLiteralValue().get())
            .sorted()
            .collect(Collectors.toList());
        List<String> expectedValues = Arrays.asList(expected);
        Collections.sort(expectedValues);
        Assertions.assertEquals(expectedValues, actual);
    }

    private static long countStatus(
        TemporalState state,
        MemoryFactVersionStatus status) {
        return state.getVersions().stream()
            .filter(version -> version.getStatus() == status)
            .count();
    }

    private static MemoryFact cityFact(String id, String city) {
        return MemoryFact.attribute(id, ALICE, "city", city);
    }

    private static List<Evidence> evidence(String id, String content) {
        return Collections.singletonList(new Evidence(id, PROFILE, content));
    }

    private static TimeInterval interval(String start, String end) {
        return new TimeInterval(time(start), time(end));
    }

    private static Instant time(String value) {
        return Instant.parse(value);
    }
}
