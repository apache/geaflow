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

package org.apache.geaflow.ai.temporal.query;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TemporalQueryServiceTest {

    private static final Instant BASE = Instant.parse("2024-01-01T00:00:00Z");
    private static final FactKey KEY = new FactKey("person:alice", "city", "profile");

    private final TemporalQueryService query = new TemporalQueryService();

    @Test
    public void testTwoDimensionalHalfOpenBoundaryMatrixAtNanosecondPrecision() {
        MemoryFactVersion version = active("bounded", interval(10, 20), interval(30, 40));
        TemporalState state = state(KEY, version);
        long[] validPoints = {9, 10, 11, 19, 20, 21};
        long[] systemPoints = {29, 30, 31, 39, 40, 41};

        for (long validPoint : validPoints) {
            for (long systemPoint : systemPoints) {
                boolean visible = validPoint >= 10 && validPoint < 20
                    && systemPoint >= 30 && systemPoint < 40;
                List<String> expected = visible
                    ? Collections.singletonList("bounded") : Collections.emptyList();
                List<MemoryFactVersion> actual = query.asOf(
                    state, KEY, at(validPoint), at(systemPoint));
                Assertions.assertEquals(expected, ids(actual),
                    "valid=" + validPoint + ", system=" + systemPoint);
                Assertions.assertEquals(actual,
                    query.asOf(state, at(validPoint), at(systemPoint)));
            }
        }
    }

    @Test
    public void testCurrentUsesOpenSystemEndAndExplicitValidTime() {
        MemoryFactVersion current = active("current", interval(10, 20), open(30));
        MemoryFactVersion closed = active("closed", interval(10, 20), interval(0, 30));
        MemoryFactVersion tombstone = version(
            "tombstone", MemoryFactVersionStatus.RETRACTED, interval(10, 20), open(30));
        TemporalState state = state(KEY, tombstone, current, closed);

        Assertions.assertTrue(query.current(state, KEY, at(9)).isEmpty());
        Assertions.assertEquals(Collections.singletonList("current"),
            ids(query.current(state, KEY, at(10))));
        Assertions.assertEquals(Collections.singletonList("current"),
            ids(query.current(state, KEY, at(19))));
        Assertions.assertTrue(query.current(state, KEY, at(20)).isEmpty());
        Assertions.assertEquals(query.current(state, KEY, at(10)),
            query.current(state, at(10)));
    }

    @Test
    public void testHistoricalKeepsClosedAndTombstoneWithoutHidingConflict() {
        MemoryFactVersion closed = active("closed", open(0), interval(0, 10));
        MemoryFactVersion conflict = active("conflict", open(0), open(0));
        MemoryFactVersion tombstone = version(
            "tombstone", MemoryFactVersionStatus.RETRACTED, open(0), open(10));
        TemporalState state = state(KEY, tombstone, conflict, closed);

        Assertions.assertEquals(Arrays.asList("closed", "conflict", "tombstone"),
            ids(query.historical(state, KEY, at(5))));
        Assertions.assertEquals(query.historical(state, KEY, at(5)),
            query.historical(state, at(5)));
        Assertions.assertTrue(query.historical(state, KEY, at(-1)).isEmpty());
        Assertions.assertEquals(Arrays.asList("closed", "conflict"),
            ids(query.asOf(state, KEY, at(5), at(9))));
        Assertions.assertEquals(Collections.singletonList("conflict"),
            ids(query.asOf(state, KEY, at(5), at(10))));
        Assertions.assertEquals(Collections.singletonList("conflict"),
            ids(query.current(state, KEY, at(5))));
    }

    @Test
    public void testOpenIntervalsIncludeInstantMax() {
        MemoryFactVersion open = active("open", open(0), open(0));
        MemoryFactVersion finite = active("finite",
            new TimeInterval(BASE, Instant.MAX), new TimeInterval(BASE, Instant.MAX));
        TemporalState state = state(KEY, finite, open);

        Assertions.assertEquals(Collections.singletonList("open"),
            ids(query.asOf(state, KEY, Instant.MAX, Instant.MAX)));
        Assertions.assertEquals(Collections.singletonList("open"),
            ids(query.current(state, KEY, Instant.MAX)));
        Assertions.assertEquals(Collections.singletonList("open"),
            ids(query.historical(state, KEY, Instant.MAX)));
    }

    @Test
    public void testExactFactKeyFilteringIncludesScope() {
        FactKey otherScope = new FactKey(KEY.getSubjectId(), KEY.getPredicate(), "work");
        FactKey otherPredicate = new FactKey(KEY.getSubjectId(), "status", KEY.getScope());
        FactKey otherSubject = new FactKey("person:bob", KEY.getPredicate(), KEY.getScope());
        Map<FactKey, List<MemoryFactVersion>> groups = new LinkedHashMap<>();
        groups.put(otherScope, Collections.singletonList(active("work", otherScope)));
        groups.put(otherSubject, Collections.singletonList(active("bob", otherSubject)));
        groups.put(KEY, Collections.singletonList(active("profile", open(0), open(0))));
        groups.put(otherPredicate,
            Collections.singletonList(active("status", otherPredicate)));
        TemporalState state = new TemporalState(groups, Collections.emptyList());

        Assertions.assertEquals(Collections.singletonList("profile"),
            ids(query.current(state, new FactKey("person:alice", "city", "profile"), BASE)));
        Assertions.assertEquals(Collections.singletonList("profile"),
            ids(query.historical(state, KEY, BASE)));
        Assertions.assertEquals(Collections.singletonList("work"),
            ids(query.asOf(state, otherScope, BASE, BASE)));
        Assertions.assertEquals(Arrays.asList("profile", "work", "status", "bob"),
            ids(query.current(state, BASE)));
        FactKey absent = new FactKey("person:alice", "city", "absent");
        Assertions.assertTrue(query.current(state, absent, BASE).isEmpty());
        Assertions.assertTrue(query.historical(state, absent, BASE).isEmpty());
        Assertions.assertTrue(query.asOf(state, absent, BASE, BASE).isEmpty());
    }

    @Test
    public void testStableOrderUsesBothIntervalEndsBeforeVersionId() {
        List<MemoryFactVersion> versions = Arrays.asList(
            active("tie-z", open(0), open(0)),
            active("a-valid-later", open(1), open(0)),
            active("m-system-open", interval(0, 30), open(0)),
            active("z-system-long", interval(0, 30), interval(0, 20)),
            active("tie-a", open(0), open(0)),
            active("n-system-later", interval(0, 30), open(1)),
            active("a-system-short", interval(0, 30), interval(0, 10)),
            active("z-valid-end", interval(0, 20), open(3)));
        TemporalState state = new TemporalState(
            Collections.singletonMap(KEY, versions), Collections.emptyList());
        List<MemoryFactVersion> reversed = new ArrayList<>(versions);
        Collections.reverse(reversed);
        TemporalState reordered = new TemporalState(
            Collections.singletonMap(KEY, reversed), Collections.emptyList());

        Assertions.assertEquals(Arrays.asList("z-valid-end", "a-system-short", "z-system-long",
            "m-system-open", "n-system-later", "tie-a", "tie-z", "a-valid-later"),
            ids(query.historical(state, at(15))));
        Assertions.assertEquals(Arrays.asList("z-valid-end", "m-system-open", "n-system-later",
            "tie-a", "tie-z", "a-valid-later"), ids(query.current(state, at(15))));
        Assertions.assertEquals(Arrays.asList("z-valid-end", "z-system-long", "m-system-open",
            "n-system-later", "tie-a", "tie-z", "a-valid-later"),
            ids(query.asOf(state, at(15), at(15))));
        Assertions.assertEquals(query.historical(state, at(15)),
            query.historical(reordered, at(15)));
        Assertions.assertEquals(query.current(state, at(15)),
            query.current(reordered, at(15)));
        Assertions.assertEquals(query.asOf(state, at(15), at(15)),
            query.asOf(reordered, at(15), at(15)));
    }

    @Test
    public void testResultsAreImmutableAndInputsUnchanged() {
        MemoryFactVersion version = active("active", open(0), open(0));
        TemporalState state = state(KEY, version);

        Assertions.assertThrows(UnsupportedOperationException.class,
            () -> query.current(state, KEY, BASE).clear());
        Assertions.assertThrows(UnsupportedOperationException.class,
            () -> query.historical(state, KEY, BASE).clear());
        Assertions.assertThrows(UnsupportedOperationException.class,
            () -> query.asOf(state, KEY, BASE, BASE).clear());
        Assertions.assertEquals(Collections.singletonList(version), state.getVersions());
    }

    @Test
    public void testEmptyStateAndNullArguments() {
        TemporalState empty = new TemporalState(Collections.emptyMap(), Collections.emptyList());

        Assertions.assertTrue(query.current(empty, BASE).isEmpty());
        Assertions.assertTrue(query.historical(empty, BASE).isEmpty());
        Assertions.assertTrue(query.asOf(empty, BASE, BASE).isEmpty());
        Assertions.assertThrows(NullPointerException.class, () -> query.current(null, BASE));
        Assertions.assertThrows(NullPointerException.class, () -> query.historical(null, BASE));
        Assertions.assertThrows(NullPointerException.class, () -> query.asOf(null, BASE, BASE));
        Assertions.assertThrows(NullPointerException.class, () -> query.current(empty, null));
        Assertions.assertThrows(NullPointerException.class, () -> query.historical(empty, null));
        Assertions.assertThrows(NullPointerException.class, () -> query.asOf(empty, null, BASE));
        Assertions.assertThrows(NullPointerException.class, () -> query.asOf(empty, BASE, null));
        Assertions.assertThrows(NullPointerException.class, () -> query.current(empty, null, BASE));
        Assertions.assertThrows(NullPointerException.class,
            () -> query.historical(empty, null, BASE));
        Assertions.assertThrows(NullPointerException.class,
            () -> query.asOf(empty, null, BASE, BASE));
    }

    private static TemporalState state(FactKey key, MemoryFactVersion... versions) {
        return new TemporalState(Collections.singletonMap(key, Arrays.asList(versions)),
            Collections.emptyList());
    }

    private static MemoryFactVersion active(
        String id, TimeInterval valid, TimeInterval system) {
        return version(id, MemoryFactVersionStatus.ACTIVE, valid, system);
    }

    private static MemoryFactVersion active(String id, FactKey key) {
        return version(id, key, MemoryFactVersionStatus.ACTIVE, open(0), open(0));
    }

    private static MemoryFactVersion version(
        String id,
        MemoryFactVersionStatus status,
        TimeInterval valid,
        TimeInterval system) {
        return version(id, KEY, status, valid, system);
    }

    private static MemoryFactVersion version(
        String id,
        FactKey key,
        MemoryFactVersionStatus status,
        TimeInterval valid,
        TimeInterval system) {
        MemoryFact fact = MemoryFact.attribute("fact-" + id,
            new MemoryEntity(key.getSubjectId(), "person"), key.getPredicate(), "value-" + id);
        Evidence evidence = new Evidence("evidence-" + id,
            new Source("source", "fixture"), "Evidence for " + id);
        return new MemoryFactVersion(id, fact, status, valid, system,
            Collections.singletonList(evidence));
    }

    private static TimeInterval interval(long start, long end) {
        return new TimeInterval(at(start), at(end));
    }

    private static TimeInterval open(long start) {
        return TimeInterval.unboundedFrom(at(start));
    }

    private static Instant at(long nanos) {
        return BASE.plusNanos(nanos);
    }

    private static List<String> ids(List<MemoryFactVersion> versions) {
        return versions.stream().map(MemoryFactVersion::getId).collect(Collectors.toList());
    }
}
