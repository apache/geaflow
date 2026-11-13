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

package org.apache.geaflow.ai.temporal.trace;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.temporal.api.QueryMode;
import org.apache.geaflow.ai.temporal.api.QueryResult;
import org.apache.geaflow.ai.temporal.api.TemporalMemoryFacade;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.query.TemporalQueryService;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.VersionIdentity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Explanations cover the complete merged interval and retain the query's result order. */
public class TraceBuilderTest {

    private static final FactKey KEY = new FactKey("alice", "city", "profile");
    private static final Instant JANUARY = Instant.parse("2024-01-01T00:00:00Z");
    private static final Instant MARCH = Instant.parse("2024-03-01T00:00:00Z");
    private static final Instant APRIL = Instant.parse("2024-04-01T00:00:00Z");
    private static final Instant MAY = Instant.parse("2024-05-01T00:00:00Z");
    private static final Instant JUNE = Instant.parse("2024-06-01T00:00:00Z");
    private static final Instant OCTOBER = Instant.parse("2024-10-01T00:00:00Z");
    private static final Instant NOVEMBER = Instant.parse("2024-11-01T00:00:00Z");
    private static final Instant DECEMBER = Instant.parse("2024-12-01T00:00:00Z");

    @Test
    public void testMergedSpanIncludesConflictOutsideSupplementalEvidenceInterval() {
        final NormalizedMemoryEvent paris = assertion("paris", "Paris", JANUARY, DECEMBER, MARCH);
        NormalizedMemoryEvent berlin = assertion("berlin", "Berlin", JANUARY, MARCH, APRIL);
        NormalizedMemoryEvent supplemental = assertion("supplemental", "Paris",
            OCTOBER, NOVEMBER, MAY);
        String berlinVersion = VersionIdentity.create(berlin, berlin.getEvent().getFact().get(),
            MemoryFactVersionStatus.ACTIVE, berlin.getValidTime(), null);
        String parisVersion = VersionIdentity.create(paris, paris.getEvent().getFact().get(),
            MemoryFactVersionStatus.ACTIVE, paris.getValidTime(), null);
        JsonObject trace = TraceBuilder.replay(Arrays.asList(supplemental, berlin, paris))
            .get(supplemental.getEventId());

        Assertions.assertTrue(values(trace.getAsJsonArray("candidateVersionIds")).contains(berlinVersion));
        Assertions.assertTrue(values(trace.getAsJsonArray("readVersionIds")).contains(berlinVersion));
        Assertions.assertTrue(values(trace.getAsJsonArray("readVersionIds")).contains(parisVersion));
        Assertions.assertTrue(values(trace.getAsJsonArray("rules")).contains("MERGE_EQUAL_VALUE"));
        Assertions.assertTrue(values(trace.getAsJsonArray("rules")).contains("COEXIST_CONFLICT"));
        Assertions.assertEquals(Collections.singletonList(parisVersion),
            values(trace.getAsJsonArray("closedVersionIds")));
        Assertions.assertTrue(trace.getAsJsonArray("addedRelations").size() >= 2);
    }

    @Test
    public void testQueryHitIdsFollowQueryIntervalOrderInsteadOfLexicographicIds() {
        NormalizedMemoryEvent earlierValid = assertion("z-earlier-valid", "Paris",
            JANUARY, DECEMBER, MARCH);
        NormalizedMemoryEvent laterValid = assertion("a-later-valid", "Berlin",
            APRIL, DECEMBER, MAY);
        TemporalMemoryFacade facade = new TemporalMemoryFacade();
        facade.replay(Arrays.asList(laterValid, earlierValid));
        List<MemoryFactVersion> expected = new TemporalQueryService().current(
            facade.getSnapshot().getState(), KEY, JUNE);
        final QueryResult result = facade.query(QueryMode.CURRENT, KEY, JUNE, null);
        List<String> expectedIds = new ArrayList<>();
        for (MemoryFactVersion version : expected) {
            expectedIds.add(version.getId());
        }

        Assertions.assertEquals(2, expected.size());
        Assertions.assertTrue(expectedIds.get(0).startsWith("z-earlier-valid:"));
        Assertions.assertTrue(expectedIds.get(1).startsWith("a-later-valid:"));
        Assertions.assertEquals(expected, result.getVersions());
        Assertions.assertEquals(expectedIds,
            values(result.getTrace().toJson().getAsJsonArray("hitVersionIds")));
    }

    private static NormalizedMemoryEvent assertion(String id, String value,
                                                    Instant from, Instant to, Instant recordedAt) {
        MemoryFact fact = MemoryFact.attribute("fact-" + id,
            new MemoryEntity("alice", "person"), "city", value);
        Evidence evidence = new Evidence("proof-" + id,
            new Source("source", "Registry"), "Evidence for " + id);
        return new EventNormalizer().normalize(MemoryEvent.add(id, fact,
            new TimeInterval(from, to), recordedAt, Collections.singletonList(evidence)), KEY);
    }

    private static List<String> values(JsonArray array) {
        List<String> result = new ArrayList<>();
        for (JsonElement value : array) {
            result.add(value.getAsString());
        }
        return result;
    }
}
