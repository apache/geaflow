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

package org.apache.geaflow.ai.temporal.semantics;

import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EventLedgerTest {

    private final EventNormalizer normalizer = new EventNormalizer();

    @Test
    public void testCheckDoesNotMutateBeforeCommit() {
        EventLedger ledger = new EventLedger();
        NormalizedMemoryEvent original = normalized(
            "event-1",
            "Beijing");
        NormalizedMemoryEvent reusedBeforeCommit = normalized(
            "event-1",
            "Shanghai");

        Assertions.assertEquals(
            EventLedgerDecision.ACCEPTED,
            ledger.check(original));
        Assertions.assertEquals(
            EventLedgerDecision.ACCEPTED,
            ledger.check(reusedBeforeCommit));
        Assertions.assertEquals(
            Optional.empty(),
            ledger.getPayloadHash("event-1"));

        Assertions.assertEquals(
            EventLedgerDecision.ACCEPTED,
            ledger.commit(original));
        Assertions.assertEquals(
            Optional.of(original.getPayloadHash()),
            ledger.getPayloadHash("event-1"));
    }

    @Test
    public void testDuplicateNoopAndRejectedReuseRemainAtomic() {
        EventLedger ledger = new EventLedger();
        NormalizedMemoryEvent original = normalized(
            "event-1",
            "Beijing");
        NormalizedMemoryEvent duplicate = normalized(
            "event-1",
            "Beijing");
        NormalizedMemoryEvent reused = normalized(
            "event-1",
            "Shanghai");
        NormalizedMemoryEvent otherId = normalized(
            "event-2",
            "Beijing");

        ledger.commit(original);

        Assertions.assertEquals(
            EventLedgerDecision.DUPLICATE_NOOP,
            ledger.check(duplicate));
        Assertions.assertEquals(
            EventLedgerDecision.DUPLICATE_NOOP,
            ledger.commit(duplicate));
        Assertions.assertEquals(
            EventLedgerDecision.REJECT_EVENT_ID_REUSE,
            ledger.check(reused));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> ledger.commit(reused));
        Assertions.assertEquals(
            Optional.of(original.getPayloadHash()),
            ledger.getPayloadHash("event-1"));
        Assertions.assertEquals(
            EventLedgerDecision.ACCEPTED,
            ledger.check(otherId));
        Assertions.assertEquals(
            Optional.empty(),
            ledger.getPayloadHash("event-2"));
    }

    @Test
    public void testRejectNullEvent() {
        EventLedger ledger = new EventLedger();

        Assertions.assertThrows(
            NullPointerException.class,
            () -> ledger.check(null));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> ledger.commit(null));
    }

    private NormalizedMemoryEvent normalized(
        String eventId,
        String value) {
        MemoryEvent event = MemoryEvent.add(
            eventId,
            MemoryFact.attribute(
                "fact-location",
                new MemoryEntity("person:alice", "person"),
                "location",
                value),
            TimeInterval.unboundedFrom(
                time("2024-01-01T00:00:00Z")),
            time("2024-03-01T00:00:00Z"),
            Collections.singletonList(new Evidence(
                "evidence-1",
                new Source("source-1", "registry"),
                "recorded location")));
        return normalizer.normalize(
            event,
            new FactKey(
                "person:alice",
                "location",
                "profile"));
    }

    private static Instant time(String value) {
        return Instant.parse(value);
    }
}
