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

package org.apache.geaflow.ai.temporal.integration;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.oracle.FullReplayOracle;
import org.apache.geaflow.common.serialize.ISerializer;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.state.serializer.DefaultKVSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TemporalGeaFlowSerializationTest {

    private final TemporalEventAggregateFunction function =
        new TemporalEventAggregateFunction();
    private final FullReplayOracle oracle = new FullReplayOracle();

    @Test
    public void testMemoryEventRoundTripsThroughShuffleSerializer() {
        MemoryEvent event = correctEvent(
            "event-correct",
            "Shanghai",
            "2024-04-01T00:00:00Z",
            "2024-09-01T00:00:00Z",
            "2024-06-01T00:00:00Z");
        ISerializer serializer =
            SerializerFactory.getKryoSerializer();

        MemoryEvent restored = (MemoryEvent) serializer.deserialize(
            serializer.serialize(event));

        Assertions.assertEquals(event, restored);
    }

    @Test
    public void testAccumulatorRoundTripsThroughKeyValueStateSerializer() {
        MemoryEvent add = addEvent(
            "event-add",
            "Beijing",
            "2024-03-01T00:00:00Z");
        MemoryEvent correction = correctEvent(
            "event-correct",
            "Shanghai",
            "2024-04-01T00:00:00Z",
            "2024-09-01T00:00:00Z",
            "2024-06-01T00:00:00Z");
        MemoryEvent retract = retractEvent(
            "event-retract",
            "2024-08-01T00:00:00Z",
            "2024-10-01T00:00:00Z",
            "2024-11-01T00:00:00Z");
        MemoryEvent lateCorrection = correctEvent(
            "event-late",
            "Tianjin",
            "2024-02-01T00:00:00Z",
            "2024-03-01T00:00:00Z",
            "2024-05-01T00:00:00Z");
        IncrementalTemporalIntegrator accumulator =
            function.createAccumulator();
        function.add(add, accumulator);
        function.add(correction, accumulator);
        function.add(retract, accumulator);
        DefaultKVSerializer<String, IncrementalTemporalIntegrator>
            serializer = new DefaultKVSerializer<>(String.class, null);

        Assertions.assertEquals(
            "fact-alice-city",
            serializer.deserializeKey(
                serializer.serializeKey("fact-alice-city")));
        IncrementalTemporalIntegrator restored =
            serializer.deserializeValue(
                serializer.serializeValue(accumulator));

        Assertions.assertNotNull(restored);
        Assertions.assertEquals(
            accumulator.eventSnapshot(),
            restored.eventSnapshot());
        Assertions.assertEquals(
            function.getResult(accumulator),
            function.getResult(restored));

        function.add(lateCorrection, restored);
        Assertions.assertEquals(
            oracle.replay(Arrays.asList(
                add,
                correction,
                retract,
                lateCorrection)),
            function.getResult(restored));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testResultSnapshotKeepsImmutabilityAfterRoundTrip() {
        IncrementalTemporalIntegrator accumulator =
            function.createAccumulator();
        function.add(
            addEvent(
                "event-add",
                "Beijing",
                "2024-03-01T00:00:00Z"),
            accumulator);
        List<MemoryFactVersion> snapshot =
            function.getResult(accumulator);
        ISerializer serializer =
            SerializerFactory.getKryoSerializer();

        List<MemoryFactVersion> restored =
            (List<MemoryFactVersion>) serializer.deserialize(
                serializer.serialize(snapshot));

        Assertions.assertEquals(snapshot, restored);
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> restored.clear());
    }

    private static MemoryEvent addEvent(
        String eventId,
        String value,
        String transactionTime) {
        return MemoryEvent.add(
            eventId,
            fact(value),
            TimeInterval.unboundedFrom(
                time("2024-01-01T00:00:00Z")),
            time(transactionTime),
            evidence(eventId));
    }

    private static MemoryEvent correctEvent(
        String eventId,
        String value,
        String validStart,
        String validEnd,
        String transactionTime) {
        return MemoryEvent.correct(
            eventId,
            fact(value),
            interval(validStart, validEnd),
            time(transactionTime),
            evidence(eventId));
    }

    private static MemoryEvent retractEvent(
        String eventId,
        String validStart,
        String validEnd,
        String transactionTime) {
        return MemoryEvent.retract(
            eventId,
            "fact-alice-city",
            interval(validStart, validEnd),
            time(transactionTime),
            evidence(eventId));
    }

    private static MemoryFact fact(String value) {
        return MemoryFact.attribute(
            "fact-alice-city",
            new MemoryEntity("person:alice", "person"),
            "city",
            value);
    }

    private static TimeInterval interval(
        String start,
        String end) {
        return new TimeInterval(time(start), time(end));
    }

    private static List<Evidence> evidence(String eventId) {
        return Collections.singletonList(new Evidence(
            "evidence-" + eventId,
            new Source("source-1", "customer-database"),
            "Evidence for " + eventId));
    }

    private static Instant time(String value) {
        return Instant.parse(value);
    }
}
