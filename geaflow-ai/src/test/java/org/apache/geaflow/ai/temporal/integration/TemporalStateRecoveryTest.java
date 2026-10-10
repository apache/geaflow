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

import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.oracle.FullReplayOracle;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.state.KeyValueState;
import org.apache.geaflow.state.StateFactory;
import org.apache.geaflow.state.StoreType;
import org.apache.geaflow.state.descriptor.KeyValueStateDescriptor;
import org.apache.geaflow.utils.keygroup.DefaultKeyGroupAssigner;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

@DisabledOnOs(
    value = OS.WINDOWS,
    disabledReason = "GeaFlow LOCAL persistence requires Hadoop winutils.exe")
public class TemporalStateRecoveryTest {

    private static final String FACT_ID = "fact-alice-city";
    private static final String BOB_FACT_ID = "fact-bob-city";
    private static final long CHECKPOINT_ID = 1L;

    private final FullReplayOracle oracle = new FullReplayOracle();

    @TempDir
    Path tempDirectory;

    @Test
    public void testAccumulatorRecoversFromRocksdbCheckpoint() {
        MemoryEvent add = addEvent();
        IncrementalTemporalIntegrator accumulator =
            new IncrementalTemporalIntegrator();
        accumulator.apply(add);

        Configuration configuration = stateConfiguration();
        KeyValueStateDescriptor<String, IncrementalTemporalIntegrator>
            descriptor = stateDescriptor();
        KeyValueState<String, IncrementalTemporalIntegrator> originalState =
            StateFactory.buildKeyValueState(descriptor, configuration);

        try {
            originalState.manage().operate()
                .setCheckpointId(CHECKPOINT_ID);
            originalState.put(FACT_ID, accumulator);
            originalState.manage().operate().finish();
            originalState.manage().operate().archive();
        } finally {
            closeAndDrop(originalState);
        }

        KeyValueState<String, IncrementalTemporalIntegrator> recoveredState =
            StateFactory.buildKeyValueState(descriptor, configuration);
        try {
            recoveredState.manage().operate()
                .setCheckpointId(CHECKPOINT_ID);
            recoveredState.manage().operate().recover();

            IncrementalTemporalIntegrator recovered =
                recoveredState.get(FACT_ID);
            Assertions.assertNotNull(recovered);
            Assertions.assertEquals(
                Collections.singletonList(add),
                recovered.eventSnapshot());
            Assertions.assertEquals(
                oracle.replay(Collections.singletonList(add)),
                recovered.snapshot());
        } finally {
            closeAndDrop(recoveredState);
        }
    }

    @Test
    public void testRecoverDiscardsUncheckpointedChanges() {
        MemoryEvent add = addEvent();
        MemoryEvent correction = correctEvent();
        Configuration configuration = stateConfiguration();
        KeyValueStateDescriptor<String, IncrementalTemporalIntegrator>
            descriptor = stateDescriptor();
        KeyValueState<String, IncrementalTemporalIntegrator> state =
            StateFactory.buildKeyValueState(descriptor, configuration);

        try {
            IncrementalTemporalIntegrator accumulator =
                new IncrementalTemporalIntegrator();
            accumulator.apply(add);
            state.manage().operate().setCheckpointId(CHECKPOINT_ID);
            state.put(FACT_ID, accumulator);
            state.manage().operate().finish();
            state.manage().operate().archive();

            state.manage().operate()
                .setCheckpointId(CHECKPOINT_ID + 1);
            IncrementalTemporalIntegrator uncheckpointed =
                state.get(FACT_ID);
            uncheckpointed.apply(correction);
            state.put(FACT_ID, uncheckpointed);
            Assertions.assertEquals(
                oracle.replay(Arrays.asList(add, correction)),
                state.get(FACT_ID).snapshot());

            state.manage().operate().setCheckpointId(CHECKPOINT_ID);
            state.manage().operate().recover();

            IncrementalTemporalIntegrator recovered =
                state.get(FACT_ID);
            Assertions.assertNotNull(recovered);
            Assertions.assertEquals(
                Collections.singletonList(add),
                recovered.eventSnapshot());
            Assertions.assertEquals(
                oracle.replay(Collections.singletonList(add)),
                recovered.snapshot());
        } finally {
            closeAndDrop(state);
        }
    }

    @Test
    public void testRecoveredAccumulatorHandlesLateCorrection() {
        MemoryEvent add = addEvent();
        MemoryEvent correction = correctEvent();
        MemoryEvent lateCorrection = lateCorrectionEvent();
        Configuration configuration = stateConfiguration();
        KeyValueStateDescriptor<String, IncrementalTemporalIntegrator>
            descriptor = stateDescriptor();
        KeyValueState<String, IncrementalTemporalIntegrator> originalState =
            StateFactory.buildKeyValueState(descriptor, configuration);

        try {
            IncrementalTemporalIntegrator accumulator =
                new IncrementalTemporalIntegrator();
            accumulator.apply(add);
            accumulator.apply(correction);
            originalState.manage().operate()
                .setCheckpointId(CHECKPOINT_ID);
            originalState.put(FACT_ID, accumulator);
            originalState.manage().operate().finish();
            originalState.manage().operate().archive();
        } finally {
            closeAndDrop(originalState);
        }

        KeyValueState<String, IncrementalTemporalIntegrator> recoveredState =
            StateFactory.buildKeyValueState(descriptor, configuration);
        try {
            recoveredState.manage().operate()
                .setCheckpointId(CHECKPOINT_ID);
            recoveredState.manage().operate().recover();

            IncrementalTemporalIntegrator recovered =
                recoveredState.get(FACT_ID);
            Assertions.assertNotNull(recovered);
            recovered.apply(lateCorrection);
            recoveredState.put(FACT_ID, recovered);

            IncrementalTemporalIntegrator updated =
                recoveredState.get(FACT_ID);
            Assertions.assertEquals(
                Arrays.asList(add, lateCorrection, correction),
                updated.eventSnapshot());
            Assertions.assertEquals(
                oracle.replay(Arrays.asList(
                    add,
                    correction,
                    lateCorrection)),
                updated.snapshot());
        } finally {
            closeAndDrop(recoveredState);
        }
    }

    @Test
    public void testContinuedUpdatesSurviveNextCheckpoint() {
        MemoryEvent add = addEvent();
        MemoryEvent correction = correctEvent();
        MemoryEvent lateCorrection = lateCorrectionEvent();
        Configuration configuration = stateConfiguration();
        KeyValueStateDescriptor<String, IncrementalTemporalIntegrator>
            descriptor = stateDescriptor();
        KeyValueState<String, IncrementalTemporalIntegrator> originalState =
            StateFactory.buildKeyValueState(descriptor, configuration);

        try {
            IncrementalTemporalIntegrator accumulator =
                new IncrementalTemporalIntegrator();
            accumulator.apply(add);
            accumulator.apply(correction);
            originalState.manage().operate()
                .setCheckpointId(CHECKPOINT_ID);
            originalState.put(FACT_ID, accumulator);
            originalState.manage().operate().finish();
            originalState.manage().operate().archive();
        } finally {
            closeAndDrop(originalState);
        }

        KeyValueState<String, IncrementalTemporalIntegrator> continuedState =
            StateFactory.buildKeyValueState(descriptor, configuration);
        try {
            continuedState.manage().operate()
                .setCheckpointId(CHECKPOINT_ID);
            continuedState.manage().operate().recover();

            IncrementalTemporalIntegrator recovered =
                continuedState.get(FACT_ID);
            Assertions.assertNotNull(recovered);
            recovered.apply(lateCorrection);
            continuedState.put(FACT_ID, recovered);
            continuedState.manage().operate()
                .setCheckpointId(CHECKPOINT_ID + 1);
            continuedState.manage().operate().finish();
            continuedState.manage().operate().archive();
        } finally {
            closeAndDrop(continuedState);
        }

        KeyValueState<String, IncrementalTemporalIntegrator> restoredState =
            StateFactory.buildKeyValueState(descriptor, configuration);
        try {
            restoredState.manage().operate()
                .setCheckpointId(CHECKPOINT_ID + 1);
            restoredState.manage().operate().recover();

            IncrementalTemporalIntegrator restored =
                restoredState.get(FACT_ID);
            Assertions.assertNotNull(restored);
            Assertions.assertEquals(
                Arrays.asList(add, lateCorrection, correction),
                restored.eventSnapshot());
            Assertions.assertEquals(
                oracle.replay(Arrays.asList(
                    add,
                    correction,
                    lateCorrection)),
                restored.snapshot());
        } finally {
            closeAndDrop(restoredState);
        }
    }

    @Test
    public void testConflictingEventAfterRecoveryIsAtomic() {
        MemoryEvent add = addEvent();
        MemoryEvent conflictingAdd = conflictingAddEvent();
        MemoryEvent correction = correctEvent();
        Configuration configuration = stateConfiguration();
        KeyValueStateDescriptor<String, IncrementalTemporalIntegrator>
            descriptor = stateDescriptor();
        KeyValueState<String, IncrementalTemporalIntegrator> originalState =
            StateFactory.buildKeyValueState(descriptor, configuration);

        try {
            IncrementalTemporalIntegrator accumulator =
                new IncrementalTemporalIntegrator();
            accumulator.apply(add);
            originalState.manage().operate()
                .setCheckpointId(CHECKPOINT_ID);
            originalState.put(FACT_ID, accumulator);
            originalState.manage().operate().finish();
            originalState.manage().operate().archive();
        } finally {
            closeAndDrop(originalState);
        }

        KeyValueState<String, IncrementalTemporalIntegrator> recoveredState =
            StateFactory.buildKeyValueState(descriptor, configuration);
        try {
            recoveredState.manage().operate()
                .setCheckpointId(CHECKPOINT_ID);
            recoveredState.manage().operate().recover();

            IncrementalTemporalIntegrator recovered =
                recoveredState.get(FACT_ID);
            Assertions.assertNotNull(recovered);
            Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> recovered.apply(conflictingAdd));
            Assertions.assertEquals(
                Collections.singletonList(add),
                recovered.eventSnapshot());
            Assertions.assertEquals(
                oracle.replay(Collections.singletonList(add)),
                recovered.snapshot());

            recovered.apply(correction);
            recoveredState.put(FACT_ID, recovered);
            IncrementalTemporalIntegrator updated =
                recoveredState.get(FACT_ID);
            Assertions.assertEquals(
                Arrays.asList(add, correction),
                updated.eventSnapshot());
            Assertions.assertEquals(
                oracle.replay(Arrays.asList(add, correction)),
                updated.snapshot());
        } finally {
            closeAndDrop(recoveredState);
        }
    }

    @Test
    public void testMultipleFactIdsRecoverIndependently() {
        MemoryEvent aliceAdd = addEvent();
        MemoryEvent aliceCorrection = correctEvent();
        MemoryEvent bobAdd = bobAddEvent();
        Configuration configuration = stateConfiguration();
        KeyValueStateDescriptor<String, IncrementalTemporalIntegrator>
            descriptor = stateDescriptor();
        KeyValueState<String, IncrementalTemporalIntegrator> originalState =
            StateFactory.buildKeyValueState(descriptor, configuration);

        try {
            IncrementalTemporalIntegrator aliceAccumulator =
                new IncrementalTemporalIntegrator();
            aliceAccumulator.apply(aliceAdd);
            IncrementalTemporalIntegrator bobAccumulator =
                new IncrementalTemporalIntegrator();
            bobAccumulator.apply(bobAdd);

            originalState.manage().operate()
                .setCheckpointId(CHECKPOINT_ID);
            originalState.put(FACT_ID, aliceAccumulator);
            originalState.put(BOB_FACT_ID, bobAccumulator);
            originalState.manage().operate().finish();
            originalState.manage().operate().archive();
        } finally {
            closeAndDrop(originalState);
        }

        KeyValueState<String, IncrementalTemporalIntegrator> recoveredState =
            StateFactory.buildKeyValueState(descriptor, configuration);
        try {
            recoveredState.manage().operate()
                .setCheckpointId(CHECKPOINT_ID);
            recoveredState.manage().operate().recover();

            IncrementalTemporalIntegrator recoveredAlice =
                recoveredState.get(FACT_ID);
            IncrementalTemporalIntegrator recoveredBob =
                recoveredState.get(BOB_FACT_ID);
            Assertions.assertNotNull(recoveredAlice);
            Assertions.assertNotNull(recoveredBob);
            Assertions.assertEquals(
                Collections.singletonList(aliceAdd),
                recoveredAlice.eventSnapshot());
            Assertions.assertEquals(
                oracle.replay(Collections.singletonList(aliceAdd)),
                recoveredAlice.snapshot());
            Assertions.assertEquals(
                Collections.singletonList(bobAdd),
                recoveredBob.eventSnapshot());
            Assertions.assertEquals(
                oracle.replay(Collections.singletonList(bobAdd)),
                recoveredBob.snapshot());

            recoveredAlice.apply(aliceCorrection);
            recoveredState.put(FACT_ID, recoveredAlice);

            IncrementalTemporalIntegrator updatedAlice =
                recoveredState.get(FACT_ID);
            IncrementalTemporalIntegrator unchangedBob =
                recoveredState.get(BOB_FACT_ID);
            Assertions.assertEquals(
                Arrays.asList(aliceAdd, aliceCorrection),
                updatedAlice.eventSnapshot());
            Assertions.assertEquals(
                oracle.replay(Arrays.asList(
                    aliceAdd,
                    aliceCorrection)),
                updatedAlice.snapshot());
            Assertions.assertEquals(
                Collections.singletonList(bobAdd),
                unchangedBob.eventSnapshot());
            Assertions.assertEquals(
                oracle.replay(Collections.singletonList(bobAdd)),
                unchangedBob.snapshot());
        } finally {
            closeAndDrop(recoveredState);
        }
    }

    @Test
    public void testDuplicateEventAfterRecoveryIsIdempotent() {
        MemoryEvent add = addEvent();
        Configuration configuration = stateConfiguration();
        KeyValueStateDescriptor<String, IncrementalTemporalIntegrator>
            descriptor = stateDescriptor();
        KeyValueState<String, IncrementalTemporalIntegrator> originalState =
            StateFactory.buildKeyValueState(descriptor, configuration);

        try {
            IncrementalTemporalIntegrator accumulator =
                new IncrementalTemporalIntegrator();
            accumulator.apply(add);
            originalState.manage().operate()
                .setCheckpointId(CHECKPOINT_ID);
            originalState.put(FACT_ID, accumulator);
            originalState.manage().operate().finish();
            originalState.manage().operate().archive();
        } finally {
            closeAndDrop(originalState);
        }

        KeyValueState<String, IncrementalTemporalIntegrator> recoveredState =
            StateFactory.buildKeyValueState(descriptor, configuration);
        try {
            recoveredState.manage().operate()
                .setCheckpointId(CHECKPOINT_ID);
            recoveredState.manage().operate().recover();

            IncrementalTemporalIntegrator recovered =
                recoveredState.get(FACT_ID);
            Assertions.assertNotNull(recovered);
            recovered.apply(add);
            recoveredState.put(FACT_ID, recovered);

            IncrementalTemporalIntegrator updated =
                recoveredState.get(FACT_ID);
            Assertions.assertEquals(
                Collections.singletonList(add),
                updated.eventSnapshot());
            Assertions.assertEquals(
                oracle.replay(Collections.singletonList(add)),
                updated.snapshot());
        } finally {
            closeAndDrop(recoveredState);
        }
    }

    @Test
    public void testRecoveredAccumulatorHandlesRetraction() {
        MemoryEvent add = addEvent();
        MemoryEvent correction = correctEvent();
        MemoryEvent retraction = retractEvent();
        Configuration configuration = stateConfiguration();
        KeyValueStateDescriptor<String, IncrementalTemporalIntegrator>
            descriptor = stateDescriptor();
        KeyValueState<String, IncrementalTemporalIntegrator> originalState =
            StateFactory.buildKeyValueState(descriptor, configuration);

        try {
            IncrementalTemporalIntegrator accumulator =
                new IncrementalTemporalIntegrator();
            accumulator.apply(add);
            accumulator.apply(correction);
            originalState.manage().operate()
                .setCheckpointId(CHECKPOINT_ID);
            originalState.put(FACT_ID, accumulator);
            originalState.manage().operate().finish();
            originalState.manage().operate().archive();
        } finally {
            closeAndDrop(originalState);
        }

        KeyValueState<String, IncrementalTemporalIntegrator> recoveredState =
            StateFactory.buildKeyValueState(descriptor, configuration);
        try {
            recoveredState.manage().operate()
                .setCheckpointId(CHECKPOINT_ID);
            recoveredState.manage().operate().recover();

            IncrementalTemporalIntegrator recovered =
                recoveredState.get(FACT_ID);
            Assertions.assertNotNull(recovered);
            recovered.apply(retraction);
            recoveredState.put(FACT_ID, recovered);

            IncrementalTemporalIntegrator updated =
                recoveredState.get(FACT_ID);
            Assertions.assertEquals(
                Arrays.asList(add, correction, retraction),
                updated.eventSnapshot());
            Assertions.assertEquals(
                oracle.replay(Arrays.asList(
                    add,
                    correction,
                    retraction)),
                updated.snapshot());
        } finally {
            closeAndDrop(recoveredState);
        }
    }

    private Configuration stateConfiguration() {
        Map<String, String> config = new HashMap<>();
        config.put(
            ExecutionConfigKeys.JOB_APP_NAME.getKey(),
            "TemporalStateRecoveryTest");
        config.put(
            ExecutionConfigKeys.JOB_WORK_PATH.getKey(),
            tempDirectory.resolve("work").toString());
        config.put(
            FileConfigKeys.PERSISTENT_TYPE.getKey(),
            "LOCAL");
        config.put(
            FileConfigKeys.ROOT.getKey(),
            tempDirectory.resolve("checkpoints").toString());
        return new Configuration(config);
    }

    private static KeyValueStateDescriptor<String,
        IncrementalTemporalIntegrator> stateDescriptor() {
        KeyValueStateDescriptor<String, IncrementalTemporalIntegrator>
            descriptor = KeyValueStateDescriptor.build(
                "temporal-recovery",
                StoreType.ROCKSDB.name());
        descriptor.withKeyGroup(new KeyGroup(0, 0))
            .withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        return descriptor;
    }

    private static void closeAndDrop(
        KeyValueState<String, IncrementalTemporalIntegrator> state) {
        state.manage().operate().close();
        state.manage().operate().drop();
    }

    private static MemoryEvent addEvent() {
        return addEvent("Beijing");
    }

    private static MemoryEvent conflictingAddEvent() {
        return addEvent("Shenzhen");
    }

    private static MemoryEvent bobAddEvent() {
        return MemoryEvent.add(
            "event-bob-add",
            fact(BOB_FACT_ID, "person:bob", "Paris"),
            TimeInterval.unboundedFrom(
                Instant.parse("2024-01-01T00:00:00Z")),
            Instant.parse("2024-04-01T00:00:00Z"),
            evidence("event-bob-add"));
    }

    private static MemoryEvent addEvent(String value) {
        return MemoryEvent.add(
            "event-alice-add",
            fact(value),
            TimeInterval.unboundedFrom(
                Instant.parse("2024-01-01T00:00:00Z")),
            Instant.parse("2024-03-01T00:00:00Z"),
            evidence("event-alice-add"));
    }

    private static MemoryEvent correctEvent() {
        return MemoryEvent.correct(
            "event-alice-correct",
            fact("Shanghai"),
            new TimeInterval(
                Instant.parse("2024-04-01T00:00:00Z"),
                Instant.parse("2024-09-01T00:00:00Z")),
            Instant.parse("2024-06-01T00:00:00Z"),
            evidence("event-alice-correct"));
    }

    private static MemoryEvent lateCorrectionEvent() {
        return MemoryEvent.correct(
            "event-alice-late",
            fact("Tianjin"),
            new TimeInterval(
                Instant.parse("2024-02-01T00:00:00Z"),
                Instant.parse("2024-03-01T00:00:00Z")),
            Instant.parse("2024-05-01T00:00:00Z"),
            evidence("event-alice-late"));
    }

    private static MemoryEvent retractEvent() {
        return MemoryEvent.retract(
            "event-alice-retract",
            FACT_ID,
            new TimeInterval(
                Instant.parse("2024-08-01T00:00:00Z"),
                Instant.parse("2024-10-01T00:00:00Z")),
            Instant.parse("2024-11-01T00:00:00Z"),
            evidence("event-alice-retract"));
    }

    private static MemoryFact fact(String value) {
        return fact(FACT_ID, "person:alice", value);
    }

    private static MemoryFact fact(
        String factId,
        String subjectId,
        String value) {
        return MemoryFact.attribute(
            factId,
            new MemoryEntity(subjectId, "person"),
            "city",
            value);
    }

    private static List<Evidence> evidence(String eventId) {
        return Collections.singletonList(new Evidence(
            "evidence-" + eventId,
            new Source("source-1", "customer-database"),
            "Evidence for " + eventId));
    }
}
