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
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TargetNormalizationTest {

    private static final FactKey KEY = new FactKey("alice", "city", "profile");
    private static final MemoryFact FACT = MemoryFact.attribute(
        "fact", new MemoryEntity("alice", "person"), "city", "Beijing");
    private static final Instant RECORDED = Instant.parse("2024-03-01T00:00:00.123456789Z");
    private static final TimeInterval VALID = TimeInterval.unboundedFrom(
        Instant.parse("2024-01-01T00:00:00.000000001Z"));
    private final EventNormalizer normalizer = new EventNormalizer();

    @Test
    public void testNormalizedChangeRequiresExplicitTargetButRawFactoriesRemainCompatible() {
        MemoryEvent correct = correction();
        MemoryEvent retract = MemoryEvent.retract("retract", "fact", VALID, RECORDED,
            Collections.singletonList(proof()));
        for (MemoryEvent event : new MemoryEvent[] {correct, retract}) {
            Assertions.assertFalse(event.getTargetVersionId().isPresent());
            TemporalValidationException failure = Assertions.assertThrows(
                TemporalValidationException.class, () -> normalizer.normalize(event, KEY));
            Assertions.assertEquals(TemporalErrorCode.REJECT_TARGET_REQUIRED, failure.getCode());
        }
    }

    @Test
    public void testTargetIsImmutableCanonicalAndPartOfPayloadIdentity() {
        MemoryEvent original = correction();
        MemoryEvent targeted = original.targeting("target-e\u0301");
        NormalizedMemoryEvent first = normalizer.normalize(targeted, KEY);
        NormalizedMemoryEvent equivalent = normalizer.normalize(
            original.targeting("target-\u00e9"), KEY);
        NormalizedMemoryEvent different = normalizer.normalize(
            original.targeting("other-target"), KEY);
        Assertions.assertFalse(original.getTargetVersionId().isPresent());
        Assertions.assertNotEquals(original, targeted);
        Assertions.assertEquals(first, equivalent);
        Assertions.assertEquals("target-\u00e9", first.getTargetVersionId().get());
        Assertions.assertNotEquals(first.getPayloadHash(), different.getPayloadHash());
        Assertions.assertEquals(RECORDED, first.getRecordedAt());
        Assertions.assertEquals(VALID, first.getValidTime());
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> original.targeting(" "));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> MemoryEvent.add("add", FACT, VALID, RECORDED,
                Collections.singletonList(proof())).targeting("target"));
    }

    @Test
    public void testSemanticVersionIdentityCannotRebindToDifferentResidual() {
        NormalizedMemoryEvent event = normalizer.normalize(correction().targeting("source"), KEY);
        String identity = VersionIdentity.create(event, FACT,
            MemoryFactVersionStatus.ACTIVE, VALID, "source");
        Assertions.assertEquals(identity, VersionIdentity.create(event, FACT,
            MemoryFactVersionStatus.ACTIVE, VALID, "source"));
        Assertions.assertNotEquals(identity, VersionIdentity.create(event, FACT,
            MemoryFactVersionStatus.ACTIVE, VALID, "another-source"));
        Assertions.assertNotEquals(identity, VersionIdentity.create(event, FACT,
            MemoryFactVersionStatus.RETRACTED, VALID, "source"));
        Assertions.assertNotEquals(identity, VersionIdentity.create(event, FACT,
            MemoryFactVersionStatus.ACTIVE, TimeInterval.unboundedFrom(
                VALID.getStart().plusNanos(1)), "source"));
        MemoryFact other = MemoryFact.attribute(
            "fact", new MemoryEntity("alice", "person"), "city", "Shanghai");
        Assertions.assertNotEquals(identity, VersionIdentity.create(event, other,
            MemoryFactVersionStatus.ACTIVE, VALID, "source"));
        Assertions.assertTrue(identity.startsWith("correct:version:v2:"));
    }

    private static MemoryEvent correction() {
        return MemoryEvent.correct("correct", FACT, VALID, RECORDED,
            Collections.singletonList(proof()));
    }

    private static Evidence proof() {
        return new Evidence("proof", new Source("registry", "Registry"), "source text");
    }
}
