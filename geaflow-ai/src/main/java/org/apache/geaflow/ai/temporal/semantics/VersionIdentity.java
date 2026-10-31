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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.TimeInterval;

/** Stable serialization of version identity, shared without state transition logic. */
public final class VersionIdentity {

    private VersionIdentity() {
    }

    public static String create(
        NormalizedMemoryEvent event,
        MemoryFact fact,
        MemoryFactVersionStatus status,
        TimeInterval validTime,
        String sourceVersionId) {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(fact, "fact");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(validTime, "validTime");
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
        field(digest, "temporal-version-v2");
        field(digest, event.getEventId());
        field(digest, event.getPayloadHash());
        field(digest, fact.getId());
        field(digest, fact.getSubject().getId());
        field(digest, fact.getSubject().getLabel());
        field(digest, fact.getPredicate());
        field(digest, fact.isRelationship() ? "ENTITY_REF" : "LITERAL");
        field(digest, fact.isRelationship()
            ? fact.getTarget().get().getId() : fact.getLiteralValue().get());
        field(digest, status.name());
        field(digest, validTime.getStart().toString());
        field(digest, validTime.getEnd().map(Object::toString).orElse(null));
        field(digest, sourceVersionId);
        StringBuilder hex = new StringBuilder();
        for (byte value : digest.digest()) {
            hex.append(Character.forDigit((value & 0xff) >>> 4, 16));
            hex.append(Character.forDigit(value & 0x0f, 16));
        }
        return event.getEventId() + ":version:v2:" + hex;
    }

    private static void field(MessageDigest digest, String value) {
        if (value == null) {
            digest.update((byte) 0);
            return;
        }
        digest.update((byte) 1);
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }
}
