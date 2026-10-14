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
import java.text.Normalizer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.FactValue;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryEventOperation;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;

/**
 * Canonicalizes temporal events before replay or ledger checks.
 */
public final class EventNormalizer {

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private static final Comparator<Evidence> EVIDENCE_ORDER =
        Comparator.comparing(Evidence::getId)
            .thenComparing(evidence -> evidence.getSource().getId())
            .thenComparing(evidence -> evidence.getSource().getName())
            .thenComparing(Evidence::getContent);

    public NormalizedMemoryEvent normalize(
        MemoryEvent event,
        FactKey factKey) {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(factKey, "factKey");

        FactKey normalizedKey = new FactKey(
            normalizeText(factKey.getSubjectId()),
            normalizeText(factKey.getPredicate()),
            normalizeText(factKey.getScope()));
        String eventId = normalizeText(event.getId());
        String factId = normalizeText(event.getFactId());
        TimeInterval validTime = normalizeInterval(
            event.getValidTime());
        Instant recordedAt = normalizeTime(
            event.getTransactionTime());
        List<Evidence> evidence = normalizeEvidence(
            event.getEvidence());
        MemoryFact fact = normalizeFact(event, factId);

        validateFactKey(event.getOperation(), fact, normalizedKey);
        MemoryEvent normalizedEvent = createEvent(
            eventId,
            event.getOperation(),
            factId,
            fact,
            validTime,
            recordedAt,
            evidence);
        FactValue factValue = createFactValue(fact);
        String payloadHash = payloadHash(
            normalizedEvent,
            normalizedKey,
            factValue);

        return new NormalizedMemoryEvent(
            normalizedEvent,
            normalizedKey,
            factValue,
            payloadHash);
    }

    private static MemoryFact normalizeFact(
        MemoryEvent event,
        String factId) {
        Optional<MemoryFact> optionalFact = event.getFact();
        if (!optionalFact.isPresent()) {
            if (event.getOperation() != MemoryEventOperation.RETRACT) {
                throw new IllegalArgumentException(
                    "Event operation requires a fact");
            }
            return null;
        }

        MemoryFact fact = optionalFact.get();
        MemoryEntity subject = normalizeEntity(fact.getSubject());
        String predicate = normalizeText(fact.getPredicate());
        if (fact.isRelationship()) {
            return MemoryFact.relationship(
                factId,
                subject,
                predicate,
                normalizeEntity(fact.getTarget().get()));
        }
        return MemoryFact.attribute(
            factId,
            subject,
            predicate,
            normalizeText(fact.getLiteralValue().get()));
    }

    private static MemoryEntity normalizeEntity(
        MemoryEntity entity) {
        return new MemoryEntity(
            normalizeText(entity.getId()),
            normalizeText(entity.getLabel()));
    }

    private static List<Evidence> normalizeEvidence(
        List<Evidence> evidence) {
        List<Evidence> normalized = new ArrayList<>();
        for (Evidence item : evidence) {
            Source source = item.getSource();
            normalized.add(new Evidence(
                normalizeText(item.getId()),
                new Source(
                    normalizeText(source.getId()),
                    normalizeText(source.getName())),
                normalizeText(item.getContent())));
        }
        Collections.sort(normalized, EVIDENCE_ORDER);
        return normalized;
    }

    private static TimeInterval normalizeInterval(
        TimeInterval interval) {
        Instant end = interval.getEnd().isPresent()
            ? normalizeTime(interval.getEnd().get()) : null;
        return new TimeInterval(
            normalizeTime(interval.getStart()),
            end);
    }

    private static Instant normalizeTime(Instant time) {
        return time;
    }

    private static String normalizeText(String value) {
        return Normalizer.normalize(value, Normalizer.Form.NFC);
    }

    private static void validateFactKey(
        MemoryEventOperation operation,
        MemoryFact fact,
        FactKey factKey) {
        if (operation == MemoryEventOperation.RETRACT) {
            if (fact != null) {
                throw new IllegalArgumentException(
                    "Retract event must not contain a fact");
            }
            return;
        }
        if (fact == null
            || !fact.getSubject().getId().equals(
                factKey.getSubjectId())
            || !fact.getPredicate().equals(
                factKey.getPredicate())) {
            throw new IllegalArgumentException(
                "Fact key does not match event fact");
        }
    }

    private static MemoryEvent createEvent(
        String eventId,
        MemoryEventOperation operation,
        String factId,
        MemoryFact fact,
        TimeInterval validTime,
        Instant recordedAt,
        List<Evidence> evidence) {
        if (operation == MemoryEventOperation.ADD) {
            return MemoryEvent.add(
                eventId,
                fact,
                validTime,
                recordedAt,
                evidence);
        }
        if (operation == MemoryEventOperation.CORRECT) {
            return MemoryEvent.correct(
                eventId,
                fact,
                validTime,
                recordedAt,
                evidence);
        }
        if (operation == MemoryEventOperation.RETRACT) {
            return MemoryEvent.retract(
                eventId,
                factId,
                validTime,
                recordedAt,
                evidence);
        }
        throw new UnsupportedOperationException(
            "Unsupported memory event operation: " + operation);
    }

    private static FactValue createFactValue(MemoryFact fact) {
        if (fact == null) {
            return null;
        }
        if (fact.isRelationship()) {
            return FactValue.entityReference(
                fact.getTarget().get().getId());
        }
        return FactValue.literal(
            fact.getLiteralValue().get());
    }

    private static String payloadHash(
        MemoryEvent event,
        FactKey factKey,
        FactValue factValue) {
        MessageDigest digest = sha256();
        updateText(digest, event.getOperation().name());
        updateText(digest, event.getFactId());
        updateText(digest, factKey.getSubjectId());
        updateText(digest, factKey.getPredicate());
        updateText(digest, factKey.getScope());
        updateText(
            digest,
            factValue == null ? null : factValue.getKind().name());
        updateText(
            digest,
            factValue == null ? null : factValue.getValue());

        MemoryFact fact = event.getFact().orElse(null);
        updateText(
            digest,
            fact == null ? null : fact.getSubject().getLabel());
        updateText(
            digest,
            fact != null && fact.isRelationship()
                ? fact.getTarget().get().getLabel() : null);
        updateTime(
            digest,
            event.getValidTime().getStart());
        updateOptionalTime(
            digest,
            event.getValidTime().getEnd());
        updateTime(
            digest,
            event.getTransactionTime());
        updateInt(digest, event.getEvidence().size());
        for (Evidence evidence : event.getEvidence()) {
            updateText(digest, evidence.getId());
            updateText(digest, evidence.getSource().getId());
            updateText(digest, evidence.getSource().getName());
            updateText(digest, evidence.getContent());
        }
        return toHex(digest.digest());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(
                "SHA-256 is unavailable",
                exception);
        }
    }

    private static void updateText(
        MessageDigest digest,
        String value) {
        if (value == null) {
            digest.update((byte) 0);
            return;
        }
        digest.update((byte) 1);
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        updateInt(digest, bytes.length);
        digest.update(bytes);
    }

    private static void updateOptionalTime(
        MessageDigest digest,
        Optional<Instant> time) {
        if (time.isPresent()) {
            digest.update((byte) 1);
            updateTime(digest, time.get());
        } else {
            digest.update((byte) 0);
        }
    }

    private static void updateTime(
        MessageDigest digest,
        Instant time) {
        updateLong(digest, time.getEpochSecond());
        updateInt(digest, time.getNano());
    }

    private static void updateInt(
        MessageDigest digest,
        int value) {
        digest.update(ByteBuffer.allocate(Integer.BYTES)
            .putInt(value)
            .array());
    }

    private static void updateLong(
        MessageDigest digest,
        long value) {
        digest.update(ByteBuffer.allocate(Long.BYTES)
            .putLong(value)
            .array());
    }

    private static String toHex(byte[] bytes) {
        char[] characters = new char[bytes.length * 2];
        for (int index = 0; index < bytes.length; index++) {
            int value = bytes[index] & 0xff;
            characters[index * 2] = HEX[value >>> 4];
            characters[index * 2 + 1] = HEX[value & 0x0f];
        }
        return new String(characters);
    }
}
