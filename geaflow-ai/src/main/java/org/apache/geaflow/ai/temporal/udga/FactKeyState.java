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

package org.apache.geaflow.ai.temporal.udga;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.apache.geaflow.ai.temporal.integration.IncrementalTemporalIntegrator;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.common.type.primitive.BooleanType;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.common.type.primitive.StringType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;

/** Immutable, independently recoverable timeline for one FactKey. */
public final class FactKeyState {

    private final FactKey key;
    private final CanonicalSnapshot snapshot;

    private FactKeyState(FactKey key, List<NormalizedMemoryEvent> events) {
        this.key = Objects.requireNonNull(key, "key");
        for (NormalizedMemoryEvent event : events) {
            if (!key.equals(event.getFactKey())) {
                throw new IllegalArgumentException("Timeline belongs to another FactKey");
            }
        }
        IncrementalTemporalIntegrator integrator = new IncrementalTemporalIntegrator();
        integrator.applyBatch(events);
        snapshot = integrator.canonicalSnapshot();
    }

    public static FactKeyState empty(FactKey key) {
        return new FactKeyState(key, Collections.emptyList());
    }

    public FactKeyState applyBatch(List<NormalizedMemoryEvent> events) {
        List<NormalizedMemoryEvent> merged = new ArrayList<>(snapshot.getEvents());
        merged.addAll(events);
        return new FactKeyState(key, merged);
    }

    public FactKey getKey() {
        return key;
    }

    public CanonicalSnapshot getSnapshot() {
        return snapshot;
    }

    public String getPayload() {
        JsonObject value = new JsonObject();
        value.add("key", new TemporalJsonCodec().factKeyToJson(key));
        JsonArray events = new JsonArray();
        for (NormalizedMemoryEvent event : snapshot.getEvents()) {
            events.add(new TemporalJsonCodec().eventToJson(event));
        }
        value.add("events", events);
        return value.toString();
    }

    public String getDigest() {
        return digest(new TemporalJsonCodec().factKeyToJson(key).toString()
            + new TemporalJsonCodec().snapshotToJson(snapshot).toString());
    }

    static String digest(String content) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(
                content.getBytes(StandardCharsets.UTF_8));
            StringBuilder value = new StringBuilder();
            for (byte item : hash) {
                value.append(String.format(java.util.Locale.ROOT, "%02x", item & 0xff));
            }
            return value.toString();
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private String watermark(boolean eventId) {
        List<NormalizedMemoryEvent> events = snapshot.getEvents();
        if (events.isEmpty()) {
            return "";
        }
        NormalizedMemoryEvent last = events.get(events.size() - 1);
        return eventId ? last.getEventId() : last.getRecordedAt().toString();
    }

    public Row toRow() {
        return ObjectRow.create(1, watermark(false), watermark(true), getPayload(),
            getDigest(), false);
    }

    public String encode() {
        JsonObject value = new JsonObject();
        value.addProperty("schemaVersion", 1);
        value.addProperty("watermarkRecordedAt", watermark(false));
        value.addProperty("watermarkEventId", watermark(true));
        value.addProperty("timelinePayload", getPayload());
        value.addProperty("digest", getDigest());
        value.addProperty("dirtyFlag", false);
        return TemporalGraphSchema.encode(value.toString());
    }

    public static FactKeyState decode(String encoded) {
        JsonObject value = new TemporalJsonCodec().parseObject(TemporalGraphSchema.decode(encoded));
        JsonObject payload = new TemporalJsonCodec().parseObject(
            value.get("timelinePayload").getAsString());
        FactKey key = key(payload.getAsJsonObject("key"));
        return fromRow(key, ObjectRow.create(value.get("schemaVersion").getAsInt(),
            value.get("watermarkRecordedAt").getAsString(),
            value.get("watermarkEventId").getAsString(), value.get("timelinePayload").getAsString(),
            value.get("digest").getAsString(), value.get("dirtyFlag").getAsBoolean()));
    }

    public static FactKeyState fromRow(FactKey key, Row row) {
        if (!Integer.valueOf(1).equals(row.getField(0, IntegerType.INSTANCE))
            || !Boolean.FALSE.equals(row.getField(5, BooleanType.INSTANCE))) {
            throw new IllegalArgumentException("Unsupported or uncommitted FactKey Row");
        }
        String payload = (String) row.getField(3, StringType.INSTANCE);
        JsonObject value = new TemporalJsonCodec().parseObject(payload);
        if (!key.equals(key(value.getAsJsonObject("key")))) {
            throw new IllegalArgumentException("Row key does not match vertex key");
        }
        List<NormalizedMemoryEvent> events = new ArrayList<>();
        for (JsonElement event : value.getAsJsonArray("events")) {
            events.add(new TemporalJsonCodec().decodeEvent(event.getAsJsonObject()));
        }
        FactKeyState result = new FactKeyState(key, events);
        if (!result.getDigest().equals(row.getField(4, StringType.INSTANCE))
            || !result.watermark(false).equals(row.getField(1, StringType.INSTANCE))
            || !result.watermark(true).equals(row.getField(2, StringType.INSTANCE))
            || !result.getPayload().equals(payload)) {
            throw new IllegalArgumentException("FactKey Row integrity check failed");
        }
        return result;
    }

    static FactKey key(JsonObject key) {
        return new FactKey(key.get("subjectId").getAsString(), key.get("predicate").getAsString(),
            key.get("scope").getAsString());
    }
}
