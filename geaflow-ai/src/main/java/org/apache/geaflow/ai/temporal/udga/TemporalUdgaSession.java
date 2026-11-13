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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.geaflow.ai.temporal.adapter.MemoryGraphAdapter;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.apache.geaflow.ai.temporal.integration.IncrementalTemporalIntegrator;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.VersionRelation;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.EventLedgerDecision;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.SnapshotComparator;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;

/** Finite-batch experimental entry point. Only validated Local Mode output is published. */
public final class TemporalUdgaSession {

    private final Path workDirectory;
    private Map<FactKey, FactKeyState> states = new TreeMap<>();
    private CanonicalSnapshot snapshot = combine(states);

    public TemporalUdgaSession(Path workDirectory) {
        this.workDirectory = java.util.Objects.requireNonNull(workDirectory, "workDirectory");
    }

    public synchronized CanonicalSnapshot applyBatch(List<NormalizedMemoryEvent> events)
        throws Exception {
        IncrementalTemporalIntegrator validation = new IncrementalTemporalIntegrator();
        validation.applyBatch(snapshot.getEvents());
        List<EventLedgerDecision> decisions = validation.applyBatch(events);
        Map<FactKey, FactKeyState> affected = new TreeMap<>();
        List<NormalizedMemoryEvent> accepted = new ArrayList<>();
        // applyBatch returns decisions in canonical event order.
        List<NormalizedMemoryEvent> ordered = new ArrayList<>(events);
        ordered.sort(java.util.Comparator.comparing(NormalizedMemoryEvent::getRecordedAt)
            .thenComparing(NormalizedMemoryEvent::getEventId));
        for (int index = 0; index < ordered.size(); index++) {
            if (decisions.get(index) == EventLedgerDecision.ACCEPTED) {
                NormalizedMemoryEvent event = ordered.get(index);
                accepted.add(event);
                affected.put(event.getFactKey(), states.getOrDefault(event.getFactKey(),
                    FactKeyState.empty(event.getFactKey())));
            }
        }
        if (accepted.isEmpty()) {
            return snapshot;
        }
        Files.createDirectories(workDirectory);
        Path attempt = Files.createTempDirectory(workDirectory, "batch-");
        List<String> rows = TemporalLocalRunner.run(attempt, affected, accepted, -1);
        Map<FactKey, FactKeyState> staged = new TreeMap<>(states);
        java.util.Set<FactKey> received = new java.util.HashSet<>();
        for (String row : rows) {
            String[] fields = row.split(",", -1);
            if (fields.length != 3) {
                throw new IOException("Malformed Local Mode output: " + row);
            }
            if ("RESULT".equals(fields[2])) {
                FactKeyState state = FactKeyState.decode(fields[1]);
                if (!affected.containsKey(state.getKey()) || !received.add(state.getKey())
                    || !TemporalGraphSchema.factId(state.getKey()).equals(fields[0])) {
                    throw new IOException("Unexpected or duplicate FactKey output");
                }
                staged.put(state.getKey(), state);
            }
        }
        if (!received.equals(affected.keySet())) {
            throw new IOException("Incomplete Local Mode result");
        }
        CanonicalSnapshot result = combine(staged);
        MemoryGraphAdapter adapter = new MemoryGraphAdapter();
        result = adapter.fromGraph(adapter.toGraph(result));
        if (!new SnapshotComparator().compare(validation.canonicalSnapshot(), result).isEquivalent()) {
            throw new IOException("Local Mode output differs from validated batch");
        }
        states = staged;
        snapshot = result;
        return snapshot;
    }

    public synchronized CanonicalSnapshot getSnapshot() {
        return snapshot;
    }

    public synchronized String checkpoint() {
        JsonObject value = new JsonObject();
        value.addProperty("schemaVersion", 1);
        JsonArray rows = new JsonArray();
        for (FactKeyState state : states.values()) {
            rows.add(new com.google.gson.JsonPrimitive(state.encode()));
        }
        value.add("rows", rows);
        value.addProperty("digest", FactKeyState.digest(rows.toString()));
        return value.toString();
    }

    public synchronized void save(Path path) throws IOException {
        Path target = path.toAbsolutePath();
        Path temporary = Files.createTempFile(target.getParent(), "temporal-", ".tmp");
        try {
            Files.write(temporary, checkpoint().getBytes(StandardCharsets.UTF_8));
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException unsupported) {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    public static TemporalUdgaSession load(Path directory, Path checkpoint) throws IOException {
        return restore(directory, new String(Files.readAllBytes(checkpoint), StandardCharsets.UTF_8));
    }

    public static TemporalUdgaSession restore(Path directory, String checkpoint) {
        JsonObject value = new TemporalJsonCodec().parseObject(checkpoint);
        if (!value.has("schemaVersion") || !value.has("rows") || !value.has("digest")
            || value.get("schemaVersion").getAsBigDecimal().intValueExact() != 1
            || !FactKeyState.digest(value.get("rows").toString())
                .equals(value.get("digest").getAsString())) {
            throw new IllegalArgumentException("Unsupported session schema");
        }
        TemporalUdgaSession result = new TemporalUdgaSession(directory);
        for (JsonElement encoded : value.getAsJsonArray("rows")) {
            FactKeyState state = FactKeyState.decode(encoded.getAsString());
            if (result.states.put(state.getKey(), state) != null) {
                throw new IllegalArgumentException("Duplicate checkpoint FactKey");
            }
        }
        CanonicalSnapshot combined = combine(result.states);
        IncrementalTemporalIntegrator validation = new IncrementalTemporalIntegrator();
        validation.applyBatch(combined.getEvents());
        MemoryGraphAdapter adapter = new MemoryGraphAdapter();
        result.snapshot = adapter.fromGraph(adapter.toGraph(combined));
        if (!new TemporalJsonCodec().parseObject(result.checkpoint()).equals(value)) {
            throw new IllegalArgumentException("Noncanonical session checkpoint");
        }
        return result;
    }

    private static CanonicalSnapshot combine(Map<FactKey, FactKeyState> states) {
        Map<FactKey, List<MemoryFactVersion>> versions = new TreeMap<>();
        List<VersionRelation> relations = new ArrayList<>();
        List<NormalizedMemoryEvent> events = new ArrayList<>();
        Map<String, String> generating = new TreeMap<>();
        for (FactKeyState state : states.values()) {
            CanonicalSnapshot part = state.getSnapshot();
            versions.putAll(part.getState().getVersionsByFactKey());
            relations.addAll(part.getState().getRelations());
            events.addAll(part.getEvents());
            generating.putAll(part.getGeneratingEventIds());
        }
        return new CanonicalSnapshot(new TemporalState(versions, relations), events, generating);
    }
}
