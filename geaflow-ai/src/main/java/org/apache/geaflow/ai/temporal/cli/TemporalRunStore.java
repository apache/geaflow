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

package org.apache.geaflow.ai.temporal.cli;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.ai.temporal.api.QueryMode;
import org.apache.geaflow.ai.temporal.api.TemporalMemoryFacade;
import org.apache.geaflow.ai.temporal.candidate.RetrievalProfile;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalErrorCode;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;
import org.apache.geaflow.ai.temporal.trace.DecisionTrace;
import org.apache.geaflow.ai.temporal.trace.QueryTrace;

/** Stores a reproducible journal and verifies every persisted result when loading. */
public final class TemporalRunStore {

    public static final int ARTIFACT_VERSION = 1;
    private final TemporalJsonCodec codec = new TemporalJsonCodec();

    public void save(Path path, TemporalMemoryFacade facade) throws IOException {
        writeAtomic(path, codec.toJson(artifact(facade)));
    }

    public TemporalMemoryFacade load(Path path) throws IOException {
        JsonObject stored = codec.parseObject(new String(Files.readAllBytes(path),
            StandardCharsets.UTF_8));
        fields(stored, "run", "artifactVersion", "schemaVersion", "operations", "snapshot",
            "decisionTraces", "queryTraces");
        version(stored, "artifactVersion", ARTIFACT_VERSION);
        version(stored, "schemaVersion", NormalizedMemoryEvent.SCHEMA_VERSION);
        JsonArray operations = array(stored.get("operations"), "operations");
        TemporalMemoryFacade facade = new TemporalMemoryFacade();
        for (JsonElement element : operations) {
            replay(facade, object(element, "operation"));
        }
        if (!stored.equals(artifact(facade))) {
            throw invalid("Run journal does not reproduce its snapshot and traces");
        }
        return facade;
    }

    private JsonObject artifact(TemporalMemoryFacade facade) {
        JsonObject value = new JsonObject();
        value.addProperty("artifactVersion", ARTIFACT_VERSION);
        value.addProperty("schemaVersion", NormalizedMemoryEvent.SCHEMA_VERSION);
        value.add("operations", facade.getOperations());
        value.add("snapshot", codec.snapshotToJson(facade.getSnapshot()));
        JsonArray decisions = new JsonArray();
        for (DecisionTrace trace : facade.getDecisionTraces()) {
            decisions.add(trace.toJson());
        }
        value.add("decisionTraces", decisions);
        JsonArray queries = new JsonArray();
        for (QueryTrace trace : facade.getQueryTraces()) {
            queries.add(trace.toJson());
        }
        value.add("queryTraces", queries);
        return value;
    }

    private void replay(TemporalMemoryFacade facade, JsonObject operation) {
        String method = text(operation, "method");
        if ("QUERY".equals(method)) {
            fields(operation, "query operation", "method", "mode", "factKey", "validAt", "systemAt");
            JsonObject key = object(operation.get("factKey"), "factKey");
            fields(key, "factKey", "subjectId", "predicate", "scope");
            QueryMode mode;
            try {
                mode = QueryMode.valueOf(text(operation, "mode"));
            } catch (IllegalArgumentException exception) {
                throw invalid("Unknown query mode in run journal");
            }
            String systemAt = nullableText(operation, "systemAt");
            facade.query(mode, new FactKey(text(key, "subjectId"), text(key, "predicate"),
                text(key, "scope")), parseTime(text(operation, "validAt"), "validAt"),
                systemAt == null ? null : parseTime(systemAt, "systemAt"));
            return;
        }
        if (!"REPLAY".equals(method) && !"APPLY_BATCH".equals(method)) {
            throw invalid("Unknown run operation: " + method);
        }
        final boolean decisionRetrieval = operation.has("candidateProfile") || operation.has("candidateTopK");
        final RetrievalProfile candidateProfile;
        final int candidateTopK;
        if (decisionRetrieval) {
            fields(operation, "event operation", "method", "events", "expectedError",
                "candidateProfile", "candidateTopK");
            try {
                candidateProfile = RetrievalProfile.valueOf(text(operation, "candidateProfile"));
                JsonElement topK = operation.get("candidateTopK");
                if (!topK.isJsonPrimitive() || !topK.getAsJsonPrimitive().isNumber()) {
                    throw invalid("candidateTopK must be a positive integer");
                }
                candidateTopK = new BigDecimal(topK.getAsString()).intValueExact();
            } catch (IllegalArgumentException | ArithmeticException exception) {
                throw invalid("Invalid decision retrieval parameters in run journal");
            }
            if (!candidateProfile.isCandidateStrategy() || candidateTopK <= 0) {
                throw invalid("Invalid decision retrieval strategy or topK in run journal");
            }
        } else {
            fields(operation, "event operation", "method", "events", "expectedError");
            candidateProfile = null;
            candidateTopK = 20;
        }
        List<NormalizedMemoryEvent> events = new ArrayList<>();
        for (JsonElement element : array(operation.get("events"), "events")) {
            events.add(codec.decodeEvent(object(element, "event")));
        }
        String expectedError = nullableText(operation, "expectedError");
        if (expectedError != null) {
            try {
                TemporalErrorCode.valueOf(expectedError);
            } catch (IllegalArgumentException exception) {
                throw invalid("Unknown expectedError in run journal: " + expectedError);
            }
        }
        String actualError = null;
        try {
            if ("REPLAY".equals(method)) {
                if (decisionRetrieval) {
                    facade.replay(events, candidateProfile, candidateTopK);
                } else {
                    facade.replay(events);
                }
            } else {
                if (decisionRetrieval) {
                    facade.applyBatch(events, candidateProfile, candidateTopK);
                } else {
                    facade.applyBatch(events);
                }
            }
        } catch (TemporalValidationException exception) {
            actualError = exception.getCode().name();
        }
        if (expectedError == null ? actualError != null : !expectedError.equals(actualError)) {
            throw invalid("Run operation outcome differs from expectedError: " + method);
        }
    }

    static void writeAtomic(Path path, String json) throws IOException {
        Path target = path.toAbsolutePath().normalize();
        if (Files.exists(target) && !Files.isRegularFile(target)) {
            throw new IOException("Output must be a regular file: " + target);
        }
        Path directory = target.getParent();
        Files.createDirectories(directory);
        Path temporary = Files.createTempFile(directory, ".temporal-run-", ".tmp");
        try {
            Files.write(temporary, (json + "\n").getBytes(StandardCharsets.UTF_8));
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException exception) {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    static Instant parseTime(String value, String name) {
        if (!value.endsWith("Z")) {
            throw invalid(name + " must be an ISO-8601 UTC timestamp ending in Z");
        }
        try {
            return OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeParseException exception) {
            throw invalid(name + " is not a valid ISO-8601 UTC timestamp");
        }
    }

    private static void fields(JsonObject object, String context, String... allowed) {
        Set<String> names = new HashSet<>(Arrays.asList(allowed));
        for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
            if (!names.contains(entry.getKey())) {
                throw invalid("Unknown " + context + " field: " + entry.getKey());
            }
        }
        for (String name : allowed) {
            if (!object.has(name)) {
                throw invalid("Missing " + context + " field: " + name);
            }
        }
    }

    private static JsonObject object(JsonElement value, String name) {
        if (value == null || !value.isJsonObject()) {
            throw invalid(name + " must be an object");
        }
        return value.getAsJsonObject();
    }

    private static JsonArray array(JsonElement value, String name) {
        if (value == null || !value.isJsonArray()) {
            throw invalid(name + " must be an array");
        }
        return value.getAsJsonArray();
    }

    private static String text(JsonObject value, String name) {
        String text = nullableText(value, name);
        if (text == null) {
            throw invalid(name + " must not be null");
        }
        return text;
    }

    private static String nullableText(JsonObject value, String name) {
        JsonElement element = value.get(name);
        if (element == null) {
            throw invalid("Missing required field: " + name);
        }
        if (element.isJsonNull()) {
            return null;
        }
        if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isString()
            || element.getAsString().trim().isEmpty()) {
            throw invalid(name + " must be a nonblank string");
        }
        return element.getAsString();
    }

    private static void version(JsonObject value, String name, int expected) {
        JsonElement element = value.get(name);
        if (element != null && element.isJsonPrimitive()
            && element.getAsJsonPrimitive().isNumber()) {
            try {
                if (element.getAsBigDecimal().intValueExact() == expected) {
                    return;
                }
            } catch (ArithmeticException exception) {
                throw invalid(name + " must be an integer");
            }
        }
        throw new TemporalValidationException(TemporalErrorCode.REJECT_SCHEMA_VERSION,
            "Unsupported " + name + "; expected " + expected);
    }

    private static TemporalValidationException invalid(String message) {
        return new TemporalValidationException(TemporalErrorCode.REJECT_INPUT, message);
    }
}
