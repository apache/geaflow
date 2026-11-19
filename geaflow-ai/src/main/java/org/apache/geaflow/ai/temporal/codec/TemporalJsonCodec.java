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

package org.apache.geaflow.ai.temporal.codec;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryEventOperation;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.model.VersionRelation;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalErrorCode;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;

/** Strict schema version 2 JSON boundary; immutable models use their public constructors. */
public final class TemporalJsonCodec {

    private static final Gson GSON = new GsonBuilder().serializeNulls()
        .disableHtmlEscaping().create();
    private final EventNormalizer normalizer = new EventNormalizer();

    public NormalizedMemoryEvent decodeEvent(String json) {
        return decodeEvent(parseObject(json));
    }

    public NormalizedMemoryEvent decodeEvent(JsonObject json) {
        try {
            Objects.requireNonNull(json, "event");
            fields(json, "event", "schemaVersion", "id", "operation", "subject",
                "predicate", "scope", "factId", "value", "validFrom", "validTo",
                "recordedAt", "evidence", "target", "payloadHash");
            schemaVersion(json);
            String operation = text(json, "operation");
            if (!Arrays.asList("ASSERT", "CORRECT", "RETRACT").contains(operation)) {
                throw invalid("operation must be ASSERT, CORRECT, or RETRACT");
            }
            JsonObject subject = object(required(json, "subject"), "subject");
            fields(subject, "subject", "id", "label");
            String subjectId = text(subject, "id");
            String subjectLabel = "RETRACT".equals(operation)
                ? nullableText(required(subject, "label"), "subject.label") : text(subject, "label");
            String predicate = text(json, "predicate");
            final FactKey key = new FactKey(subjectId, predicate, text(json, "scope"));
            String factId = text(json, "factId");
            JsonElement value = required(json, "value");
            TimeInterval validTime = new TimeInterval(time(required(json, "validFrom"), "validFrom"),
                nullableTime(required(json, "validTo"), "validTo"));
            Instant recordedAt = time(required(json, "recordedAt"), "recordedAt");
            List<Evidence> evidence = decodeEvidence(required(json, "evidence"));
            String target = json.has("target") ? nullableText(json.get("target"), "target") : null;
            if ("ASSERT".equals(operation) && target != null) {
                throw invalid("ASSERT must not contain a target");
            }
            if (!"ASSERT".equals(operation) && target == null) {
                throw new TemporalValidationException(TemporalErrorCode.REJECT_TARGET_REQUIRED,
                    "CORRECT/RETRACT requires an explicit target version ID");
            }
            MemoryEvent event;
            if ("RETRACT".equals(operation)) {
                if (!value.isJsonNull()) {
                    throw invalid("RETRACT value must be null");
                }
                event = MemoryEvent.retract(text(json, "id"), factId, validTime, recordedAt, evidence);
            } else {
                MemoryFact fact = decodeFact(value, factId,
                    new MemoryEntity(subjectId, subjectLabel), predicate);
                event = "ASSERT".equals(operation)
                    ? MemoryEvent.add(text(json, "id"), fact, validTime, recordedAt, evidence)
                    : MemoryEvent.correct(text(json, "id"), fact, validTime, recordedAt, evidence);
            }
            if (target != null) {
                event = event.targeting(target);
            }
            NormalizedMemoryEvent normalized = normalizer.normalize(event, key);
            if (json.has("payloadHash")
                && !text(json, "payloadHash").equals(normalized.getPayloadHash())) {
                throw invalid("payloadHash does not match the normalized event");
            }
            return normalized;
        } catch (TemporalValidationException exception) {
            throw exception;
        } catch (IllegalArgumentException | NullPointerException exception) {
            throw invalid(exception.getMessage());
        }
    }

    public JsonObject eventToJson(NormalizedMemoryEvent event) {
        Objects.requireNonNull(event, "event");
        JsonObject json = new JsonObject();
        json.addProperty("schemaVersion", NormalizedMemoryEvent.SCHEMA_VERSION);
        json.addProperty("id", event.getEventId());
        json.addProperty("operation", event.getOperation() == MemoryEventOperation.ADD
            ? "ASSERT" : event.getOperation().name());
        MemoryFact fact = event.getEvent().getFact().orElse(null);
        json.add("subject", subjectToJson(event.getFactKey().getSubjectId(),
            fact == null ? null : fact.getSubject().getLabel()));
        json.addProperty("predicate", event.getFactKey().getPredicate());
        json.addProperty("scope", event.getFactKey().getScope());
        json.addProperty("factId", event.getFactId());
        json.add("value", fact == null ? JsonNull.INSTANCE : valueToJson(fact));
        intervalToJson(json, "validFrom", "validTo", event.getValidTime());
        json.addProperty("recordedAt", event.getRecordedAt().toString());
        json.add("evidence", evidenceToJson(event.getEvidence()));
        json.addProperty("target", event.getTargetVersionId().orElse(null));
        json.addProperty("payloadHash", event.getPayloadHash());
        return json;
    }

    public JsonObject snapshotToJson(CanonicalSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        JsonObject json = new JsonObject();
        json.addProperty("schemaVersion", NormalizedMemoryEvent.SCHEMA_VERSION);
        JsonArray events = new JsonArray();
        for (NormalizedMemoryEvent event : snapshot.getEvents()) {
            events.add(eventToJson(event));
        }
        json.add("events", events);
        JsonArray versions = new JsonArray();
        for (Map.Entry<FactKey, List<MemoryFactVersion>> entry
            : snapshot.getState().getVersionsByFactKey().entrySet()) {
            for (MemoryFactVersion version : entry.getValue()) {
                versions.add(versionToJson(entry.getKey(), version,
                    snapshot.getGeneratingEventIds().get(version.getId())));
            }
        }
        json.add("versions", versions);
        JsonArray relations = new JsonArray();
        for (VersionRelation relation : snapshot.getState().getRelations()) {
            relations.add(relationToJson(relation));
        }
        json.add("relations", relations);
        return json;
    }

    public JsonObject versionToJson(FactKey key, MemoryFactVersion version, String eventId) {
        JsonObject json = new JsonObject();
        json.addProperty("id", version.getId());
        json.addProperty("eventId", eventId);
        json.add("factKey", factKeyToJson(key));
        MemoryFact fact = version.getFact();
        json.add("subject", subjectToJson(fact.getSubject().getId(), fact.getSubject().getLabel()));
        json.addProperty("predicate", fact.getPredicate());
        json.addProperty("factId", fact.getId());
        json.add("value", valueToJson(fact));
        json.addProperty("status", version.getStatus().name());
        intervalToJson(json, "validFrom", "validTo", version.getValidTime());
        intervalToJson(json, "systemFrom", "systemTo", version.getTransactionTime());
        json.add("evidence", evidenceToJson(version.getEvidence()));
        return json;
    }

    public JsonObject factKeyToJson(FactKey key) {
        JsonObject json = new JsonObject();
        json.addProperty("subjectId", key.getSubjectId());
        json.addProperty("predicate", key.getPredicate());
        json.addProperty("scope", key.getScope());
        return json;
    }

    public JsonObject evidenceToJson(Evidence evidence) {
        JsonObject json = new JsonObject();
        json.addProperty("id", evidence.getId());
        JsonObject source = new JsonObject();
        source.addProperty("id", evidence.getSource().getId());
        source.addProperty("name", evidence.getSource().getName());
        json.add("source", source);
        json.addProperty("content", evidence.getContent());
        return json;
    }

    public JsonObject relationToJson(VersionRelation relation) {
        JsonObject json = new JsonObject();
        json.addProperty("type", relation.getType().name());
        json.addProperty("from", relation.getFromVersionId());
        json.addProperty("to", relation.getToVersionId());
        return json;
    }

    public String toJson(JsonElement json) {
        return GSON.toJson(Objects.requireNonNull(json, "json"));
    }

    /** Rejects duplicate keys, trailing content, and Gson's optional lenient syntax. */
    public JsonObject parseObject(String json) {
        if (json == null) {
            throw invalid("JSON must not be null");
        }
        validateLexicalJson(json);
        try (JsonReader reader = new JsonReader(new StringReader(json))) {
            reader.setLenient(false);
            JsonObject object = object(readJson(reader, 0), "JSON");
            if (reader.peek() != JsonToken.END_DOCUMENT) {
                throw invalid("Unexpected trailing JSON content");
            }
            return object;
        } catch (TemporalValidationException exception) {
            throw exception;
        } catch (IOException | IllegalArgumentException | IllegalStateException exception) {
            throw invalid("Malformed JSON: " + exception.getMessage());
        }
    }

    // Gson 2.2.4 accepts invalid escapes, raw controls, and mixed-case literals even in strict mode.
    private static void validateLexicalJson(String json) {
        boolean quoted = false;
        for (int index = 0; index < json.length(); index++) {
            char value = json.charAt(index);
            if (!quoted) {
                if (value == '"') {
                    quoted = true;
                } else if (value == 't' || value == 'f' || value == 'n') {
                    String literal = value == 't' ? "true" : value == 'f' ? "false" : "null";
                    if (!json.startsWith(literal, index)) {
                        throw invalid("JSON literals must be lowercase true, false, or null");
                    }
                    index += literal.length() - 1;
                } else if ((value >= 'A' && value <= 'Z' && value != 'E')
                    || (value >= 'a' && value <= 'z' && value != 'e')
                    || (value < 0x20 && value != '\t' && value != '\n' && value != '\r')) {
                    throw invalid("Invalid character outside a JSON string");
                }
            } else if (value < 0x20) {
                throw invalid("JSON strings must escape control characters");
            } else if (value == '"') {
                quoted = false;
            } else if (value == '\\') {
                if (++index >= json.length()) {
                    throw invalid("Unterminated JSON escape");
                }
                char escape = json.charAt(index);
                if (escape == 'u') {
                    if (index + 4 >= json.length()) {
                        throw invalid("Unicode escapes require four hexadecimal digits");
                    }
                    for (int digit = 0; digit < 4; digit++) {
                        char hex = json.charAt(++index);
                        if (!((hex >= '0' && hex <= '9') || (hex >= 'a' && hex <= 'f')
                            || (hex >= 'A' && hex <= 'F'))) {
                            throw invalid("Unicode escapes require four hexadecimal digits");
                        }
                    }
                } else if ("\"\\/bfnrt".indexOf(escape) < 0) {
                    throw invalid("Invalid JSON escape");
                }
            }
        }
        if (quoted) {
            throw invalid("Unterminated JSON string");
        }
    }

    private static JsonElement readJson(JsonReader reader, int depth) throws IOException {
        if (depth > 64) {
            throw invalid("JSON nesting exceeds 64 levels");
        }
        switch (reader.peek()) {
            case BEGIN_OBJECT:
                JsonObject object = new JsonObject();
                reader.beginObject();
                while (reader.hasNext()) {
                    String name = reader.nextName();
                    if (object.has(name)) {
                        throw invalid("Duplicate JSON field: " + name);
                    }
                    object.add(name, readJson(reader, depth + 1));
                }
                reader.endObject();
                return object;
            case BEGIN_ARRAY:
                JsonArray array = new JsonArray();
                reader.beginArray();
                while (reader.hasNext()) {
                    array.add(readJson(reader, depth + 1));
                }
                reader.endArray();
                return array;
            case STRING:
                return new JsonPrimitive(reader.nextString());
            case NUMBER:
                return new JsonPrimitive(new BigDecimal(reader.nextString()));
            case BOOLEAN:
                return new JsonPrimitive(reader.nextBoolean());
            case NULL:
                reader.nextNull();
                return JsonNull.INSTANCE;
            default:
                throw invalid("Expected a JSON value");
        }
    }

    private static MemoryFact decodeFact(JsonElement value, String factId,
                                         MemoryEntity subject, String predicate) {
        JsonObject json = object(value, "value");
        String kind = text(json, "kind");
        if ("LITERAL".equals(kind)) {
            fields(json, "value", "kind", "value");
            return MemoryFact.attribute(factId, subject, predicate, text(json, "value"));
        }
        if ("ENTITY_REF".equals(kind)) {
            fields(json, "value", "kind", "value", "label");
            return MemoryFact.relationship(factId, subject, predicate,
                new MemoryEntity(text(json, "value"), text(json, "label")));
        }
        throw invalid("value.kind must be LITERAL or ENTITY_REF");
    }

    private static List<Evidence> decodeEvidence(JsonElement element) {
        if (!element.isJsonArray()) {
            throw invalid("evidence must be an array");
        }
        List<Evidence> evidence = new ArrayList<>();
        for (JsonElement item : element.getAsJsonArray()) {
            JsonObject json = object(item, "evidence item");
            fields(json, "evidence item", "id", "source", "content");
            JsonObject source = object(required(json, "source"), "source");
            fields(source, "source", "id", "name");
            evidence.add(new Evidence(text(json, "id"),
                new Source(text(source, "id"), text(source, "name")), text(json, "content")));
        }
        if (evidence.isEmpty()) {
            throw invalid("evidence must not be empty");
        }
        return evidence;
    }

    private JsonArray evidenceToJson(List<Evidence> evidence) {
        JsonArray json = new JsonArray();
        for (Evidence item : evidence) {
            json.add(evidenceToJson(item));
        }
        return json;
    }

    private static JsonObject subjectToJson(String id, String label) {
        JsonObject json = new JsonObject();
        json.addProperty("id", id);
        json.addProperty("label", label);
        return json;
    }

    private static JsonObject valueToJson(MemoryFact fact) {
        JsonObject json = new JsonObject();
        json.addProperty("kind", fact.isRelationship() ? "ENTITY_REF" : "LITERAL");
        json.addProperty("value", fact.isRelationship()
            ? fact.getTarget().get().getId() : fact.getLiteralValue().get());
        if (fact.isRelationship()) {
            json.addProperty("label", fact.getTarget().get().getLabel());
        }
        return json;
    }

    private static void intervalToJson(JsonObject json, String startName, String endName,
                                       TimeInterval interval) {
        json.addProperty(startName, interval.getStart().toString());
        json.addProperty(endName, interval.getEnd().map(Instant::toString).orElse(null));
    }

    private static void schemaVersion(JsonObject json) {
        JsonElement version = required(json, "schemaVersion");
        if (!version.isJsonPrimitive() || !version.getAsJsonPrimitive().isNumber()) {
            throw invalid("schemaVersion must be an integer");
        }
        try {
            if (version.getAsBigDecimal().intValueExact() == NormalizedMemoryEvent.SCHEMA_VERSION) {
                return;
            }
        } catch (ArithmeticException exception) {
            throw invalid("schemaVersion must be an integer");
        }
        throw new TemporalValidationException(TemporalErrorCode.REJECT_SCHEMA_VERSION,
            "Only schemaVersion 2 is supported; replay and rebind legacy targets before migration");
    }

    private static JsonElement required(JsonObject json, String name) {
        if (!json.has(name)) {
            throw invalid("Missing required field: " + name);
        }
        return json.get(name);
    }

    private static String text(JsonObject json, String name) {
        String value = nullableText(required(json, name), name);
        if (value == null) {
            throw invalid(name + " must not be null");
        }
        return value;
    }

    private static String nullableText(JsonElement element, String name) {
        if (element.isJsonNull()) {
            return null;
        }
        if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isString()
            || element.getAsString().trim().isEmpty()) {
            throw invalid(name + " must be a nonblank string");
        }
        return element.getAsString();
    }

    private static JsonObject object(JsonElement element, String name) {
        if (element == null || !element.isJsonObject()) {
            throw invalid(name + " must be an object");
        }
        return element.getAsJsonObject();
    }

    private static Instant nullableTime(JsonElement element, String name) {
        return element.isJsonNull() ? null : time(element, name);
    }

    private static Instant time(JsonElement element, String name) {
        String value = nullableText(element, name);
        if (value == null || !value.endsWith("Z")) {
            throw invalid(name + " must be an ISO-8601 UTC timestamp ending in Z");
        }
        try {
            return OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeParseException exception) {
            throw invalid(name + " is not a valid ISO-8601 UTC timestamp");
        }
    }

    private static void fields(JsonObject json, String context, String... allowed) {
        Set<String> names = new HashSet<>(Arrays.asList(allowed));
        for (Map.Entry<String, JsonElement> entry : json.entrySet()) {
            if (!names.contains(entry.getKey())) {
                throw invalid("Unknown " + context + " field: " + entry.getKey());
            }
        }
    }

    private static TemporalValidationException invalid(String message) {
        return new TemporalValidationException(TemporalErrorCode.REJECT_INPUT, message);
    }
}
