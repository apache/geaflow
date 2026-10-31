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

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.model.VersionRelation;
import org.apache.geaflow.ai.temporal.model.VersionRelationType;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalErrorCode;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Validates production input without fixture aliases or reflective model construction. */
public class TemporalJsonCodecTest {

    private static final String EVENT = "{\"schemaVersion\":2,\"id\":\"event-1\","
        + "\"operation\":\"ASSERT\",\"subject\":{\"id\":\"alice\",\"label\":\"person\"},"
        + "\"predicate\":\"city\",\"scope\":\"profile\",\"factId\":\"fact-1\","
        + "\"value\":{\"kind\":\"LITERAL\",\"value\":\"Paris\"},"
        + "\"validFrom\":\"2024-01-01T00:00:00.123456789Z\",\"validTo\":null,"
        + "\"recordedAt\":\"2024-03-01T00:00:00.987654321Z\",\"evidence\":["
        + "{\"id\":\"proof-b\",\"source\":{\"id\":\"source-b\",\"name\":\"Profile B\"},"
        + "\"content\":\"Paris is home\"},"
        + "{\"id\":\"proof-a\",\"source\":{\"id\":\"source-a\",\"name\":\"Profile A\"},"
        + "\"content\":\"Lives in Paris <verified>\"}]}";

    private final TemporalJsonCodec codec = new TemporalJsonCodec();

    @Test
    public void testNormalizedRoundTripIsStableAndKeepsNanoseconds() {
        NormalizedMemoryEvent event = codec.decodeEvent(EVENT);
        String json = codec.toJson(codec.eventToJson(event));
        Assertions.assertEquals(event, codec.decodeEvent(json));
        Assertions.assertEquals(json, codec.toJson(codec.eventToJson(codec.decodeEvent(json))));
        Assertions.assertEquals(123456789, event.getValidTime().getStart().getNano());
        Assertions.assertEquals(987654321, event.getRecordedAt().getNano());
        Assertions.assertEquals("proof-a", event.getEvidence().get(0).getId());
        Assertions.assertTrue(json.startsWith("{\"schemaVersion\":2,\"id\":\"event-1\","
            + "\"operation\":\"ASSERT\",\"subject\":"));
        Assertions.assertTrue(json.contains("\"validTo\":null"));
        Assertions.assertTrue(json.contains("\"target\":null"));
        Assertions.assertTrue(json.contains("Lives in Paris <verified>"));
        Assertions.assertEquals(64, event.getPayloadHash().length());
    }

    @Test
    public void testEntityCorrectionPreservesLabelsAndExplicitTarget() {
        JsonObject json = codec.parseObject(EVENT);
        json.addProperty("operation", "CORRECT");
        json.addProperty("target", "opaque-target-version");
        JsonObject value = json.getAsJsonObject("value");
        value.addProperty("kind", "ENTITY_REF");
        value.addProperty("value", "city:paris");
        value.addProperty("label", "city");
        NormalizedMemoryEvent event = codec.decodeEvent(json);
        Assertions.assertEquals("opaque-target-version", event.getTargetVersionId().get());
        Assertions.assertEquals("city", event.getEvent().getFact().get().getTarget().get().getLabel());
        Assertions.assertEquals(event, codec.decodeEvent(codec.toJson(codec.eventToJson(event))));
    }

    @Test
    public void testRetractionRoundTripDoesNotInventFactOrSubjectLabel() {
        JsonObject json = codec.parseObject(EVENT);
        json.addProperty("operation", "RETRACT");
        json.addProperty("target", "opaque-target-version");
        json.add("value", JsonNull.INSTANCE);
        NormalizedMemoryEvent event = codec.decodeEvent(json);
        JsonObject encoded = codec.eventToJson(event);
        Assertions.assertTrue(encoded.get("value").isJsonNull());
        Assertions.assertTrue(encoded.getAsJsonObject("subject").get("label").isJsonNull());
        Assertions.assertFalse(event.getEvent().getFact().isPresent());
        Assertions.assertEquals(event, codec.decodeEvent(codec.toJson(encoded)));
    }

    @Test
    public void testPayloadHashIsVerifiedAfterNormalization() {
        NormalizedMemoryEvent event = codec.decodeEvent(EVENT);
        JsonObject json = codec.eventToJson(event);
        Assertions.assertEquals(event, codec.decodeEvent(json));
        json.addProperty("payloadHash", "wrong");
        reject(json, TemporalErrorCode.REJECT_INPUT);
        json.add("payloadHash", JsonNull.INSTANCE);
        reject(json, TemporalErrorCode.REJECT_INPUT);
        json.addProperty("payloadHash", event.getPayloadHash());
        json.getAsJsonObject("subject").addProperty("label", "organization");
        reject(json, TemporalErrorCode.REJECT_INPUT);
    }

    @Test
    public void testMissingBlankWrongTypeAndEmptyFieldsAreRejected() {
        JsonObject missing = codec.parseObject(EVENT);
        missing.remove("validTo");
        reject(missing, TemporalErrorCode.REJECT_INPUT);
        JsonObject blank = codec.parseObject(EVENT);
        blank.addProperty("id", "  ");
        reject(blank, TemporalErrorCode.REJECT_INPUT);
        JsonObject numeric = codec.parseObject(EVENT);
        numeric.addProperty("factId", 3);
        reject(numeric, TemporalErrorCode.REJECT_INPUT);
        JsonObject empty = codec.parseObject(EVENT);
        empty.add("evidence", new JsonArray());
        reject(empty, TemporalErrorCode.REJECT_INPUT);
        JsonObject wrongSource = codec.parseObject(EVENT);
        wrongSource.getAsJsonArray("evidence").get(0).getAsJsonObject()
            .add("source", new JsonPrimitive(true));
        reject(wrongSource, TemporalErrorCode.REJECT_INPUT);
        JsonObject nullValue = codec.parseObject(EVENT);
        nullValue.add("value", JsonNull.INSTANCE);
        reject(nullValue, TemporalErrorCode.REJECT_INPUT);
    }

    @Test
    public void testUnknownFieldsAreRejectedAtEveryLevel() {
        JsonObject top = codec.parseObject(EVENT);
        top.addProperty("alias", "fixture-only");
        reject(top, TemporalErrorCode.REJECT_INPUT);
        JsonObject subject = codec.parseObject(EVENT);
        subject.getAsJsonObject("subject").addProperty("future", "field");
        reject(subject, TemporalErrorCode.REJECT_INPUT);
        JsonObject value = codec.parseObject(EVENT);
        value.getAsJsonObject("value").addProperty("label", "invalid-literal-label");
        reject(value, TemporalErrorCode.REJECT_INPUT);
        JsonObject evidence = codec.parseObject(EVENT);
        evidence.getAsJsonArray("evidence").get(0).getAsJsonObject()
            .addProperty("score", 10);
        reject(evidence, TemporalErrorCode.REJECT_INPUT);
        JsonObject source = codec.parseObject(EVENT);
        source.getAsJsonArray("evidence").get(0).getAsJsonObject()
            .getAsJsonObject("source").addProperty("rank", 1);
        reject(source, TemporalErrorCode.REJECT_INPUT);
    }

    @Test
    public void testOnlySchemaTwoIsAccepted() {
        JsonObject old = codec.parseObject(EVENT);
        old.addProperty("schemaVersion", 1);
        reject(old, TemporalErrorCode.REJECT_SCHEMA_VERSION);
        old.addProperty("schemaVersion", 3);
        reject(old, TemporalErrorCode.REJECT_SCHEMA_VERSION);
        old.addProperty("schemaVersion", "2");
        reject(old, TemporalErrorCode.REJECT_INPUT);
        old.addProperty("schemaVersion", 2.5);
        reject(old, TemporalErrorCode.REJECT_INPUT);
    }

    @Test
    public void testInvalidTimesAndIntervalsAreRejectedWithoutTruncation() {
        for (String time : Arrays.asList("yesterday", "2024-02-30T00:00:00Z",
            "2024-01-01T00:00:00+00:00", "2024-01-01T24:00:00Z",
            "2024-01-01T00:00:00.1234567891Z", "2024-12-31T23:59:60Z")) {
            JsonObject json = codec.parseObject(EVENT);
            json.addProperty("recordedAt", time);
            reject(json, TemporalErrorCode.REJECT_INPUT);
        }
        JsonObject empty = codec.parseObject(EVENT);
        empty.add("validTo", empty.get("validFrom"));
        reject(empty, TemporalErrorCode.REJECT_INPUT);
        JsonObject reversed = codec.parseObject(EVENT);
        reversed.addProperty("validTo", "2023-01-01T00:00:00Z");
        reject(reversed, TemporalErrorCode.REJECT_INPUT);
    }

    @Test
    public void testTargetsAndRetractionValuesAreNeverGuessed() {
        JsonObject correction = codec.parseObject(EVENT);
        correction.addProperty("operation", "CORRECT");
        reject(correction, TemporalErrorCode.REJECT_TARGET_REQUIRED);
        correction.add("target", JsonNull.INSTANCE);
        reject(correction, TemporalErrorCode.REJECT_TARGET_REQUIRED);
        correction.addProperty("target", 1);
        reject(correction, TemporalErrorCode.REJECT_INPUT);
        JsonObject assertion = codec.parseObject(EVENT);
        assertion.addProperty("target", "a-version");
        reject(assertion, TemporalErrorCode.REJECT_INPUT);
        JsonObject retract = codec.parseObject(EVENT);
        retract.addProperty("operation", "RETRACT");
        retract.addProperty("target", "a-version");
        reject(retract, TemporalErrorCode.REJECT_INPUT);
    }

    @Test
    public void testStrictJsonRejectsMalformedDuplicatesAndTrailingContent() {
        for (String json : Arrays.asList("", "null", "[]", "{", "{'id':'x'}",
            "{id:\"x\"}", "{\"id\":\"a\",\"id\":\"b\"}",
            "{\"nested\":{\"id\":1,\"id\":2}}", "{} trailing", "{}{}", "{/*x*/}")) {
            TemporalValidationException exception = Assertions.assertThrows(
                TemporalValidationException.class, () -> codec.parseObject(json), json);
            Assertions.assertEquals(TemporalErrorCode.REJECT_INPUT, exception.getCode(), json);
        }
    }

    @Test
    public void testStrictJsonRejectsRawControlsAndInvalidEscapes() {
        for (int control = 0; control < 0x20; control++) {
            String json = "{\"content\":\"before" + (char) control + "after\"}";
            Assertions.assertThrows(TemporalValidationException.class,
                () -> codec.parseObject(json), "Raw control " + control);
        }
        for (String json : Arrays.asList("{\"content\":\"bad\\qescape\"}",
            "{\"content\":\"bad\\'escape\"}", "{\"content\":\"bad\\\nescape\"}",
            "{\"content\":\"bad\\u12\"}", "{\"content\":\"bad\\u12XZ\"}",
            "{\"content\":\"bad\\u1")) {
            TemporalValidationException exception = Assertions.assertThrows(
                TemporalValidationException.class, () -> codec.parseObject(json), json);
            Assertions.assertEquals(TemporalErrorCode.REJECT_INPUT, exception.getCode());
        }
    }

    @Test
    public void testStrictJsonRejectsNonstandardLiteralCase() {
        for (String literal : Arrays.asList("TRUE", "TrUe", "FALSE", "fAlSe", "NULL", "NuLl")) {
            Assertions.assertThrows(TemporalValidationException.class,
                () -> codec.parseObject("{\"value\":" + literal + "}"), literal);
        }
        Assertions.assertThrows(TemporalValidationException.class,
            () -> codec.decodeEvent(EVENT.replace("\"validTo\":null", "\"validTo\":NULL")));
    }

    @Test
    public void testStrictJsonKeepsValidEscapedControlsUnicodeAndNumericExponent() {
        JsonObject json = codec.parseObject("{\"content\":\"quote\\\" slash\\/ backslash\\\\"
            + " controls\\b\\f\\n\\r\\t unicode\\u4E2D\\u6587\\uD83D\\uDE00\","
            + "\"number\":2E0,\"yes\":true,\"no\":false,\"empty\":null}");
        Assertions.assertEquals("quote\" slash/ backslash\\ controls\b\f\n\r\t unicode中文😀",
            json.get("content").getAsString());
        Assertions.assertEquals(2, json.get("number").getAsInt());
        Assertions.assertTrue(json.get("yes").getAsBoolean());
        Assertions.assertFalse(json.get("no").getAsBoolean());
        Assertions.assertTrue(json.get("empty").isJsonNull());
        Assertions.assertEquals(json, codec.parseObject(codec.toJson(json)));
        String eventJson = EVENT.replace("Paris is home", "Paris\\n\\u4E2D");
        NormalizedMemoryEvent event = codec.decodeEvent(eventJson);
        Assertions.assertEquals(event, codec.decodeEvent(codec.toJson(codec.eventToJson(event))));
    }

    @Test
    public void testSnapshotIncludesCompleteCanonicalVersionAndRelations() {
        NormalizedMemoryEvent first = codec.decodeEvent(EVENT);
        JsonObject secondJson = codec.parseObject(EVENT);
        secondJson.addProperty("id", "event-2");
        secondJson.addProperty("operation", "CORRECT");
        secondJson.addProperty("target", "version-first");
        secondJson.addProperty("recordedAt", "2024-04-01T00:00:00.000000001Z");
        secondJson.getAsJsonObject("value").addProperty("value", "Berlin");
        NormalizedMemoryEvent second = codec.decodeEvent(secondJson);
        MemoryFactVersion oldVersion = new MemoryFactVersion("version-first",
            first.getEvent().getFact().get(), first.getValidTime(),
            new TimeInterval(first.getRecordedAt(), second.getRecordedAt()), first.getEvidence());
        MemoryFactVersion newVersion = new MemoryFactVersion("version-second",
            second.getEvent().getFact().get(), second.getValidTime(),
            TimeInterval.unboundedFrom(second.getRecordedAt()), second.getEvidence());
        VersionRelation relation = new VersionRelation(VersionRelationType.SUPERSEDES,
            newVersion.getId(), oldVersion.getId());
        Map<String, String> generating = new HashMap<>();
        generating.put(newVersion.getId(), second.getEventId());
        generating.put(oldVersion.getId(), first.getEventId());
        CanonicalSnapshot snapshot = new CanonicalSnapshot(new TemporalState(
            Collections.singletonMap(first.getFactKey(), Arrays.asList(newVersion, oldVersion)),
            Collections.singletonList(relation)), Arrays.asList(second, first), generating);
        JsonObject json = codec.snapshotToJson(snapshot);
        Assertions.assertEquals(2, json.get("schemaVersion").getAsInt());
        Assertions.assertEquals(first, codec.decodeEvent(json.getAsJsonArray("events")
            .get(0).getAsJsonObject()));
        JsonObject old = json.getAsJsonArray("versions").get(0).getAsJsonObject();
        Assertions.assertEquals("version-first", old.get("id").getAsString());
        Assertions.assertEquals("event-1", old.get("eventId").getAsString());
        Assertions.assertEquals("profile", old.getAsJsonObject("factKey").get("scope").getAsString());
        Assertions.assertEquals("2024-04-01T00:00:00.000000001Z", old.get("systemTo").getAsString());
        Assertions.assertEquals("2024-01-01T00:00:00.123456789Z", old.get("validFrom").getAsString());
        Assertions.assertEquals("Profile A", old.getAsJsonArray("evidence").get(0)
            .getAsJsonObject().getAsJsonObject("source").get("name").getAsString());
        Assertions.assertEquals("Lives in Paris <verified>", old.getAsJsonArray("evidence").get(0)
            .getAsJsonObject().get("content").getAsString());
        Assertions.assertEquals("version-first", json.getAsJsonArray("relations").get(0)
            .getAsJsonObject().get("to").getAsString());
        Assertions.assertTrue(json.getAsJsonArray("versions").get(1).getAsJsonObject()
            .get("systemTo").isJsonNull());
        Assertions.assertEquals(codec.toJson(json), codec.toJson(codec.snapshotToJson(snapshot)));
    }

    @Test
    public void testJsonObjectNullInputHasStableError() {
        TemporalValidationException exception = Assertions.assertThrows(
            TemporalValidationException.class, () -> codec.decodeEvent((JsonObject) null));
        Assertions.assertEquals(TemporalErrorCode.REJECT_INPUT, exception.getCode());
        Assertions.assertThrows(TemporalValidationException.class,
            () -> codec.decodeEvent((String) null));
    }

    private void reject(JsonObject json, TemporalErrorCode code) {
        TemporalValidationException exception = Assertions.assertThrows(
            TemporalValidationException.class, () -> codec.decodeEvent(json));
        Assertions.assertEquals(code, exception.getCode());
    }
}
