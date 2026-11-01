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
 */
package org.apache.geaflow.ai.retrieval.codec;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import org.apache.geaflow.ai.retrieval.answer.AnswerRequest;
import org.apache.geaflow.ai.retrieval.answer.AnswerResponse;
import org.apache.geaflow.ai.retrieval.answer.Citation;

/**
 * JSON wire contract for the answer/context endpoint.
 *
 * <p>Requests mirror the retrieval request identity so an answer can be traced back to the search
 * that produced it: verification needs the retrieval query, its budget and its version pins.
 */
public final class AnswerApiJson {

    private AnswerApiJson() {
    }

    /**
     * Parses an answer request.
     *
     * @return a request carrying the retrieval identity echoed on the response
     */
    public static AnswerRequest parseRequest(String json) {
        JsonObject object = parseObject(json);
        String question = requiredString(object, "question");
        int tokenBudget = requiredInt(object, "tokenBudget");
        return new AnswerRequest(question, tokenBudget, optionalString(object, "requestId"),
            optionalString(object, "graphVersion"), optionalString(object, "indexVersion"),
            optionalString(object, "graphName"), optionalString(object, "mode"));
    }

    /** Serializes an answer response, including abstention and per-item suppression reasons. */
    public static String toJson(AnswerResponse response) {
        JsonObject object = new JsonObject();
        addNullable(object, "answer", response.getAnswer());
        addNullable(object, "abstentionReason", response.getAbstentionReason());
        object.addProperty("isAbstained", response.isAbstained());
        object.addProperty("context", response.getContext());
        object.addProperty("tokens", response.getTokens());
        object.addProperty("consideredEvidenceCount", response.getConsideredEvidenceCount());
        object.addProperty("citationCoverage", response.getCitationCoverage());
        addNullable(object, "requestId", response.getRequestId());
        addNullable(object, "graphVersion", response.getGraphVersion());
        addNullable(object, "indexVersion", response.getIndexVersion());

        JsonArray citations = new JsonArray();
        for (Citation citation : response.getCitations()) {
            JsonObject item = new JsonObject();
            item.addProperty("evidenceId", citation.getEvidenceId());
            addNullable(item, "sourceId", citation.getSourceId());
            addNullable(item, "chunkId", citation.getChunkId());
            addNullable(item, "path", citation.getPathId());
            citations.add(item);
        }
        object.add("citations", citations);
        object.add("evidenceIds", strings(response.getEvidenceIds()));
        object.add("traceIds", strings(response.getTraceIds()));

        JsonObject reasons = new JsonObject();
        for (var entry : response.getSuppressReasons().entrySet()) {
            reasons.addProperty(entry.getKey(), entry.getValue());
        }
        object.add("suppressReasons", reasons);
        return object.toString();
    }

    private static JsonArray strings(java.util.List<String> values) {
        JsonArray array = new JsonArray();
        if (values != null) {
            for (String value : values) {
                if (value == null) {
                    array.add(com.google.gson.JsonNull.INSTANCE);
                } else {
                    array.add(new com.google.gson.JsonPrimitive(value));
                }
            }
        }
        return array;
    }

    private static void addNullable(JsonObject target, String name, String value) {
        if (value == null) {
            target.add(name, com.google.gson.JsonNull.INSTANCE);
        } else {
            target.addProperty(name, value);
        }
    }

    private static JsonObject parseObject(String json) {
        try {
            JsonElement parsed = new JsonParser().parse(json);
            if (!parsed.isJsonObject()) {
                throw new JsonParseException("answer request must be a JSON object");
            }
            return parsed.getAsJsonObject();
        } catch (JsonParseException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new JsonParseException("malformed answer request", e);
        }
    }

    private static String requiredString(JsonObject object, String name) {
        String value = optionalString(object, name);
        if (value == null || value.trim().isEmpty()) {
            throw new JsonParseException(name + " is required");
        }
        return value;
    }

    private static String optionalString(JsonObject object, String name) {
        if (!object.has(name) || object.get(name).isJsonNull()) {
            return null;
        }
        if (!object.get(name).isJsonPrimitive() || !object.get(name).getAsJsonPrimitive().isString()) {
            throw new JsonParseException(name + " must be a string");
        }
        return object.get(name).getAsString();
    }

    private static int requiredInt(JsonObject object, String name) {
        if (!object.has(name) || object.get(name).isJsonNull()) {
            throw new JsonParseException(name + " is required");
        }
        try {
            return object.get(name).getAsInt();
        } catch (RuntimeException e) {
            throw new JsonParseException(name + " must be an integer", e);
        }
    }
}