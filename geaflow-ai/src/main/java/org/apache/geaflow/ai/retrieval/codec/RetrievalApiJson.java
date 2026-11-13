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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.ai.retrieval.codec;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.ai.retrieval.api.model.ExecutionMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalError;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalTrace;
import org.apache.geaflow.ai.retrieval.api.model.TraceStage;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.apache.geaflow.ai.retrieval.model.document.SourceRef;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef;
import org.apache.geaflow.ai.retrieval.service.RetrievalErrorValidator;
import org.apache.geaflow.ai.retrieval.service.RetrievalResponseValidator;

/** Strict, dependency-light JSON boundary for the v1 retrieval API. */
public final class RetrievalApiJson {

    private static final int MAX_JSON_LENGTH = 1024 * 1024;
    private static final Gson GSON = new Gson();

    private RetrievalApiJson() {
    }

    public static RetrievalRequest parseRequest(String json) {
        JsonObject object = object(json);
        validateRequestShape(object);
        return GSON.fromJson(object, RetrievalRequest.class);
    }

    public static RetrievalResponse parseResponse(String json) {
        return parseResponse(json, defaultProperties());
    }

    public static RetrievalResponse parseResponse(String json, RetrievalProperties properties) {
        JsonObject object = object(json);
        requireString(object, "requestId");
        requireString(object, "graphName");
        requireString(object, "graphVersion");
        requireArray(object, "evidence");
        requireArray(object, "paths");
        requireArray(object, "sources");
        requireArray(object, "degradedChannels");
        requireObject(object, "trace");
        requireObject(object, "effectiveBudget");
        List<Evidence> evidence = models(object, "evidence", Evidence.class);
        List<GraphPathRef> paths = models(object, "paths", GraphPathRef.class);
        List<SourceRef> sources = models(object, "sources", SourceRef.class);
        RetrievalResponse response = new RetrievalResponse();
        response.setRequestId(object.get("requestId").getAsString());
        response.setGraphName(object.get("graphName").getAsString());
        response.setGraphVersion(object.get("graphVersion").getAsString());
        response.setEvidence(evidence);
        response.setPaths(paths);
        response.setSources(sources);
        response.setTrace(parseTrace(object.getAsJsonObject("trace")));
        response.setEffectiveBudget(parseBudget(object.getAsJsonObject("effectiveBudget"), true));
        response.setDegradedChannels(strings(object, "degradedChannels"));
        return RetrievalResponseValidator.validate(response, properties);
    }

    public static RetrievalError parseError(String json) {
        JsonObject object = object(json);
        requireString(object, "requestId");
        requireString(object, "code");
        requireString(object, "message");
        String code = object.get("code").getAsString();
        RetrievalErrorCode errorCode;
        try {
            errorCode = RetrievalErrorCode.valueOf(code);
        } catch (IllegalArgumentException e) {
            throw new JsonParseException("unknown retrieval error code: " + code);
        }
        if (!object.has("retriable") || !object.get("retriable").isJsonPrimitive()
            || !object.get("retriable").getAsJsonPrimitive().isBoolean()) {
            throw new JsonParseException("retriable is required and must be boolean");
        }
        boolean retriable = object.get("retriable").getAsBoolean();
        if (retriable != errorCode.isRetriable()) {
            throw new JsonParseException("retriable does not match error code");
        }
        RetrievalError error = new RetrievalError(object.get("requestId").getAsString(),
            errorCode, object.get("message").getAsString());
        error.setRetriable(retriable);
        return RetrievalErrorValidator.validate(error);
    }

    public static String toJson(Object value) {
        return toJson(value, defaultProperties());
    }

    public static String toJson(Object value, RetrievalProperties properties) {
        if (value instanceof RetrievalResponse) {
            RetrievalResponseValidator.validate((RetrievalResponse) value, properties);
        } else if (value instanceof RetrievalError) {
            RetrievalErrorValidator.validate((RetrievalError) value);
        }
        return GSON.toJson(value);
    }

    private static RetrievalProperties defaultProperties() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.validateConfiguration();
        return properties;
    }

    private static JsonObject object(String json) {
        if (json == null || json.trim().isEmpty()) {
            throw new JsonParseException("JSON object is required");
        }
        if (json.getBytes(StandardCharsets.UTF_8).length > MAX_JSON_LENGTH) {
            throw new JsonParseException("retrieval JSON exceeds maximum size");
        }
        try {
            rejectDuplicateKeys(json);
            JsonElement element = new JsonParser().parse(json);
            if (!element.isJsonObject()) {
                throw new JsonParseException("retrieval JSON must be an object");
            }
            return element.getAsJsonObject();
        } catch (JsonParseException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new JsonParseException("invalid retrieval JSON", e);
        }
    }

    private static void validateRequestShape(JsonObject object) {
        requireString(object, "graphName");
        requireString(object, "query");
        optionalString(object, "requestId");
        optionalString(object, "graphVersion");
        optionalString(object, "indexVersion");
        optionalString(object, "vectorVersion");
        optionalString(object, "vectorSource");
        optionalString(object, "mode");
        optionalString(object, "executionMode");
        if (object.has("queryVector") && !object.get("queryVector").isJsonNull()) {
            JsonElement value = object.get("queryVector");
            if (!value.isJsonArray()) {
                throw new JsonParseException("queryVector must be an array");
            }
            for (JsonElement item : value.getAsJsonArray()) {
                if (!item.isJsonPrimitive() || !item.getAsJsonPrimitive().isNumber()
                    || !Double.isFinite(item.getAsDouble())) {
                    throw new JsonParseException("queryVector must contain finite numbers");
                }
            }
        }
        if (object.has("budget") && !object.get("budget").isJsonNull()) {
            requireObject(object, "budget");
            parseBudget(object.getAsJsonObject("budget"), false);
        }
    }

    private static RetrievalBudget parseBudget(JsonObject object, boolean requiredValues) {
        if (object == null) {
            throw new JsonParseException("budget must be an object");
        }
        Integer topK = optionalInt(object, "topK");
        Integer timeoutMs = optionalInt(object, "timeoutMs");
        Integer maxCandidates = optionalInt(object, "maxCandidates");
        Integer tokenBudget = optionalInt(object, "tokenBudget");
        if (requiredValues && (topK == null || timeoutMs == null || maxCandidates == null
            || tokenBudget == null)) {
            throw new JsonParseException("effectiveBudget contains invalid values");
        }
        return new RetrievalBudget(topK, timeoutMs, maxCandidates, tokenBudget);
    }

    private static RetrievalTrace parseTrace(JsonObject object) {
        if (object == null) {
            throw new JsonParseException("trace must be an object");
        }
        RetrievalTrace trace = new RetrievalTrace();
        trace.setTraceVersion(string(object, "traceVersion"));
        trace.setRequestId(optionalString(object, "requestId"));
        trace.setOriginalQuery(string(object, "originalQuery"));
        trace.setSelectedMode(enumValue(object, "selectedMode",
            org.apache.geaflow.ai.retrieval.api.model.RetrievalMode.class));
        trace.setExecutionMode(enumValue(object, "executionMode", ExecutionMode.class));
        requireArray(object, "stages");
        trace.setStages(traceStages(object, "stages"));
        trace.setStopReason(optionalString(object, "stopReason"));
        trace.setGraphVersion(optionalString(object, "graphVersion"));
        trace.setIndexVersion(optionalString(object, "indexVersion"));
        trace.setVectorVersion(optionalString(object, "vectorVersion"));
        trace.setEffectiveCandidateBudget(optionalInt(object, "effectiveCandidateBudget") == null
            ? 0 : optionalInt(object, "effectiveCandidateBudget"));
        trace.setElapsedNanos(optionalLong(object, "elapsedNanos"));
        trace.setCandidateCounts(integerMap(object, "candidateCounts"));
        trace.setEvaluatedCounts(integerMap(object, "evaluatedCounts"));
        trace.setEffectiveTopK(optionalInt(object, "effectiveTopK") == null ? 0 : optionalInt(object, "effectiveTopK"));
        trace.setChannelStatuses(enumMap(object, "channelStatuses",
            org.apache.geaflow.ai.retrieval.execution.RecallStageStatus.class));
        trace.setChannelStopReasons(enumMap(object, "channelStopReasons",
            org.apache.geaflow.ai.retrieval.execution.RecallStopReason.class));
        trace.setAnchorsConsidered(optionalInt(object, "anchorsConsidered") == null ? 0 : optionalInt(object, "anchorsConsidered"));
        trace.setEdgesExamined(optionalInt(object, "edgesExamined") == null ? 0 : optionalInt(object, "edgesExamined"));
        trace.setNeighborsSampled(optionalInt(object, "neighborsSampled") == null ? 0 : optionalInt(object, "neighborsSampled"));
        trace.setVerticesReached(optionalInt(object, "verticesReached") == null ? 0 : optionalInt(object, "verticesReached"));
        trace.setGraphCandidatesProduced(optionalInt(object, "graphCandidatesProduced") == null ? 0 : optionalInt(object, "graphCandidatesProduced"));
        trace.setTotalCandidatesEvaluated(optionalInt(object, "totalCandidatesEvaluated") == null
            ? 0 : optionalInt(object, "totalCandidatesEvaluated"));
        trace.setSelectedChannels(strings(object, "selectedChannels"));
        trace.setChannelBudgets(integerMap(object, "channelBudgets"));
        trace.setDegradedChannels(strings(object, "degradedChannels"));
        trace.setDegradationReasons(stringMap(object, "degradationReasons"));
        trace.setBm25Weight(optionalDouble(object, "bm25Weight", 1.0));
        trace.setVectorWeight(optionalDouble(object, "vectorWeight", 1.0));
        trace.setGraphWeight(optionalDouble(object, "graphWeight", 1.0));
        trace.setRrfRankConstant(optionalInt(object, "rrfRankConstant") == null
            ? 60 : optionalInt(object, "rrfRankConstant"));
        trace.setValidationErrors(strings(object, "validationErrors"));
        return trace;
    }

    private static <T extends Enum<T>> java.util.Map<String, T> enumMap(JsonObject object, String name, Class<T> type) {
        java.util.Map<String, T> result = new java.util.LinkedHashMap<>();
        for (java.util.Map.Entry<String, String> entry : stringMap(object, name).entrySet()) {
            try {
                result.put(entry.getKey(), Enum.valueOf(type, entry.getValue()));
            } catch (IllegalArgumentException error) {
                throw new JsonParseException("unknown " + name + ": " + entry.getValue(), error);
            }
        }
        return result;
    }

    private static long optionalLong(JsonObject object, String name) {
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            return 0L;
        }
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()
            || !Double.isFinite(value.getAsDouble()) || value.getAsDouble() != Math.rint(value.getAsDouble())) {
            throw new JsonParseException(name + " must be an integer");
        }
        return value.getAsLong();
    }

    private static double optionalDouble(JsonObject object, String name, double fallback) {
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            return fallback;
        }
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()
            || !Double.isFinite(value.getAsDouble())) {
            throw new JsonParseException(name + " must be a finite number");
        }
        return value.getAsDouble();
    }

    private static java.util.Map<String, Integer> integerMap(JsonObject object, String name) {
        java.util.Map<String, Integer> result = new java.util.LinkedHashMap<>();
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            return result;
        }
        if (!value.isJsonObject()) {
            throw new JsonParseException(name + " must be an object");
        }
        for (java.util.Map.Entry<String, JsonElement> entry : value.getAsJsonObject().entrySet()) {
            JsonElement item = entry.getValue();
            if (!item.isJsonPrimitive() || !item.getAsJsonPrimitive().isNumber()
                || !Double.isFinite(item.getAsDouble()) || item.getAsDouble() != Math.rint(item.getAsDouble())
                || item.getAsDouble() < Integer.MIN_VALUE || item.getAsDouble() > Integer.MAX_VALUE) {
                throw new JsonParseException(name + " values must be integers");
            }
            result.put(entry.getKey(), item.getAsInt());
        }
        return result;
    }

    private static java.util.Map<String, String> stringMap(JsonObject object, String name) {
        java.util.Map<String, String> result = new java.util.LinkedHashMap<>();
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            return result;
        }
        if (!value.isJsonObject()) {
            throw new JsonParseException(name + " must be an object");
        }
        for (java.util.Map.Entry<String, JsonElement> entry : value.getAsJsonObject().entrySet()) {
            JsonElement item = entry.getValue();
            if (!item.isJsonPrimitive() || !item.getAsJsonPrimitive().isString()) {
                throw new JsonParseException(name + " values must be strings");
            }
            result.put(entry.getKey(), item.getAsString());
        }
        return result;
    }

    private static List<TraceStage> traceStages(JsonObject object, String name) {
        JsonElement element = object.get(name);
        if (element == null || element.isJsonNull()) {
            return new ArrayList<>();
        }
        if (!element.isJsonArray()) {
            throw new JsonParseException(name + " must be an array");
        }
        List<TraceStage> result = new ArrayList<>();
        for (JsonElement item : element.getAsJsonArray()) {
            if (!item.isJsonObject()) {
                throw new JsonParseException(name + " elements must be objects");
            }
            JsonObject stage = item.getAsJsonObject();
            result.add(new TraceStage(string(stage, "name"), string(stage, "status"),
                optionalString(stage, "reason")));
        }
        return result;
    }

    private static <T> List<T> models(JsonObject object, String name, Class<T> type) {
        JsonElement element = object.get(name);
        if (element == null || element.isJsonNull()) {
            return new ArrayList<>();
        }
        if (!element.isJsonArray()) {
            throw new JsonParseException(name + " must be an array");
        }
        List<T> result = new ArrayList<>();
        for (JsonElement item : element.getAsJsonArray()) {
            if (!item.isJsonObject()) {
                throw new JsonParseException(name + " elements must be objects");
            }
            String itemJson = item.toString();
            if (type == Evidence.class) {
                result.add(type.cast(RetrievalModelJson.fromJson(itemJson, Evidence.class)));
            } else if (type == GraphPathRef.class) {
                result.add(type.cast(RetrievalModelJson.fromJson(itemJson, GraphPathRef.class)));
            } else if (type == SourceRef.class) {
                result.add(type.cast(RetrievalModelJson.fromJson(itemJson, SourceRef.class)));
            } else {
                result.add(GSON.fromJson(item, type));
            }
        }
        return result;
    }

    private static List<String> strings(JsonObject object, String name) {
        JsonElement element = object.get(name);
        if (element == null || element.isJsonNull()) {
            return new ArrayList<>();
        }
        if (!element.isJsonArray()) {
            throw new JsonParseException(name + " must be an array");
        }
        List<String> result = new ArrayList<>();
        for (JsonElement item : element.getAsJsonArray()) {
            if (!item.isJsonPrimitive() || !item.getAsJsonPrimitive().isString()) {
                throw new JsonParseException(name + " must contain strings");
            }
            result.add(item.getAsString());
        }
        return result;
    }

    private static Integer optionalInt(JsonObject object, String name) {
        JsonElement element = object.get(name);
        if (element == null || element.isJsonNull()) {
            return null;
        }
        if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isNumber()) {
            throw new JsonParseException(name + " must be an integer");
        }
        double value = element.getAsDouble();
        if (!Double.isFinite(value) || value != Math.rint(value)
            || value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new JsonParseException(name + " must be an integer");
        }
        return (int) value;
    }

    private static String string(JsonObject object, String name) {
        requireString(object, name);
        return object.get(name).getAsString();
    }

    private static String requireString(JsonObject object, String name) {
        JsonElement element = object.get(name);
        if (element == null || element.isJsonNull() || !element.isJsonPrimitive()
            || !element.getAsJsonPrimitive().isString()) {
            throw new JsonParseException(name + " is required and must be a string");
        }
        return element.getAsString();
    }

    private static void requireArray(JsonObject object, String name) {
        JsonElement element = object.get(name);
        if (element == null || element.isJsonNull() || !element.isJsonArray()) {
            throw new JsonParseException(name + " is required and must be an array");
        }
    }

    private static void requireObject(JsonObject object, String name) {
        JsonElement element = object.get(name);
        if (element == null || element.isJsonNull() || !element.isJsonObject()) {
            throw new JsonParseException(name + " is required and must be an object");
        }
    }

    private static String optionalString(JsonObject object, String name) {
        JsonElement element = object.get(name);
        if (element == null || element.isJsonNull()) {
            return null;
        }
        if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isString()) {
            throw new JsonParseException(name + " must be a string");
        }
        return element.getAsString();
    }

    private static <T extends Enum<T>> T enumValue(JsonObject object, String name, Class<T> type) {
        String value = string(object, name);
        try {
            return Enum.valueOf(type, value);
        } catch (IllegalArgumentException e) {
            throw new JsonParseException("unknown " + name + ": " + value);
        }
    }

    private static void rejectDuplicateKeys(String json) {
        JsonReader reader = new JsonReader(new StringReader(json));
        reader.setLenient(false);
        try {
            consume(reader);
            if (reader.peek() != JsonToken.END_DOCUMENT) {
                throw new JsonParseException("trailing JSON content");
            }
        } catch (IOException e) {
            throw new JsonParseException("invalid JSON", e);
        }
    }

    private static void consume(JsonReader reader) throws IOException {
        JsonToken token = reader.peek();
        if (token == JsonToken.BEGIN_OBJECT) {
            java.util.HashSet<String> names = new java.util.HashSet<>();
            reader.beginObject();
            while (reader.hasNext()) {
                String name = reader.nextName();
                if (!names.add(name)) {
                    throw new JsonParseException("duplicate JSON field: " + name);
                }
                consume(reader);
            }
            reader.endObject();
        } else if (token == JsonToken.BEGIN_ARRAY) {
            reader.beginArray();
            while (reader.hasNext()) {
                consume(reader);
            }
            reader.endArray();
        } else if (token == JsonToken.STRING) {
            reader.nextString();
        } else if (token == JsonToken.NUMBER) {
            reader.nextString();
        } else if (token == JsonToken.BOOLEAN) {
            reader.nextBoolean();
        } else if (token == JsonToken.NULL) {
            reader.nextNull();
        } else {
            throw new JsonParseException("invalid JSON token: " + token);
        }
    }
}
