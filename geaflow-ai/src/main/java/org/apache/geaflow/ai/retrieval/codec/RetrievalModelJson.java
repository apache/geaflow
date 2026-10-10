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

package org.apache.geaflow.ai.retrieval.codec;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.model.document.SourceDocument;
import org.apache.geaflow.ai.retrieval.model.document.SourceRef;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.evidence.ChannelScore;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.evidence.EvidenceKind;
import org.apache.geaflow.ai.retrieval.model.evidence.GraphEvidenceTrace;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.apache.geaflow.ai.retrieval.model.version.IndexVersion;

/**
 * Validated JSON boundary for retrieval domain models.
 *
 * <p>The codec keeps deserialization on the public constructors so required fields, ranges, and
 * immutable collection guarantees are applied consistently. Unknown properties are ignored for
 * additive wire compatibility; callers should use this class instead of Gson directly.</p>
 */
public final class RetrievalModelJson {

    private static final Gson GSON = new Gson();

    private RetrievalModelJson() {
    }

    public static String toJson(Object value) {
        return GSON.toJson(value);
    }

    public static <T> T fromJson(String json, Class<T> type) {
        if (json == null) {
            throw new NullPointerException("json");
        }
        if (type == null) {
            throw new NullPointerException("type");
        }
        JsonElement element = new JsonParser().parse(json);
        if (!element.isJsonObject()) {
            throw new JsonParseException("model JSON must be an object");
        }
        try {
            Object value = parse(element.getAsJsonObject(), type);
            return type.cast(value);
        } catch (JsonParseException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new JsonParseException("invalid " + type.getSimpleName() + ": "
                + e.getMessage(), e);
        }
    }

    private static Object parse(JsonObject object, Class<?> type) {
        if (type == SourceDocument.class) {
            return new SourceDocument(requiredString(object, "documentId"),
                requiredString(object, "dataset"), requiredString(object, "datasetVersion"),
                requiredString(object, "split"), optionalString(object, "title"),
                requiredString(object, "sourceUri"), requiredString(object, "sourceHash"));
        } else if (type == TextChunk.class) {
            return new TextChunk(requiredString(object, "chunkId"),
                requiredString(object, "documentId"), requiredInt(object, "chunkIndex"),
                requiredInt(object, "startOffset"), requiredInt(object, "endOffset"),
                requiredInt(object, "tokenEstimate"), requiredString(object, "text"),
                optionalString(object, "policyVersion"), optionalString(object, "textHash"),
                optionalString(object, "sourceUri"));
        } else if (type == EntityRef.class) {
            return new EntityRef(requiredString(object, "entityId"),
                requiredString(object, "canonicalName"), strings(object, "aliases"),
                requiredString(object, "type"), strings(object, "sourceChunkIds"));
        } else if (type == GraphVertexRef.class) {
            return new GraphVertexRef(requiredString(object, "label"),
                requiredString(object, "vertexId"), requiredString(object, "entityId"));
        } else if (type == GraphEdgeRef.class) {
            return new GraphEdgeRef(requiredString(object, "edgeId"),
                requiredString(object, "label"), requiredString(object, "sourceEntityId"),
                requiredString(object, "targetEntityId"),
                strings(object, "sourceChunkIds"));
        } else if (type == GraphPathRef.class) {
            return new GraphPathRef(strings(object, "vertexIds"),
                strings(object, "edgeIds"), requiredInt(object, "hop"),
                requiredBoolean(object, "sampled"));
        } else if (type == SourceRef.class) {
            return new SourceRef(requiredString(object, "documentId"),
                requiredString(object, "sourceUri"), optionalInt(object, "startOffset"),
                optionalInt(object, "endOffset"));
        } else if (type == ChannelScore.class) {
            return new ChannelScore(requiredString(object, "channel"),
                requiredDouble(object, "rawScore"), optionalDouble(object, "normalizedScore"),
                requiredInt(object, "rank"));
        } else if (type == GraphVersion.class) {
            return new GraphVersion(requiredString(object, "graphName"),
                requiredString(object, "version"));
        } else if (type == IndexVersion.class) {
            return new IndexVersion(requiredString(object, "indexName"),
                requiredString(object, "version"), requiredString(object, "graphVersion"));
        } else if (type == Evidence.class) {
            return new Evidence(optionalString(object, "evidenceId"), kind(object),
                optionalString(object, "text"), models(object, "chunks", TextChunk.class),
                models(object, "entities", EntityRef.class), models(object, "paths", GraphPathRef.class),
                models(object, "sources", SourceRef.class), scores(object),
                optionalDouble(object, "fusedScore"), optionalDouble(object, "finalScore"),
                optionalInt(object, "rank"), models(object, "graphTraces", GraphEvidenceTrace.class));
        } else if (type == GraphEvidenceTrace.class) {
            JsonObject path = object.getAsJsonObject("path");
            return new GraphEvidenceTrace(requiredString(object, "queryId"),
                requiredString(object, "graphVersion"), requiredString(object, "anchorEntityId"),
                requiredString(object, "anchorMatchType"), requiredDouble(object, "anchorConfidence"),
                (GraphPathRef) parse(path, GraphPathRef.class), strings(object, "supportingChunkIds"));
        }
        throw new JsonParseException("unsupported retrieval model: " + type.getName());
    }

    private static EvidenceKind kind(JsonObject object) {
        String value = requiredString(object, "kind");
        try {
            return EvidenceKind.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new JsonParseException("unknown evidence kind: " + value);
        }
    }

    private static Map<String, ChannelScore> scores(JsonObject object) {
        JsonElement element = object.get("stageScores");
        if (element == null || element.isJsonNull()) {
            return Collections.emptyMap();
        }
        if (!element.isJsonObject()) {
            throw new JsonParseException("stageScores must be an object");
        }
        Map<String, ChannelScore> result = new LinkedHashMap<>();
        for (Map.Entry<String, JsonElement> entry : element.getAsJsonObject().entrySet()) {
            if (!entry.getValue().isJsonObject()) {
                throw new JsonParseException("stage score must be an object");
            }
            result.put(entry.getKey(), fromElement(entry.getValue(), ChannelScore.class));
        }
        return result;
    }

    private static <T> List<T> models(JsonObject object, String name, Class<T> type) {
        JsonElement element = object.get(name);
        if (element == null || element.isJsonNull()) {
            return Collections.emptyList();
        }
        if (!element.isJsonArray()) {
            throw new JsonParseException(name + " must be an array");
        }
        List<T> result = new ArrayList<>();
        for (JsonElement item : element.getAsJsonArray()) {
            if (!item.isJsonObject()) {
                throw new JsonParseException(name + " elements must be objects");
            }
            result.add(fromElement(item, type));
        }
        return result;
    }

    private static List<String> strings(JsonObject object, String name) {
        JsonElement element = object.get(name);
        if (element == null || element.isJsonNull()) {
            return Collections.emptyList();
        }
        if (!element.isJsonArray()) {
            throw new JsonParseException(name + " must be an array");
        }
        List<String> result = new ArrayList<>();
        JsonArray array = element.getAsJsonArray();
        for (JsonElement item : array) {
            if (item.isJsonNull() || !item.isJsonPrimitive()
                || !item.getAsJsonPrimitive().isString()) {
                throw new JsonParseException(name + " must contain strings");
            }
            result.add(item.getAsString());
        }
        return result;
    }

    private static <T> T fromElement(JsonElement element, Class<T> type) {
        return type.cast(parse(element.getAsJsonObject(), type));
    }

    private static String requiredString(JsonObject object, String name) {
        JsonElement element = object.get(name);
        if (element == null || element.isJsonNull() || !element.isJsonPrimitive()
            || !element.getAsJsonPrimitive().isString()) {
            throw new JsonParseException(name + " is required");
        }
        return element.getAsString();
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

    private static int requiredInt(JsonObject object, String name) {
        JsonElement element = object.get(name);
        if (element == null || element.isJsonNull() || !element.isJsonPrimitive()
            || !element.getAsJsonPrimitive().isNumber()) {
            throw new JsonParseException(name + " is required");
        }
        int value = element.getAsInt();
        if (element.getAsDouble() != value) {
            throw new JsonParseException(name + " must be an integer");
        }
        return value;
    }

    private static Integer optionalInt(JsonObject object, String name) {
        JsonElement element = object.get(name);
        if (element == null || element.isJsonNull()) {
            return null;
        }
        if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isNumber()) {
            throw new JsonParseException(name + " must be a number");
        }
        int value = element.getAsInt();
        if (element.getAsDouble() != value) {
            throw new JsonParseException(name + " must be an integer");
        }
        return value;
    }

    private static double requiredDouble(JsonObject object, String name) {
        JsonElement element = object.get(name);
        if (element == null || element.isJsonNull() || !element.isJsonPrimitive()
            || !element.getAsJsonPrimitive().isNumber()) {
            throw new JsonParseException(name + " is required");
        }
        return element.getAsDouble();
    }

    private static Double optionalDouble(JsonObject object, String name) {
        JsonElement element = object.get(name);
        if (element == null || element.isJsonNull()) {
            return null;
        }
        if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isNumber()) {
            throw new JsonParseException(name + " must be a number");
        }
        return element.getAsDouble();
    }

    private static boolean requiredBoolean(JsonObject object, String name) {
        JsonElement element = object.get(name);
        if (element == null || element.isJsonNull() || !element.isJsonPrimitive()
            || !element.getAsJsonPrimitive().isBoolean()) {
            throw new JsonParseException(name + " is required");
        }
        return element.getAsBoolean();
    }
}
