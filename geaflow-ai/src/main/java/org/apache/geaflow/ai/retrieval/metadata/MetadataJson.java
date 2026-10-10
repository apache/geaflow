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

package org.apache.geaflow.ai.retrieval.metadata;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.ai.retrieval.codec.RetrievalModelJson;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.apache.geaflow.ai.retrieval.model.version.IndexVersion;

/** Constructor-validated JSON boundary; unknown optional fields are ignored. */
public final class MetadataJson {

    private static final Gson GSON = new Gson();

    private MetadataJson() {
    }

    public static String toJson(Object value) {
        return GSON.toJson(value);
    }

    /** Alias for serializers that use a serialize-style name. */
    public static String serialize(Object value) {
        return toJson(value);
    }

    public static <T> T fromJson(String json, Class<T> type) {
        if (json == null) {
            throw new JsonParseException("metadata JSON is required");
        }
        if (type == null) {
            throw new JsonParseException("metadata type is required");
        }
        try {
            return type.cast(decode(new JsonParser().parse(json).getAsJsonObject(), type));
        } catch (RuntimeException e) {
            throw new JsonParseException("invalid metadata: " + e.getMessage(), e);
        }
    }

    /** Alias for callers that use a parse-style name. */
    public static <T> T parse(String json, Class<T> type) {
        return fromJson(json, type);
    }

    private static Object decode(JsonObject object, Class<?> type) {
        if (type == ChunkingConfiguration.class) {
            return new ChunkingConfiguration(
                string(object, "policyVersion"),
                Math.toIntExact(number(object, "chunkSize")),
                Math.toIntExact(number(object, "overlap")));
        }
        if (type == QualityCounters.class) {
            return new QualityCounters(
                number(object, "documents"),
                number(object, "chunks"),
                number(object, "entities"),
                number(object, "edges"),
                number(object, "skippedRecords"),
                number(object, "validationErrors"));
        }
        if (type == DatasetManifest.class) {
            return new DatasetManifest(
                string(object, "manifestVersion"),
                string(object, "dataset"),
                string(object, "datasetRelease"),
                string(object, "split"),
                string(object, "sourceUri"),
                string(object, "cachePath"),
                string(object, "sha256"),
                string(object, "preprocessingVersion"),
                (ChunkingConfiguration) decode(object.getAsJsonObject("chunking"), ChunkingConfiguration.class),
                string(object, "graphSchemaVersion"),
                string(object, "vectorSource"),
                string(object, "vectorVersion"),
                number(object, "randomSeed"));
        }
        if (type == GraphBuildMetadata.class) {
            return new GraphBuildMetadata(
                RetrievalModelJson.fromJson(object.get("graphVersion").toString(), GraphVersion.class),
                string(object, "artifactUri"),
                stringOrDefault(object, "graphType", "graph"),
                stringOrDefault(object, "builderVersion", "unknown"),
                bool(object, "ready"),
                strings(object, "requiredIndexes"));
        }
        if (type == IndexBuildMetadata.class) {
            return new IndexBuildMetadata(
                RetrievalModelJson.fromJson(object.get("graphVersion").toString(), GraphVersion.class),
                RetrievalModelJson.fromJson(object.get("indexVersion").toString(), IndexVersion.class),
                string(object, "indexType"),
                string(object, "builderVersion"),
                string(object, "artifactUri"),
                bool(object, "ready"));
        }
        if (type == ImportMetadata.class) {
            return new ImportMetadata(
                (DatasetManifest) decode(object.getAsJsonObject("manifest"), DatasetManifest.class),
                string(object, "importerVersion"),
                (GraphBuildMetadata) decode(object.getAsJsonObject("graph"), GraphBuildMetadata.class),
                indexes(object),
                (QualityCounters) decode(object.getAsJsonObject("counters"), QualityCounters.class),
                ImportState.valueOf(string(object, "state")),
                string(object, "verifiedSha256"),
                string(object, "failureCode") == null ? null : MetadataException.Code.valueOf(string(object, "failureCode")),
                string(object, "failureReason"),
                number(object, "startedAt"),
                number(object, "updatedAt"));
        }
        throw new JsonParseException("unsupported metadata type: " + type);
    }

    private static String string(JsonObject object, String name) {
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            return null;
        }
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) {
            throw new JsonParseException(name + " must be a string");
        }
        return value.getAsString();
    }

    private static long number(JsonObject object, String name) {
        JsonElement value = object.get(name);
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
            throw new JsonParseException(name + " must be a number");
        }
        return new BigDecimal(value.getAsString()).longValueExact();
    }

    private static boolean bool(JsonObject object, String name) {
        JsonElement value = object.get(name);
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isBoolean()) {
            throw new JsonParseException(name + " must be boolean");
        }
        return value.getAsBoolean();
    }

    private static List<String> strings(JsonObject object, String name) {
        List<String> result = new ArrayList<>();
        JsonElement element = object.get(name);
        if (element == null || element.isJsonNull()) {
            return result;
        }
        if (!element.isJsonArray()) {
            throw new JsonParseException(name + " must be an array");
        }
        for (JsonElement value : element.getAsJsonArray()) {
            if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) {
                throw new JsonParseException(name + " must contain strings");
            }
            result.add(value.getAsString());
        }
        return result;
    }

    private static List<IndexBuildMetadata> indexes(JsonObject object) {
        List<IndexBuildMetadata> result = new ArrayList<>();
        JsonElement element = object.get("indexes");
        if (element == null || element.isJsonNull()) {
            return result;
        }
        if (!element.isJsonArray()) {
            throw new JsonParseException("indexes must be an array");
        }
        for (JsonElement value : element.getAsJsonArray()) {
            if (!value.isJsonObject()) {
                throw new JsonParseException("indexes must contain objects");
            }
            result.add((IndexBuildMetadata) decode(value.getAsJsonObject(), IndexBuildMetadata.class));
        }
        return result;
    }

    private static String stringOrDefault(JsonObject object, String name, String defaultValue) {
        String value = string(object, name);
        return value == null ? defaultValue : value;
    }
}
