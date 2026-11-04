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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
                number(object, "randomSeed"),
                string(object, "entityExtractorVersion"),
                budgetConfiguration(object));
        }
        if (type == GraphBuildMetadata.class) {
            GraphBuildMetadata decoded = decodeGraphMetadata(object);
            return applyAssociationScore(decoded, object);
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

    /**
     * Restores the association-score components recorded with a graph build.
     *
     * <p>Artifacts published before these components existed carry no field and stay null rather
     * than silently defaulting, so a replay can tell "not recorded" from "recorded as default".
     */
    private static GraphBuildMetadata applyAssociationScore(GraphBuildMetadata metadata,
                                                            JsonObject object) {
        JsonElement element = object.get("associationScoreMetadata");
        if (element == null || element.isJsonNull()) {
            element = object.get("associationScore");
        }
        if (element == null || element.isJsonNull()) {
            return metadata;
        }
        JsonObject value = element.getAsJsonObject();
        Map<String, Double> priorities = new LinkedHashMap<>();
        JsonElement priors = value.get("relationPriorities");
        if (priors != null && priors.isJsonObject()) {
            for (Map.Entry<String, JsonElement> entry : priors.getAsJsonObject().entrySet()) {
                priorities.put(entry.getKey(), entry.getValue().getAsDouble());
            }
        }
        return metadata.withAssociationScoreMetadata(new AssociationScoreMetadata(
            value.get("hopDecay").getAsDouble(),
            value.get("degreePenalty").getAsDouble(),
            value.get("anchorScoreWeight").getAsDouble(),
            value.get("edgeStrengthWeight").getAsDouble(),
            value.get("sourceRelevanceWeight").getAsDouble(),
            priorities,
            stringOrDefault(value, "edgeStrengthVersion", "relation-prior-v1"),
            stringOrDefault(value, "scoreVersion", "association-score-v1"),
            optionalString(value, "timeDecayVersion"),
            optionalNumber(value, "timeDecayWeight", 0.0D),
            optionalNumber(value, "timeDecayHalfLifeMillis", 0.0D),
            optionalNumber(value, "unrelatedSourceRelevance", 0.5D),
            optionalNumber(value, "anchorHitBaseScore", 1.0D)));
    }

    private static GraphBuildMetadata decodeGraphMetadata(JsonObject object) {
        {
            String snapshotUri = string(object, "snapshotUri");
            String deltaUri = string(object, "deltaUri");
            String artifactUri = string(object, "artifactUri");
            org.apache.geaflow.ai.retrieval.model.version.GraphVersion parent = object.has("parentGraphVersion")
                && !object.get("parentGraphVersion").isJsonNull()
                ? RetrievalModelJson.fromJson(object.get("parentGraphVersion").toString(), GraphVersion.class) : null;
            if (parent != null || snapshotUri != null || deltaUri != null || string(object, "tombstoneUri") != null) {
                return new GraphBuildMetadata(
                    RetrievalModelJson.fromJson(object.get("graphVersion").toString(), GraphVersion.class),
                    parent,
                    snapshotUri == null ? artifactUri : snapshotUri,
                    deltaUri,
                    string(object, "tombstoneUri"),
                    string(object, "deltaDigest"),
                    string(object, "statisticsVersion"),
                    stringOrDefault(object, "graphType", "graph"),
                    stringOrDefault(object, "builderVersion", "unknown"),
                    bool(object, "ready"),
                    strings(object, "requiredIndexes"));
            }
            return new GraphBuildMetadata(
                RetrievalModelJson.fromJson(object.get("graphVersion").toString(), GraphVersion.class),
                artifactUri,
                stringOrDefault(object, "graphType", "graph"),
                stringOrDefault(object, "builderVersion", "unknown"),
                bool(object, "ready"),
                strings(object, "requiredIndexes"));
        }
    }


    /**
     * Decodes the recorded budget configuration from a dataset manifest.
     *
     * <p>Absent means the manifest predates the field; it must stay null rather than defaulting, so
     * "never recorded" cannot be mistaken for "recorded as standard".
     */
    private static BudgetConfiguration budgetConfiguration(JsonObject object) {
        JsonElement element = object.get("budgetConfiguration");
        if (element == null || element.isJsonNull() || !element.isJsonObject()) {
            return null;
        }
        JsonObject value = element.getAsJsonObject();
        return new BudgetConfiguration(
            string(value, "primaryConstraint"),
            (int) number(value, "maxCandidates"),
            (int) number(value, "timeoutMs"),
            (int) number(value, "tokenBudget"),
            (int) number(value, "maxEdgeScans"));
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

    private static String optionalString(JsonObject object, String name) {
        return string(object, name);
    }

    private static double optionalNumber(JsonObject object, String name, double defaultValue) {
        JsonElement element = object.get(name);
        return element == null || element.isJsonNull() ? defaultValue : element.getAsDouble();
    }
}
