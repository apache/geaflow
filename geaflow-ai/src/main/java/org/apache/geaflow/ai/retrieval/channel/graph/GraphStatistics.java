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

package org.apache.geaflow.ai.retrieval.channel.graph;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;

/** Deterministic local graph statistics for one immutable logical view. */
public final class GraphStatistics {

    private final String version;
    private final Map<String, Integer> degrees;
    private final Map<String, Map<String, Integer>> relations;
    private final String digest;

    private GraphStatistics(String version, Map<String, Integer> degrees,
                            Map<String, Map<String, Integer>> relations) {
        this.version = version;
        this.degrees = Collections.unmodifiableMap(new TreeMap<>(degrees));
        Map<String, Map<String, Integer>> relationCopy = new TreeMap<>();
        for (Map.Entry<String, Map<String, Integer>> entry : relations.entrySet()) {
            relationCopy.put(entry.getKey(), Collections.unmodifiableMap(new TreeMap<>(entry.getValue())));
        }
        this.relations = Collections.unmodifiableMap(relationCopy);
        this.digest = digest(this.degrees, this.relations);
    }

    /** Computes degree and relation-label counts from a stable edge collection. */
    public static GraphStatistics from(Iterable<GraphEdgeRef> edges) {
        Map<String, Integer> degrees = new TreeMap<>();
        Map<String, Map<String, Integer>> relations = new TreeMap<>();
        for (GraphEdgeRef edge : edges) {
            increment(degrees, edge.getSourceEntityId());
            incrementRelation(relations, edge.getSourceEntityId(), edge.getLabel());
            if (!edge.getSourceEntityId().equals(edge.getTargetEntityId())) {
                increment(degrees, edge.getTargetEntityId());
                incrementRelation(relations, edge.getTargetEntityId(), edge.getLabel());
            }
        }
        String digest = digest(degrees, relations);
        return new GraphStatistics("stats-" + digest.substring(0, 16), degrees, relations);
    }

    private static void increment(Map<String, Integer> values, String key) {
        values.put(key, values.getOrDefault(key, 0) + 1);
    }

    private static void incrementRelation(Map<String, Map<String, Integer>> values,
                                          String entityId, String label) {
        Map<String, Integer> entityRelations = values.computeIfAbsent(entityId, ignored -> new TreeMap<>());
        entityRelations.put(label, entityRelations.getOrDefault(label, 0) + 1);
    }

    private static String digest(Map<String, Integer> degrees,
                                 Map<String, Map<String, Integer>> relations) {
        StringBuilder canonical = new StringBuilder();
        for (Map.Entry<String, Integer> entry : new TreeMap<>(degrees).entrySet()) {
            canonical.append("d\u001f").append(entry.getKey()).append("\u001f")
                .append(entry.getValue()).append('\n');
        }
        for (Map.Entry<String, Map<String, Integer>> entry : new TreeMap<>(relations).entrySet()) {
            for (Map.Entry<String, Integer> relation : new TreeMap<>(entry.getValue()).entrySet()) {
                canonical.append("r\u001f").append(entry.getKey()).append("\u001f")
                    .append(relation.getKey()).append("\u001f").append(relation.getValue()).append('\n');
            }
        }
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256")
                .digest(canonical.toString().getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(bytes.length * 2);
            for (byte value : bytes) {
                result.append(String.format("%02x", value & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is unavailable", error);
        }
    }

    public String getVersion() {
        return version;
    }

    public String getStatisticsVersion() {
        return version;
    }

    public int getDegree(String entityId) {
        return degrees.getOrDefault(entityId, 0);
    }

    public int degree(String entityId) {
        return getDegree(entityId);
    }

    public Map<String, Integer> getRelationCounts(String entityId) {
        Map<String, Integer> result = relations.get(entityId);
        return result == null ? Collections.emptyMap() : result;
    }

    public Map<String, Integer> relationCounts(String entityId) {
        return getRelationCounts(entityId);
    }

    public Map<String, Integer> getDegrees() {
        return degrees;
    }

    public Map<String, Map<String, Integer>> getRelations() {
        return relations;
    }

    public String getDigest() {
        return digest;
    }
}
