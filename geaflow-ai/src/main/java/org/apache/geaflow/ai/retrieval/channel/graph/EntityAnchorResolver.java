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

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;

/** Deterministic canonical-name and alias resolver used as the graph expansion boundary. */
public final class EntityAnchorResolver {
    private final List<NameEntry> names;
    /** Confidence assigned when the query matches an entity's canonical name exactly. */
    public static final double CANONICAL_CONFIDENCE = 1.0D;
    /** Confidence assigned when the query matches a declared alias. */
    public static final double ALIAS_CONFIDENCE = 0.9D;
    /**
     * Confidence assigned when no name matches and anchors are recovered from retrieved chunks.
     *
     * <p>Kept deliberately below {@code strongAnchorConfidence} defaults so a recovered anchor does
     * not unlock a second hop on the strength of a text hit alone.
     */
    public static final double TEXT_CANDIDATE_CONFIDENCE = 0.35D;
    static final String CANONICAL_TYPE = "CANONICAL";
    static final String ALIAS_TYPE = "ALIAS";
    static final String TEXT_CANDIDATE_TYPE = "TEXT_CANDIDATE";

    private final Map<String, List<NameEntry>> namesByToken;
    private final Map<String, List<EntityRef>> entitiesBySourceChunk;

    public EntityAnchorResolver() {
        this.names = Collections.emptyList();
        this.namesByToken = Collections.emptyMap();
        this.entitiesBySourceChunk = Collections.emptyMap();
    }

    /** Builds a reusable normalized name index for an immutable graph fixture. */
    public EntityAnchorResolver(List<EntityRef> entities) {
        Map<String, NameEntry> indexed = new LinkedHashMap<>();
        Map<String, List<EntityRef>> sourceEntities = new HashMap<>();
        if (entities != null) {
            for (EntityRef entity : entities) {
                if (entity == null) {
                    continue;
                }
                add(indexed, entity.getCanonicalName(), entity, CANONICAL_TYPE, CANONICAL_CONFIDENCE);
                for (String alias : entity.getAliases()) {
                    add(indexed, alias, entity, ALIAS_TYPE, ALIAS_CONFIDENCE);
                }
                for (String chunkId : entity.getSourceChunkIds()) {
                    sourceEntities.computeIfAbsent(chunkId, ignored -> new ArrayList<>()).add(entity);
                }
            }
        }
        List<NameEntry> ordered = new ArrayList<>(indexed.values());
        ordered.sort(Comparator.comparingInt((NameEntry value) -> value.name.length()).reversed()
            .thenComparing(value -> value.entity.getEntityId()).thenComparing(value -> value.name));
        this.names = Collections.unmodifiableList(ordered);
        Map<String, List<NameEntry>> byToken = new HashMap<>();
        for (NameEntry entry : ordered) {
            String[] parts = entry.name.split(" ");
            if (parts.length == 0 || parts[0].isEmpty()) {
                continue;
            }
            byToken.computeIfAbsent(parts[0], ignored -> new ArrayList<>()).add(entry);
        }
        for (List<NameEntry> entries : byToken.values()) {
            entries.sort(Comparator.comparingInt((NameEntry value) -> value.name.length()).reversed()
                .thenComparing(value -> value.entity.getEntityId()).thenComparing(value -> value.name));
        }
        this.namesByToken = Collections.unmodifiableMap(byToken);
        for (List<EntityRef> values : sourceEntities.values()) {
            values.sort(Comparator.comparing(EntityRef::getEntityId));
        }
        this.entitiesBySourceChunk = Collections.unmodifiableMap(sourceEntities);
    }

    private static void add(Map<String, NameEntry> indexed, String name, EntityRef entity,
                            String type, double confidence) {
        String normalized = normalize(name);
        if (!normalized.isEmpty()) {
            String key = entity.getEntityId().length() + ":" + entity.getEntityId() + normalized;
            NameEntry previous = indexed.get(key);
            if (previous == null || confidence > previous.confidence) {
                indexed.put(key, new NameEntry(normalized, entity, type, confidence));
            }
        }
    }

    /** Resolves question mentions using a fixture index built once at load time. */
    public List<Anchor> resolve(String query, long deadlineNanos, LongSupplier clock) {
        if (query == null || names.isEmpty()) {
            return Collections.emptyList();
        }
        String normalized = normalize(query);
        Map<String, Anchor> matches = new LinkedHashMap<>();
        Set<NameEntry> candidates = new HashSet<>();
        for (String token : normalized.split(" ")) {
            List<NameEntry> entries = namesByToken.get(token);
            if (entries != null) {
                candidates.addAll(entries);
            }
        }
        List<NameEntry> orderedCandidates = new ArrayList<>(candidates);
        orderedCandidates.sort(Comparator.comparingInt((NameEntry value) -> value.name.length()).reversed()
            .thenComparing(value -> value.entity.getEntityId()).thenComparing(value -> value.name));
        for (NameEntry entry : orderedCandidates) {
            if (deadlineNanos != 0L && clock.getAsLong() - deadlineNanos >= 0L) {
                break;
            }
            if (containsName(normalized, entry.name)) {
                Anchor previous = matches.get(entry.entity.getEntityId());
                if (previous == null || entry.confidence > previous.getConfidence()) {
                    matches.put(entry.entity.getEntityId(), new Anchor(entry.entity, entry.type, entry.confidence));
                }
            }
        }
        List<Anchor> result = new ArrayList<>(matches.values());
        result.sort(Comparator.comparing(Anchor::getConfidence).reversed()
            .thenComparing(a -> a.getEntity().getEntityId()));
        return result;
    }

    /** Resolves anchors from already retrieved chunks when the question has no name match. */
    public List<Anchor> resolveFromCandidates(String query, List<EntityRef> entities, List<String> chunkIds,
                                              int maxAnchors, long deadlineNanos, LongSupplier clock) {
        List<Anchor> direct = resolve(query, entities, deadlineNanos, clock);
        if (!direct.isEmpty() || chunkIds == null || maxAnchors < 1) {
            return direct;
        }
        Map<String, EntityRef> matches = new LinkedHashMap<>();
        for (String chunkId : chunkIds) {
            if (deadlineNanos != 0L && clock.getAsLong() - deadlineNanos >= 0L) {
                break;
            }
            for (EntityRef entity : entitiesBySourceChunk.getOrDefault(chunkId, Collections.emptyList())) {
                matches.putIfAbsent(entity.getEntityId(), entity);
                if (matches.size() >= maxAnchors) {
                    break;
                }
            }
            if (matches.size() >= maxAnchors) {
                break;
            }
        }
        List<Anchor> result = new ArrayList<>();
        for (EntityRef entity : matches.values()) {
            result.add(new Anchor(entity, TEXT_CANDIDATE_TYPE, TEXT_CANDIDATE_CONFIDENCE));
        }
        return result;
    }

    private static boolean containsName(String query, String name) {
        int offset = -1;
        while ((offset = query.indexOf(name, offset + 1)) >= 0) {
            int end = offset + name.length();
            boolean leftBoundary = offset == 0 || !Character.isLetterOrDigit(query.charAt(offset - 1));
            boolean rightBoundary = end == query.length() || !Character.isLetterOrDigit(query.charAt(end));
            if (leftBoundary && rightBoundary) {
                return true;
            }
        }
        return false;
    }

    public List<Anchor> resolve(String query, List<EntityRef> entities) {
        return resolve(query, entities, 0L, System::nanoTime);
    }

    public List<Anchor> resolve(String query, List<EntityRef> entities, long deadlineNanos, LongSupplier clock) {
        if (!names.isEmpty()) {
            return resolve(query, deadlineNanos, clock);
        }
        if (query == null || entities == null) {
            return Collections.emptyList();
        }
        String normalized = normalize(query);
        if (normalized.isEmpty()) {
            return Collections.emptyList();
        }
        List<Anchor> result = new ArrayList<>();
        for (EntityRef entity : entities) {
            if (deadlineNanos != 0L && clock.getAsLong() - deadlineNanos >= 0L) {
                break;
            }
            if (entity == null) {
                continue;
            }
            String canonical = normalize(entity.getCanonicalName());
            if (normalized.equals(canonical)) {
                result.add(new Anchor(entity, CANONICAL_TYPE, CANONICAL_CONFIDENCE));
            } else {
                for (String alias : entity.getAliases()) {
                    if (normalized.equals(normalize(alias))) {
                        result.add(new Anchor(entity, ALIAS_TYPE, ALIAS_CONFIDENCE));
                        break;
                    }
                }
            }
        }
        result.sort(Comparator.comparing(Anchor::getConfidence).reversed()
            .thenComparing(a -> a.getEntity().getEntityId()));
        return result;
    }

    public static String normalize(String value) {
        if (value == null) {
            return "";
        }
        String normalized = Normalizer.normalize(value, Normalizer.Form.NFKC);
        return normalized.trim().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
    }

    private static final class NameEntry {
        private final String name;
        private final EntityRef entity;
        private final String type;
        private final double confidence;

        private NameEntry(String name, EntityRef entity, String type, double confidence) {
            this.name = name;
            this.entity = entity;
            this.type = type;
            this.confidence = confidence;
        }
    }

    /** An entity matched to a query with deterministic confidence metadata. */
    public static final class Anchor {
        private final EntityRef entity;
        private final String matchType;
        private final double confidence;

        public Anchor(EntityRef entity, String matchType, double confidence) {
            this.entity = Objects.requireNonNull(entity, "entity");
            this.matchType = Objects.requireNonNull(matchType, "matchType");
            this.confidence = confidence;
        }

        public EntityRef getEntity() {
            return entity;
        }

        public String getMatchType() {
            return matchType;
        }

        public double getConfidence() {
            return confidence;
        }
    }
}
