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
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.LongSupplier;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;

/** Deterministic canonical-name and alias resolver used as the graph expansion boundary. */
public final class EntityAnchorResolver {
    public List<Anchor> resolve(String query, List<EntityRef> entities) {
        return resolve(query, entities, 0L, System::nanoTime);
    }

    public List<Anchor> resolve(String query, List<EntityRef> entities, long deadlineNanos, LongSupplier clock) {
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
                result.add(new Anchor(entity, "CANONICAL", 1.0));
            } else {
                for (String alias : entity.getAliases()) {
                    if (normalized.equals(normalize(alias))) {
                        result.add(new Anchor(entity, "ALIAS", 0.9));
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
