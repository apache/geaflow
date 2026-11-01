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

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/** Immutable identifiers deleted by one graph delta. */
public final class GraphTombstones {

    private final Set<String> chunks;
    private final Set<String> entities;
    private final Set<String> vertices;
    private final Set<String> edges;
    private final Set<String> aliases;

    public GraphTombstones(Set<String> chunks, Set<String> entities, Set<String> edges,
                           Set<String> aliases) {
        this(chunks, entities, Collections.emptySet(), edges, aliases);
    }

    public GraphTombstones(Set<String> chunks, Set<String> entities, Set<String> vertices,
                           Set<String> edges, Set<String> aliases) {
        this.chunks = immutable(chunks);
        this.entities = immutable(entities);
        this.vertices = immutable(vertices);
        this.edges = immutable(edges);
        this.aliases = immutable(aliases);
    }

    public static GraphTombstones empty() {
        return new GraphTombstones(Collections.emptySet(), Collections.emptySet(),
            Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    }

    private static Set<String> immutable(Set<String> values) {
        return Collections.unmodifiableSet(new TreeSet<>(values == null ? Collections.emptySet() : values));
    }

    public Set<String> getChunks() {
        return chunks;
    }

    public Set<String> getEntities() {
        return entities;
    }

    public Set<String> getVertices() {
        return vertices;
    }

    public Set<String> getEdges() {
        return edges;
    }

    /**
     * Alias keys are encoded as {@code entityId\u001falias}.
     */
    public Set<String> getAliases() {
        return aliases;
    }

    public boolean deletesAlias(String entityId, String alias) {
        return aliases.contains(aliasKey(entityId, alias));
    }

    public static String aliasKey(String entityId, String alias) {
        return entityId + "\u001f" + alias;
    }
}
