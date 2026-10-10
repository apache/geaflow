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

package org.apache.geaflow.ai.retrieval.ingest;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;

/** Dataset-neutral extracted graph records. */
public final class ExtractionResult {

    private final List<EntityRef> entities;
    private final List<GraphEdgeRef> edges;

    public ExtractionResult(List<EntityRef> entities, List<GraphEdgeRef> edges) {
        this.entities = immutable(entities, "entities");
        this.edges = immutable(edges, "edges");
    }

    private static <T> List<T> immutable(List<T> values, String name) {
        Objects.requireNonNull(values, name);
        if (values.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException(name + " must not contain null");
        }
        return Collections.unmodifiableList(new java.util.ArrayList<>(values));
    }

    public List<EntityRef> getEntities() {
        return entities;
    }

    public List<GraphEdgeRef> getEdges() {
        return edges;
    }
}
