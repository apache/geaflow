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

import java.util.List;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;

/** Versioned graph boundary used by the bounded graph retriever. */
public interface GraphNeighborProvider {

    /** Returns non-null neighbors ordered by edge ID, source, target, and label. */
    List<GraphEdgeRef> neighbors(String entityId);

    /** Returns the graph vertex identity for an entity; providers may preserve distinct IDs. */
    default GraphVertexRef vertex(EntityRef entity) {
        return new GraphVertexRef(entity.getType(), entity.getEntityId(), entity.getEntityId(),
            entity.getDataset(), entity.getDatasetVersion(), entity.getSplit(), entity.getSourceHash());
    }

    /** Returns the pinned graph version when the provider is version-aware. */
    default GraphVersion getGraphVersion() {
        return null;
    }

    /** Returns the deterministic digest of applied deltas, or an empty value for a snapshot. */
    default String getDeltaDigest() {
        return "";
    }

    /** Returns the statistics build version, or an empty value for a legacy provider. */
    default String getStatisticsVersion() {
        return "";
    }

    /** Returns deterministic local graph statistics when available. */
    default GraphStatistics getStatistics() {
        return GraphStatistics.from(java.util.Collections.emptyList());
    }

    /** Returns a stable reader lease over this provider. */
    default GraphQueryLease openLease() {
        return new GraphQueryLease(this);
    }

    /** Returns a stable fingerprint for the logical graph view. */
    default String getFingerprint() {
        return "";
    }
}
