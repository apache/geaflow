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

package org.apache.geaflow.api.graph.sampling;

import java.io.Serializable;
import java.util.Objects;
import org.apache.geaflow.model.graph.IGraphElementWithLabelField;
import org.apache.geaflow.model.graph.IGraphElementWithTimeField;
import org.apache.geaflow.model.graph.edge.IEdge;

/** Stable identity of a normalized logical edge, independent of storage replicas and values. */
public final class LogicalEdgeId<K> implements Serializable {

    private final K sourceId;
    private final K targetId;
    private final String label;
    private final Long time;

    public LogicalEdgeId(K sourceId, K targetId, String label, Long time) {
        this.sourceId = Objects.requireNonNull(sourceId, "sourceId");
        this.targetId = Objects.requireNonNull(targetId, "targetId");
        this.label = label;
        this.time = time;
    }

    public static <K> LogicalEdgeId<K> fromNormalized(IEdge<K, ?> edge) {
        Objects.requireNonNull(edge, "edge");
        String edgeLabel = edge instanceof IGraphElementWithLabelField
            ? ((IGraphElementWithLabelField) edge).getLabel() : null;
        Long edgeTime = edge instanceof IGraphElementWithTimeField
            ? ((IGraphElementWithTimeField) edge).getTime() : null;
        return new LogicalEdgeId<>(edge.getSrcId(), edge.getTargetId(), edgeLabel, edgeTime);
    }

    public K getSourceId() {
        return sourceId;
    }

    public K getTargetId() {
        return targetId;
    }

    public String getLabel() {
        return label;
    }

    public Long getTime() {
        return time;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof LogicalEdgeId)) {
            return false;
        }
        LogicalEdgeId<?> that = (LogicalEdgeId<?>) other;
        return Objects.equals(sourceId, that.sourceId)
            && Objects.equals(targetId, that.targetId)
            && Objects.equals(label, that.label)
            && Objects.equals(time, that.time);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceId, targetId, label, time);
    }
}
