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

package org.apache.geaflow.dsl.runtime.traversal.path;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.geaflow.common.utils.ArrayUtil;
import org.apache.geaflow.dsl.common.data.RowVertex;

public class SourceEdgeTreePath extends AbstractSingleTreePath {

    /**
     * Edge sets with same (srcId, targetId) pair.
     */
    private final EdgeSet edges;

    private SourceEdgeTreePath(Set<Object> requestIds, EdgeSet edges) {
        this.edges = edges;
        this.requestIds = requestIds;
    }

    public static SourceEdgeTreePath of(Set<Object> requestIds, EdgeSet edges) {
        return new SourceEdgeTreePath(requestIds, edges);
    }

    public static SourceEdgeTreePath of(Object requestId, EdgeSet edges) {
        if (requestId == null) {
            return of(null, edges);
        }
        return of(Sets.newHashSet(requestId), edges);
    }

    @Override
    public RowVertex getVertex() {
        throw new IllegalArgumentException("Illegal call");
    }

    @Override
    public void setVertex(RowVertex vertex) {
        throw new IllegalArgumentException("Illegal call");
    }

    @Override
    public Object getVertexId() {
        return edges.getTargetId();
    }

    @Override
    public NodeType getNodeType() {
        return NodeType.EDGE_TREE;
    }

    @Override
    public List<ITreePath> getParents() {
        return new ArrayList<>();
    }

    @Override
    public EdgeSet getEdgeSet() {
        return edges;
    }

    @Override
    public ITreePath copy() {
        return new SourceEdgeTreePath(ArrayUtil.copySet(requestIds), edges.copy());
    }

    @Override
    public ITreePath copy(List<ITreePath> parents) {
        assert parents.isEmpty();
        return copy();
    }

    @Override
    public ITreePath getTreePath(Object requestId) {
        if (requestIds != null && !requestIds.contains(requestId)) {
            return null;
        }
        return of(requestId, edges.copy());
    }

    @Override
    public int size() {
        return edges.size();
    }

    @Override
    public boolean equalNode(ITreePath other) {
        if (other.getNodeType() == NodeType.EDGE_TREE) {
            return Objects.equals(edges, other.getEdgeSet());
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SourceEdgeTreePath)) {
            return false;
        }
        SourceEdgeTreePath that = (SourceEdgeTreePath) o;
        return Objects.equals(edges, that.edges) && Objects.equals(requestIds, that.requestIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(edges, requestIds);
    }

    public static class SourceEdgeTreePathSerializer extends Serializer<SourceEdgeTreePath> {

        @Override
        public void write(Kryo kryo, Output output, SourceEdgeTreePath object) {
            kryo.writeClassAndObject(output, object.getRequestIds());
            kryo.writeClassAndObject(output, object.getEdgeSet());
        }

        @Override
        public SourceEdgeTreePath read(Kryo kryo, Input input, Class<SourceEdgeTreePath> type) {
            Set<Object> requestIds = (Set<Object>) kryo.readClassAndObject(input);
            EdgeSet edges = (EdgeSet) kryo.readClassAndObject(input);
            return SourceEdgeTreePath.of(requestIds, edges);
        }

        @Override
        public SourceEdgeTreePath copy(Kryo kryo, SourceEdgeTreePath original) {
            return (SourceEdgeTreePath) original.copy();
        }
    }

}
