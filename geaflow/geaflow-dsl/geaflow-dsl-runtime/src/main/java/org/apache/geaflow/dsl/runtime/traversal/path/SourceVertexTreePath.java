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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.geaflow.common.utils.ArrayUtil;
import org.apache.geaflow.dsl.common.data.RowVertex;

public class SourceVertexTreePath extends AbstractSingleTreePath {

    /**
     * Vertex.
     */
    private RowVertex vertex;

    private SourceVertexTreePath(Set<Object> requestIds, RowVertex vertex) {
        this.vertex = vertex;
        this.requestIds = requestIds;
    }

    public static SourceVertexTreePath of(Set<Object> requestIds, RowVertex vertex) {
        return new SourceVertexTreePath(requestIds, vertex);
    }

    public static SourceVertexTreePath of(Object requestId, RowVertex vertex) {
        return new SourceVertexTreePath(Sets.newHashSet(requestId), vertex);
    }

    @Override
    public RowVertex getVertex() {
        return vertex;
    }

    @Override
    public void setVertex(RowVertex vertex) {
        this.vertex = Objects.requireNonNull(vertex, "vertex is null");
    }

    @Override
    public Object getVertexId() {
        return vertex.getId();
    }

    @Override
    public NodeType getNodeType() {
        return NodeType.VERTEX_TREE;
    }

    @Override
    public List<ITreePath> getParents() {
        return Collections.emptyList();
    }

    @Override
    public void addParent(ITreePath parent) {
        throw new IllegalArgumentException("Illegal call");
    }

    @Override
    public EdgeSet getEdgeSet() {
        throw new IllegalArgumentException("Illegal call");
    }

    @Override
    public ITreePath copy() {
        return new SourceVertexTreePath(ArrayUtil.copySet(requestIds), vertex);
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
        return of(requestId, vertex);
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public boolean equalNode(ITreePath other) {
        if (other.getNodeType() == NodeType.VERTEX_TREE) {
            return Objects.equals(vertex, other.getVertex());
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SourceVertexTreePath)) {
            return false;
        }
        SourceVertexTreePath that = (SourceVertexTreePath) o;
        return Objects.equals(vertex, that.vertex)
            && Objects.equals(requestIds, that.requestIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertex, requestIds);
    }

    public static class SourceVertexTreePathSerializer extends Serializer<SourceVertexTreePath> {

        @Override
        public void write(Kryo kryo, Output output, SourceVertexTreePath object) {
            kryo.writeClassAndObject(output, object.getRequestIds());
            kryo.writeClassAndObject(output, object.getVertex());
        }

        @Override
        public SourceVertexTreePath read(Kryo kryo, Input input, Class<SourceVertexTreePath> type) {
            Set<Object> requestIds = (Set<Object>) kryo.readClassAndObject(input);
            RowVertex vertex = (RowVertex) kryo.readClassAndObject(input);
            return SourceVertexTreePath.of(requestIds, vertex);
        }

        @Override
        public SourceVertexTreePath copy(Kryo kryo, SourceVertexTreePath original) {
            return (SourceVertexTreePath) original.copy();
        }
    }

}
