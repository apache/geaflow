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

public class VertexTreePath extends AbstractSingleTreePath {

    /**
     * Parent node.
     */
    private List<ITreePath> parents = new ArrayList<>();

    /**
     * Vertex.
     */
    private RowVertex vertex;

    private VertexTreePath(Object sessionId, RowVertex vertex) {
        this(Sets.newHashSet(sessionId), vertex);
    }

    private VertexTreePath(Set<Object> requestIds, RowVertex vertex) {
        this.requestIds = requestIds;
        this.vertex = vertex;
    }

    public static VertexTreePath of(Object sessionId, RowVertex vertex) {
        return new VertexTreePath(sessionId, vertex);
    }

    public static VertexTreePath of(Set<Object> requestIds, RowVertex vertex) {
        return new VertexTreePath(requestIds, vertex);
    }

    public RowVertex getVertex() {
        return vertex;
    }

    @Override
    public void setVertex(RowVertex vertex) {
        this.vertex = Objects.requireNonNull(vertex, "vertex is null");
    }

    @Override
    public Object getVertexId() {
        return vertex != null ? vertex.getId() : null;
    }

    @Override
    public List<ITreePath> getParents() {
        return parents;
    }

    @Override
    public EdgeSet getEdgeSet() {
        throw new IllegalArgumentException("Illegal call");
    }

    @Override
    public VertexTreePath copy() {
        VertexTreePath copyTree = new VertexTreePath(ArrayUtil.copySet(requestIds), vertex);
        List<ITreePath> parentsCopy = new ArrayList<>();
        for (ITreePath parent : parents) {
            parentsCopy.add(parent.copy());
        }
        copyTree.parents = parentsCopy;
        return copyTree;
    }

    @Override
    public ITreePath copy(List<ITreePath> parents) {
        VertexTreePath copyTree = new VertexTreePath(ArrayUtil.copySet(requestIds), vertex);
        copyTree.parents = parents;
        return copyTree;
    }

    @Override
    public ITreePath getTreePath(Object requestId) {
        if (requestIds != null && !requestIds.contains(requestId)) {
            return null;
        }
        ITreePath treePathOnSession = of(requestId, vertex);
        for (ITreePath parent : parents) {
            ITreePath sessionParent = parent.getTreePath(requestId);
            if (sessionParent != null) {
                treePathOnSession.addParent(sessionParent);
            }
        }
        return treePathOnSession;
    }

    @Override
    public int size() {
        if (parents.isEmpty()) {
            return 1;
        }
        int size = 0;
        for (ITreePath parent : parents) {
            size += parent.size();
        }
        return size;
    }

    @Override
    public boolean equalNode(ITreePath other) {
        if (other.getNodeType() == NodeType.VERTEX_TREE) {
            return Objects.equals(vertex, other.getVertex());
        }
        return false;
    }

    @Override
    public NodeType getNodeType() {
        return NodeType.VERTEX_TREE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof VertexTreePath)) {
            return false;
        }
        VertexTreePath that = (VertexTreePath) o;
        return Objects.equals(parents, that.parents) && Objects.equals(vertex, that.vertex)
            && Objects.equals(requestIds, that.requestIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parents, vertex, requestIds);
    }

    public static class VertexTreePathSerializer extends Serializer<VertexTreePath> {

        @Override
        public void write(Kryo kryo, Output output, VertexTreePath object) {
            kryo.writeClassAndObject(output, object.getRequestIds());
            kryo.writeClassAndObject(output, object.getParents());
            kryo.writeClassAndObject(output, object.getVertex());
        }

        @Override
        public VertexTreePath read(Kryo kryo, Input input, Class<VertexTreePath> type) {
            Set<Object> requestIds = (Set<Object>) kryo.readClassAndObject(input);
            List<ITreePath> parents = (List<ITreePath>) kryo.readClassAndObject(input);
            RowVertex vertex = (RowVertex) kryo.readClassAndObject(input);
            VertexTreePath vertexTreePath = VertexTreePath.of(requestIds, vertex);
            vertexTreePath.parents.addAll(parents);
            return vertexTreePath;
        }

        @Override
        public VertexTreePath copy(Kryo kryo, VertexTreePath original) {
            return original.copy();
        }
    }

}
