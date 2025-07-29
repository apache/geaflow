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
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;

public class EdgeTreePath extends AbstractSingleTreePath {

    /**
     * The parent nodes.
     */
    private List<ITreePath> parents = new ArrayList<>();

    /**
     * Edge sets with same (srcId, targetId) pair.
     */
    private final EdgeSet edges;

    private EdgeTreePath(Set<Object> requestIds, EdgeSet edges) {
        this.edges = Objects.requireNonNull(edges, "edges is null");
        this.requestIds = requestIds;
    }

    public static EdgeTreePath of(Set<Object> requestIds, EdgeSet edges) {
        return new EdgeTreePath(requestIds, edges);
    }

    public static EdgeTreePath of(Object requestId, RowEdge edge) {
        if (requestId == null) {
            return of(null, edge);
        }
        return of(Sets.newHashSet(requestId), edge);
    }

    public static EdgeTreePath of(Set<Object> requestIds, RowEdge edge) {
        EdgeSet edgeSet = new DefaultEdgeSet();
        edgeSet.addEdge(edge);
        return new EdgeTreePath(requestIds, edgeSet);
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
        return parents;
    }

    @Override
    public EdgeSet getEdgeSet() {
        return edges;
    }

    @Override
    public ITreePath copy() {
        EdgeTreePath copyTree = new EdgeTreePath(ArrayUtil.copySet(requestIds), edges.copy());
        List<ITreePath> copyParents = new ArrayList<>();
        for (ITreePath parent : parents) {
            copyParents.add(parent.copy());
        }
        copyTree.parents = copyParents;
        return copyTree;
    }

    @Override
    public ITreePath copy(List<ITreePath> parents) {
        EdgeTreePath copyTree = new EdgeTreePath(ArrayUtil.copySet(requestIds), edges.copy());
        copyTree.parents = parents;
        return copyTree;
    }

    @Override
    public ITreePath getTreePath(Object sessionId) {
        if (requestIds != null && !requestIds.contains(sessionId)) {
            return null;
        }
        ITreePath treePathOnSession = new EdgeTreePath(Sets.newHashSet(sessionId), edges.copy());
        for (ITreePath parent : parents) {
            ITreePath sessionParent = parent.getTreePath(sessionId);
            if (sessionParent != null) {
                treePathOnSession.addParent(sessionParent);
            }
        }
        return treePathOnSession;
    }

    @Override
    public int size() {
        if (parents.isEmpty()) {
            return edges.size();
        }
        int parentSize = 0;
        for (ITreePath parent : parents) {
            parentSize += parent.size();
        }
        return edges.size() * parentSize;
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
        if (!(o instanceof EdgeTreePath)) {
            return false;
        }
        EdgeTreePath that = (EdgeTreePath) o;
        return Objects.equals(parents, that.parents) && Objects.equals(edges, that.edges)
            && Objects.equals(requestIds, that.requestIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parents, edges, requestIds);
    }


    public static class EdgeTreePathSerializer extends Serializer<EdgeTreePath> {

        @Override
        public void write(Kryo kryo, Output output, EdgeTreePath object) {
            kryo.writeClassAndObject(output, object.getRequestIds());
            kryo.writeClassAndObject(output, object.getParents());
            kryo.writeClassAndObject(output, object.getEdgeSet());
        }

        @Override
        public EdgeTreePath read(Kryo kryo, Input input, Class<EdgeTreePath> type) {
            Set<Object> requestIds = (Set<Object>) kryo.readClassAndObject(input);
            List<ITreePath> parents = (List<ITreePath>) kryo.readClassAndObject(input);
            EdgeSet edges = (EdgeSet) kryo.readClassAndObject(input);
            EdgeTreePath treePath = EdgeTreePath.of(requestIds, edges);
            treePath.parents.addAll(parents);
            return treePath;
        }

        @Override
        public EdgeTreePath copy(Kryo kryo, EdgeTreePath original) {
            return (EdgeTreePath) original.copy();
        }
    }

}
