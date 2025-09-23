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

package com.antgroup.geaflow.dsl.runtime.traversal.path;

import com.antgroup.geaflow.dsl.common.data.Path;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class UnionTreePath extends AbstractTreePath {

    private final List<ITreePath> nodes;

    private static final Logger LOGGER = LoggerFactory.getLogger(UnionTreePath.class);

    private UnionTreePath(List<ITreePath> nodes) {
        this.nodes = Objects.requireNonNull(nodes);
    }

    public static ITreePath create(List<ITreePath> nodes) {
        List<ITreePath> notEmptyTreePath =
            Objects.requireNonNull(nodes).stream().filter(n -> n.getNodeType() != NodeType.EMPTY_TREE).collect(Collectors.toList());
        if (notEmptyTreePath.isEmpty()) {
            return EmptyTreePath.of();
        } else if (notEmptyTreePath.size() == 1) {
            return notEmptyTreePath.get(0);
        } else {
            return new UnionTreePath(notEmptyTreePath);
        }
    }

    private UnionTreePath() {
        this(new ArrayList<>());
    }

    @Override
    public RowVertex getVertex() {
        throw new IllegalArgumentException("Illegal call");
    }

    @Override
    public void setVertex(RowVertex vertex) {
        for (ITreePath node : nodes) {
            node.setVertex(vertex);
        }
    }

    @Override
    public Object getVertexId() {
        throw new IllegalArgumentException("Illegal call");
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.UNION_TREE;
    }

    @Override
    public List<ITreePath> getParents() {
        throw new IllegalArgumentException("Illegal call");
    }

    @Override
    public int getDepth() {
        if (nodes.isEmpty()) {
            return 0;
        }
        return nodes.get(0).getDepth();
    }

    @Override
    public ITreePath merge(ITreePath other) {
        if (other instanceof UnionTreePath) {
            UnionTreePath unionTreePath = (UnionTreePath) other;
            for (ITreePath otherNode : unionTreePath.nodes) {
                addNode(otherNode, false);
            }
        } else {
            addNode(other, false);
        }
        return this;
    }

    private void addNode(ITreePath node, boolean mergeEdgeSet) {
        ITreePath existNode = null;
        for (ITreePath thisNode : nodes) {
            if (thisNode.equalNode(node) && thisNode.getDepth() == node.getDepth()) {
                existNode = thisNode;
                break;
            }
        }
        if (existNode != null) {
            ITreePath merged = existNode.merge(node);
            assert merged == existNode;
        } else {
            boolean hasMerged = false;
            if (mergeEdgeSet) {
                // merge edge with same source and target id.
                if (node.getNodeType() == NodeType.EDGE_TREE) {
                    for (ITreePath thisNode : nodes) {
                        if (thisNode.getNodeType() == NodeType.EDGE_TREE
                            && thisNode.getEdgeSet().like(node.getEdgeSet())
                            && Objects.equals(thisNode.getParents(), node.getParents())) {
                            thisNode.getEdgeSet().addEdges(node.getEdgeSet());
                            hasMerged = true;
                            break;
                        }
                    }
                }
            }
            if (!hasMerged) {
                nodes.add(node);
            }
        }
    }

    @Override
    public ITreePath optimize() {
        if (nodes.isEmpty()) {
            return this;
        }
        UnionTreePath unionTreePath = new UnionTreePath();

        for (int i = 0; i < nodes.size(); i++) {
            ITreePath node = nodes.get(i);
            if (i == 0) {
                node = node.copy();
            }
            unionTreePath.addNode(node, true);
        }
        if (unionTreePath.nodes.size() == 1) {
            return unionTreePath.nodes.get(0);
        } else if (unionTreePath.nodes.size() == 0) {
            return EmptyTreePath.of();
        }
        return unionTreePath;
    }

    @Override
    public void setRequestId(Object requestId) {
        for (ITreePath node : nodes) {
            node.setRequestId(requestId);
        }
    }

    @Override
    public void setRequestIdForTree(Object requestId) {
        for (ITreePath node : nodes) {
            node.setRequestIdForTree(requestId);
        }
    }

    @Override
    public EdgeSet getEdgeSet() {
        throw new IllegalArgumentException("Illegal call");
    }

    @Override
    public ITreePath copy() {
        List<ITreePath> copyNodes = new ArrayList<>(nodes.size());
        for (ITreePath node : nodes) {
            copyNodes.add(node.copy());
        }
        return new UnionTreePath(copyNodes);
    }

    @Override
    public ITreePath copy(List<ITreePath> parents) {
        throw new IllegalArgumentException("Illegal call");
    }

    @Override
    public ITreePath getTreePath(Object sessionId) {
        List<ITreePath> sessionNodes = new ArrayList<>(nodes.size());
        for (ITreePath node : nodes) {
            ITreePath nodeOnSession = node.getTreePath(sessionId);
            if (nodeOnSession != null) {
                sessionNodes.add(nodeOnSession.getTreePath(sessionId));
            }
        }
        return UnionTreePath.create(sessionNodes);
    }

    @Override
    public Set<Object> getRequestIds() {
        throw new IllegalArgumentException("Illegal call");
    }

    @Override
    public void addRequestIds(Collection<Object> requestIds) {
        for (ITreePath node : nodes) {
            node.addRequestIds(requestIds);
        }
    }

    @Override
    public int size() {
        int size = 0;
        for (ITreePath node : nodes) {
            size += node.size();
        }
        return size;
    }

    @Override
    public ITreePath extendTo(Set<Object> requestIds, List<RowEdge> edges) {
        LOGGER.info("[{}@{}] Edge is: {}; ", this.getClass().getSimpleName(), System.identityHashCode(this), edges);
        EdgeSet edgeSet = new DefaultEdgeSet(edges);
        ITreePath newTreePath = EdgeTreePath.of(requestIds, edgeSet);
        for (ITreePath parent : nodes) {
            newTreePath.addParent(parent);
        }
        return newTreePath;
    }

    @Override
    public ITreePath extendTo(Set<Object> requestIds, RowVertex vertex) {
        LOGGER.info("[{}@{}] Vertex is: {}; ", this.getClass().getSimpleName(),System.identityHashCode(this), vertex);
        ITreePath newTreePath = VertexTreePath.of(requestIds, vertex);
        for (ITreePath parent : nodes) {
            newTreePath.addParent(parent);
        }
        return newTreePath;
    }

    @Override
    public List<Path> select(int... pathIndices) {
        Set<Path> selectPaths = new HashSet<>();
        for (ITreePath node : nodes) {
            selectPaths.addAll(node.select(pathIndices));
        }
        return Lists.newArrayList(selectPaths);
    }

    @Override
    public boolean walkTree(List<Object> pathNodes, WalkFunction walkFunction, int maxDepth, PathIdCounter pathId) {
        for (ITreePath node : nodes) {
            if (!node.walkTree(pathNodes, walkFunction, maxDepth, pathId)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected ITreePath filter(PathFilterFunction filterFunction,
                               int[] refPathIndices, int[] fieldMapping,
                               Path currentPath, int maxDepth, PathIdCounter pathId) {
        List<ITreePath> filterNodes = new ArrayList<>();
        for (ITreePath node : nodes) {
            ITreePath filterNode = ((AbstractTreePath) node).filter(filterFunction,
                refPathIndices, fieldMapping, currentPath, maxDepth, pathId);
            if (filterNode != null) {
                filterNodes.add(filterNode);
            }
        }
        return UnionTreePath.create(filterNodes);
    }

    @Override
    public boolean equalNode(ITreePath other) {
        if (other.getNodeType() == NodeType.UNION_TREE) {
            UnionTreePath unionTreePath = (UnionTreePath) other;
            if (nodes.size() != unionTreePath.nodes.size()) {
                return false;
            }
            for (int i = 0; i < nodes.size(); i++) {
                if (!nodes.get(i).equalNode(unionTreePath.nodes.get(i))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof UnionTreePath)) {
            return false;
        }
        UnionTreePath that = (UnionTreePath) o;
        return Objects.equals(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodes);
    }

    public List<ITreePath> getNodes() {
        return nodes;
    }

    public List<ITreePath> expand() {
        List<ITreePath> paths = new ArrayList<>();
        for (ITreePath node : nodes) {
            if (node instanceof UnionTreePath) {
                paths.addAll(((UnionTreePath) node).expand());
            } else {
                paths.add(node);
            }
        }
        return paths;
    }

    public static class UnionTreePathSerializer extends Serializer<UnionTreePath> {

        @Override
        public void write(Kryo kryo, Output output, UnionTreePath object) {
            kryo.writeClassAndObject(output, object.getNodes());
        }

        @Override
        public UnionTreePath read(Kryo kryo, Input input, Class<UnionTreePath> type) {
            List<ITreePath> nodes = (List<ITreePath>) kryo.readClassAndObject(input);
            return new UnionTreePath(nodes);
        }

        @Override
        public UnionTreePath copy(Kryo kryo, UnionTreePath original) {
            return (UnionTreePath) original.copy();
        }
    }

}
