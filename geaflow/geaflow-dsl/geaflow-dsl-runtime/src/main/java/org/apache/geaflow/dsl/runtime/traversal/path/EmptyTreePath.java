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
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;

public class EmptyTreePath extends AbstractSingleTreePath implements KryoSerializable {

    public static final ITreePath INSTANCE = new EmptyTreePath();

    private EmptyTreePath() {

    }

    @SuppressWarnings("unchecked")
    public static EmptyTreePath of() {
        return (EmptyTreePath) INSTANCE;
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
        throw new IllegalArgumentException("Illegal call");
    }

    @Override
    public NodeType getNodeType() {
        return NodeType.EMPTY_TREE;
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
    public ITreePath merge(ITreePath other) {
        return other;
    }

    @Override
    public EdgeSet getEdgeSet() {
        throw new IllegalArgumentException("Illegal call");
    }

    @Override
    public ITreePath copy() {
        return new EmptyTreePath();
    }

    @Override
    public ITreePath copy(List<ITreePath> parents) {
        return copy();
    }

    @Override
    public ITreePath getTreePath(Object requestId) {
        return INSTANCE;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public ITreePath extendTo(Set<Object> requestIds, List<RowEdge> edges) {
        EdgeSet edgeSet = new DefaultEdgeSet(edges);
        return SourceEdgeTreePath.of(requestIds, edgeSet);
    }

    @Override
    public ITreePath extendTo(Set<Object> requestIds, RowVertex vertex) {
        return SourceVertexTreePath.of(requestIds, vertex);
    }

    @Override
    public List<Path> select(int... pathIndices) {
        return Collections.emptyList();
    }

    @Override
    public boolean walkTree(List<Object> pathNodes, WalkFunction walkFunction, int maxDepth, PathIdCounter pathId) {
        return false;
    }

    @Override
    protected ITreePath filter(PathFilterFunction filterFunction,
                               int[] refPathIndices, int[] fieldMapping,
                               Path currentPath, int maxDepth, PathIdCounter pathId) {
        return EmptyTreePath.of();
    }

    @Override
    public boolean equalNode(ITreePath other) {
        return other.getNodeType() == NodeType.EMPTY_TREE;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        // no fields to serialize
    }

    @Override
    public void read(Kryo kryo, Input input) {
        // no fields to deserialize
    }

}