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
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.runtime.traversal.message.IMessage;
import org.apache.geaflow.dsl.runtime.traversal.message.MessageType;

public class ParameterizedTreePath extends AbstractTreePath {

    private final ITreePath baseTreePath;

    private final Object requestId;

    private final Row parameter;

    public ParameterizedTreePath(ITreePath baseTreePath, Object requestId, Row parameter) {
        this.baseTreePath = baseTreePath;
        this.requestId = requestId;
        this.parameter = parameter;
    }

    @Override
    public MessageType getType() {
        return baseTreePath.getType();
    }

    @Override
    public IMessage combine(IMessage other) {
        throw new IllegalArgumentException("read only tree path, combine is not support");
    }

    @Override
    public ITreePath getMessageByRequestId(Object requestId) {
        return (ITreePath) baseTreePath.getMessageByRequestId(requestId);
    }

    @Override
    public RowVertex getVertex() {
        return baseTreePath.getVertex();
    }

    @Override
    public void setVertex(RowVertex vertex) {
        throw new IllegalArgumentException("read only tree path, setVertex is not support");
    }

    @Override
    public Object getVertexId() {
        return baseTreePath.getVertexId();
    }

    @Override
    public NodeType getNodeType() {
        return baseTreePath.getNodeType();
    }

    @Override
    public List<ITreePath> getParents() {
        return baseTreePath.getParents();
    }

    @Override
    public void addParent(ITreePath parent) {
        throw new IllegalArgumentException("read only tree path, addParent is not support");
    }

    @Override
    public ITreePath merge(ITreePath other) {
        throw new IllegalArgumentException("read only tree path, merge is not support");
    }

    @Override
    public EdgeSet getEdgeSet() {
        return baseTreePath.getEdgeSet();
    }

    @Override
    public ITreePath copy() {
        return new ParameterizedTreePath(baseTreePath.copy(), requestId, parameter);
    }

    @Override
    public ITreePath copy(List<ITreePath> parents) {
        return new ParameterizedTreePath(baseTreePath.copy(parents), requestId, parameter);
    }

    @Override
    public ITreePath getTreePath(Object requestId) {
        return new ParameterizedTreePath(baseTreePath.getTreePath(requestId), requestId, parameter);
    }

    @Override
    public Set<Object> getRequestIds() {
        return baseTreePath.getRequestIds();
    }

    @Override
    public void addRequestIds(Collection<Object> requestIds) {
        throw new IllegalArgumentException("read only tree path, addRequestIds is not support");
    }

    @Override
    public boolean isEmpty() {
        return baseTreePath.isEmpty();
    }

    @Override
    public int size() {
        return baseTreePath.size();
    }

    @Override
    public ITreePath limit(int n) {
        return new ParameterizedTreePath(baseTreePath.limit(n), requestId, parameter);
    }

    @Override
    public ITreePath filter(PathFilterFunction filterFunction, int[] refPathIndices) {
        return new ParameterizedTreePath(baseTreePath.filter(filterFunction, refPathIndices), requestId, parameter);
    }

    @Override
    public ITreePath mapTree(PathMapFunction<Path> mapFunction) {
        return new ParameterizedTreePath(baseTreePath.mapTree(mapFunction), requestId, parameter);
    }

    @Override
    public List<Path> toList() {
        return baseTreePath.toList();
    }

    @Override
    public ITreePath extendTo(Set<Object> requestIds, List<RowEdge> edges) {
        throw new IllegalArgumentException("read only tree path, extendTo is not support");
    }

    @Override
    public ITreePath extendTo(Set<Object> requestIds, RowVertex vertex) {
        throw new IllegalArgumentException("read only tree path, extendTo is not support");
    }

    @Override
    public List<Path> select(int... pathIndices) {
        return baseTreePath.select(pathIndices);
    }

    @Override
    public ITreePath subPath(int... labelIndices) {
        return new ParameterizedTreePath(baseTreePath.subPath(labelIndices), requestId, parameter);
    }

    @Override
    public int getDepth() {
        return baseTreePath.getDepth();
    }

    @Override
    public boolean walkTree(List<Object> pathNodes, WalkFunction walkFunction, int maxDepth, PathIdCounter pathId) {
        return baseTreePath.walkTree(pathNodes, walkFunction, maxDepth, pathId);
    }

    @Override
    public boolean equalNode(ITreePath other) {
        return baseTreePath.equalNode(other);
    }

    @Override
    public ITreePath optimize() {
        throw new IllegalArgumentException("read only tree path, optimize is not support");
    }

    @Override
    public void setRequestIdForTree(Object requestId) {
        throw new IllegalArgumentException("read only tree path, setRequestIdForTree is not support");
    }

    @Override
    public void setRequestId(Object requestId) {
        throw new IllegalArgumentException("read only tree path, setRequestId is not support");
    }

    public Object getRequestId() {
        return requestId;
    }

    public Row getParameter() {
        return parameter;
    }

    public static class ParameterizedTreePathSerializer extends Serializer<ParameterizedTreePath> {

        @Override
        public void write(Kryo kryo, Output output, ParameterizedTreePath object) {
            kryo.writeClassAndObject(output, object.baseTreePath);
            kryo.writeClassAndObject(output, object.getRequestId());
            kryo.writeClassAndObject(output, object.getParameter());
        }

        @Override
        public ParameterizedTreePath read(Kryo kryo, Input input, Class<ParameterizedTreePath> type) {
            ITreePath baseTreePath = (ITreePath) kryo.readClassAndObject(input);
            Object requestId = kryo.readClassAndObject(input);
            Row parameter = (Row) kryo.readClassAndObject(input);
            return new ParameterizedTreePath(baseTreePath, requestId, parameter);
        }

        @Override
        public ParameterizedTreePath copy(Kryo kryo, ParameterizedTreePath original) {
            return (ParameterizedTreePath) original.copy();
        }
    }

}
