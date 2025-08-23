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

package org.apache.geaflow.dsl.common.data.impl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.Collection;
import java.util.List;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.Row;

public class DefaultParameterizedPath implements ParameterizedPath {

    private final Path basePath;

    private final Object requestId;

    private final Row parameterRow;

    private final Row systemVariableRow;

    public DefaultParameterizedPath(Path basePath, Object requestId, Row parameterRow,
                                    Row systemVariableRow) {
        this.basePath = basePath;
        this.requestId = requestId;
        this.parameterRow = parameterRow;
        this.systemVariableRow = systemVariableRow;
    }

    public DefaultParameterizedPath(Path basePath, Object requestId, Row parameterRow) {
        this(basePath, requestId, parameterRow, null);
    }

    @Override
    public Row getParameter() {
        return parameterRow;
    }

    @Override
    public Row getSystemVariables() {
        return systemVariableRow;
    }

    @Override
    public Object getRequestId() {
        return requestId;
    }

    @Override
    public Row getField(int i, IType<?> type) {
        return basePath.getField(i, type);
    }

    @Override
    public List<Row> getPathNodes() {
        return basePath.getPathNodes();
    }

    @Override
    public void addNode(Row node) {
        basePath.addNode(node);
    }

    @Override
    public void remove(int index) {
        basePath.remove(index);
    }

    @Override
    public Path copy() {
        return new DefaultParameterizedPath(basePath.copy(), requestId,
            parameterRow, systemVariableRow);
    }

    @Override
    public int size() {
        return basePath.size();
    }

    @Override
    public Path subPath(Collection<Integer> indices) {
        return new DefaultParameterizedPath(basePath.subPath(indices), requestId,
            parameterRow, systemVariableRow);
    }

    @Override
    public Path subPath(int[] indices) {
        return new DefaultParameterizedPath(basePath.subPath(indices), requestId,
            parameterRow, systemVariableRow);
    }

    @Override
    public long getId() {
        return basePath.getId();
    }

    @Override
    public void setId(long id) {
        basePath.setId(id);
    }

    public static class DefaultParameterizedPathSerializer extends Serializer<DefaultParameterizedPath> {

        @Override
        public void write(Kryo kryo, Output output, DefaultParameterizedPath object) {
            kryo.writeClassAndObject(output, object.basePath);
            kryo.writeClassAndObject(output, object.getRequestId());
            kryo.writeClassAndObject(output, object.getParameter());
            kryo.writeClassAndObject(output, object.getSystemVariables());
        }

        @Override
        public DefaultParameterizedPath read(Kryo kryo, Input input, Class<DefaultParameterizedPath> aClass) {
            Path basePath = (Path) kryo.readClassAndObject(input);
            Object requestId = kryo.readClassAndObject(input);
            Row parameterRow = (Row) kryo.readClassAndObject(input);
            Row systemVariableRow = (Row) kryo.readClassAndObject(input);
            return new DefaultParameterizedPath(basePath, requestId, parameterRow, systemVariableRow);
        }

        @Override
        public DefaultParameterizedPath copy(Kryo kryo, DefaultParameterizedPath original) {
            Path basePath = kryo.copy(original.basePath);
            Object requestId = kryo.copy(original.getRequestId());
            Row parameterRow = kryo.copy(original.getParameter());
            Row systemVariableRow = kryo.copy(original.getSystemVariables());
            return new DefaultParameterizedPath(basePath, requestId, parameterRow, systemVariableRow);
        }
    }

}
