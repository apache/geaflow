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
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.Row;

public class DefaultPath implements Path {

    private long id;

    private final List<Row> pathNodes;

    public DefaultPath(List<Row> pathNodes, long id) {
        this.pathNodes = Objects.requireNonNull(pathNodes);
        this.id = id;
    }

    public DefaultPath(List<Row> pathNodes) {
        this(pathNodes, -1L);
    }

    public DefaultPath(Row[] pathNodes) {
        this(Lists.newArrayList(pathNodes));
    }

    public DefaultPath() {
        this(new ArrayList<>());
    }

    @Override
    public Row getField(int i, IType<?> type) {
        return pathNodes.get(i);
    }

    @Override
    public List<Row> getPathNodes() {
        return pathNodes;
    }

    @Override
    public void addNode(Row node) {
        pathNodes.add(node);
    }

    @Override
    public void remove(int index) {
        pathNodes.remove(index);
    }

    @Override
    public Path copy() {
        return new DefaultPath(Lists.newArrayList(pathNodes), id);
    }

    @Override
    public int size() {
        return pathNodes.size();
    }

    @Override
    public Path subPath(Collection<Integer> indices) {
        List<Integer> indexList = new ArrayList<>(indices);
        Collections.sort(indexList);

        Path subPath = new DefaultPath();
        for (Integer index : indexList) {
            subPath.addNode(pathNodes.get(index));
        }
        return subPath;
    }

    @Override
    public Path subPath(int[] indices) {
        Path subPath = new DefaultPath();
        for (Integer index : indices) {
            subPath.addNode(pathNodes.get(index));
        }
        return subPath;
    }

    @Override
    public String toString() {
        return "DefaultPath{"
            + "pathNodes=" + pathNodes
            + '}';
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public void setId(long id) {
        this.id = id;
    }

    public static class DefaultPathSerializer extends Serializer<DefaultPath> {

        @Override
        public void write(Kryo kryo, Output output, DefaultPath defaultPath) {
            output.writeInt(defaultPath.getPathNodes().size());
            for (Row pathNode : defaultPath.getPathNodes()) {
                kryo.writeClassAndObject(output, pathNode);
            }
        }

        @Override
        public DefaultPath read(Kryo kryo, Input input, Class<DefaultPath> aClass) {
            int size = input.readInt();
            List<Row> pathNodes = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                pathNodes.add((Row) kryo.readClassAndObject(input));
            }
            return new DefaultPath(pathNodes);
        }

        @Override
        public DefaultPath copy(Kryo kryo, DefaultPath original) {
            List<Row> pathNodes = new ArrayList<>(original.getPathNodes().size());
            for (Row pathNode : original.getPathNodes()) {
                pathNodes.add(kryo.copy(pathNode));
            }
            return new DefaultPath(pathNodes);
        }
    }
}
