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

package org.apache.geaflow.dsl.runtime.traversal.data;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.utils.ArrayUtil;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.DefaultPath;

public class FieldAlignPath implements Path {

    private final Path basePath;

    private final int[] fieldMapping;

    public FieldAlignPath(Path basePath, int[] fieldMapping) {
        this.basePath = basePath;
        this.fieldMapping = fieldMapping;
    }

    @Override
    public void addNode(Row node) {
        throw new IllegalArgumentException("Read only path, addNode() method is not supported");
    }

    @Override
    public void remove(int index) {
        throw new IllegalArgumentException("Read only path, remove() method is not supported");
    }

    @Override
    public Path copy() {
        return new FieldAlignPath(basePath.copy(), fieldMapping);
    }

    @Override
    public int size() {
        return fieldMapping.length;
    }

    @Override
    public Path subPath(Collection<Integer> indices) {
        return subPath(ArrayUtil.toIntArray(indices));
    }

    @Override
    public Path subPath(int[] indices) {
        Path subPath = new DefaultPath();
        for (int index : indices) {
            subPath.addNode(getField(index, null));
        }
        return subPath;
    }

    @Override
    public long getId() {
        return basePath.getId();
    }

    @Override
    public void setId(long id) {
        basePath.setId(id);
    }

    @Override
    public Row getField(int i, IType<?> type) {
        int mappingIndex = fieldMapping[i];
        if (mappingIndex < 0) {
            return null;
        }
        return basePath.getField(mappingIndex, type);
    }

    @Override
    public List<Row> getPathNodes() {
        List<Row> pathNodes = new ArrayList<>(fieldMapping.length);
        for (int i = 0; i < fieldMapping.length; i++) {
            pathNodes.add(getField(i, null));
        }
        return pathNodes;
    }

    public static class FieldAlignPathSerializer extends Serializer<FieldAlignPath> {

        @Override
        public void write(Kryo kryo, Output output, FieldAlignPath object) {
            kryo.writeClassAndObject(output, object.basePath);
            if (object.fieldMapping != null) {
                output.writeInt(object.fieldMapping.length, true);
                for (int i : object.fieldMapping) {
                    output.writeInt(i);
                }
            } else {
                output.writeInt(0, true);
            }
        }

        @Override
        public FieldAlignPath read(Kryo kryo, Input input, Class<FieldAlignPath> type) {
            Path basePath = (Path) kryo.readClassAndObject(input);
            int[] fieldMapping = new int[input.readInt(true)];
            for (int i = 0; i < fieldMapping.length; i++) {
                fieldMapping[i] = input.readInt();
            }
            return new FieldAlignPath(basePath, fieldMapping);
        }

        @Override
        public FieldAlignPath copy(Kryo kryo, FieldAlignPath original) {
            return (FieldAlignPath) original.copy();
        }
    }

}
