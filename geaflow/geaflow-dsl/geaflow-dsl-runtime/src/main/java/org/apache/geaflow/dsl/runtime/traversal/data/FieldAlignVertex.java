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
import java.util.Arrays;
import java.util.Objects;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.types.DoubleVertex;
import org.apache.geaflow.dsl.common.data.impl.types.IntVertex;
import org.apache.geaflow.dsl.common.data.impl.types.LongVertex;
import org.apache.geaflow.model.graph.vertex.IVertex;

public class FieldAlignVertex implements RowVertex {

    private final RowVertex baseVertex;

    private final int[] fieldMapping;

    public FieldAlignVertex(RowVertex baseVertex, int[] fieldMapping) {
        this.baseVertex = baseVertex;
        this.fieldMapping = fieldMapping;
    }

    @Override
    public Object getField(int i, IType<?> type) {
        int mappingIndex = fieldMapping[i];
        if (mappingIndex < 0) {
            return null;
        }
        return baseVertex.getField(mappingIndex, type);
    }

    @Override
    public void setValue(Row value) {
        baseVertex.setValue(value);
    }

    @Override
    public String getLabel() {
        return baseVertex.getLabel();
    }

    @Override
    public void setLabel(String label) {
        baseVertex.setLabel(label);
    }

    @Override
    public Object getId() {
        return baseVertex.getId();
    }

    @Override
    public void setId(Object id) {
        baseVertex.setId(id);
    }

    @Override
    public Row getValue() {
        return baseVertex.getValue();
    }

    @Override
    public IVertex<Object, Row> withValue(Row value) {
        return new FieldAlignVertex((RowVertex) baseVertex.withValue(value), fieldMapping);
    }

    @Override
    public IVertex<Object, Row> withLabel(String label) {
        return new FieldAlignVertex((RowVertex) baseVertex.withLabel(label), fieldMapping);
    }

    @Override
    public IVertex<Object, Row> withTime(long time) {
        return new FieldAlignVertex((RowVertex) baseVertex.withTime(time), fieldMapping);
    }

    @Override
    public int compareTo(Object o) {
        return baseVertex.compareTo(o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RowVertex)) {
            return false;
        }
        RowVertex that = (RowVertex) o;
        return getId().equals(that.getId()) && Objects.equals(getBinaryLabel(), that.getBinaryLabel());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getBinaryLabel());
    }

    @Override
    public BinaryString getBinaryLabel() {
        return baseVertex.getBinaryLabel();
    }

    @Override
    public void setBinaryLabel(BinaryString label) {
        baseVertex.setBinaryLabel(label);
    }

    @Override
    public String toString() {
        return getId() + "#" + getBinaryLabel() + "#" + getValue();
    }

    public static RowVertex createFieldAlignedVertex(RowVertex baseVertex, int[] fieldMapping) {
        if (baseVertex instanceof LongVertex) {
            return new FieldAlignLongVertex((LongVertex) baseVertex, fieldMapping);
        } else if (baseVertex instanceof IntVertex) {
            return new FieldAlignIntVertex((IntVertex) baseVertex, fieldMapping);
        } else if (baseVertex instanceof DoubleVertex) {
            return new FieldAlignDoubleVertex((DoubleVertex) baseVertex, fieldMapping);
        }
        return new FieldAlignVertex(baseVertex, fieldMapping);
    }

    public static class FieldAlignVertexSerializer extends Serializer<FieldAlignVertex> {

        @Override
        public void write(Kryo kryo, Output output, FieldAlignVertex object) {
            kryo.writeClassAndObject(output, object.baseVertex);
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
        public FieldAlignVertex read(Kryo kryo, Input input, Class<FieldAlignVertex> type) {
            RowVertex baseVertex = (RowVertex) kryo.readClassAndObject(input);
            int[] fieldMapping = new int[input.readInt(true)];
            for (int i = 0; i < fieldMapping.length; i++) {
                fieldMapping[i] = input.readInt();
            }
            return new FieldAlignVertex(baseVertex, fieldMapping);
        }

        @Override
        public FieldAlignVertex copy(Kryo kryo, FieldAlignVertex original) {
            return new FieldAlignVertex(kryo.copy(original.baseVertex), Arrays.copyOf(original.fieldMapping, original.fieldMapping.length));
        }
    }

}
