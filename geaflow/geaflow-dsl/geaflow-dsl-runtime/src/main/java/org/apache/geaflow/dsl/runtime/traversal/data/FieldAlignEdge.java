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
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.impl.types.DoubleEdge;
import org.apache.geaflow.dsl.common.data.impl.types.IntEdge;
import org.apache.geaflow.dsl.common.data.impl.types.LongEdge;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;

public class FieldAlignEdge implements RowEdge {

    private final RowEdge baseEdge;

    private final int[] fieldMapping;

    public FieldAlignEdge(RowEdge baseEdge, int[] fieldMapping) {
        this.baseEdge = baseEdge;
        this.fieldMapping = fieldMapping;
    }

    @Override
    public Object getField(int i, IType<?> type) {
        int mappingIndex = fieldMapping[i];
        if (mappingIndex < 0) {
            return null;
        }
        return baseEdge.getField(mappingIndex, type);
    }

    @Override
    public void setValue(Row value) {
        baseEdge.setValue(value);
    }

    @Override
    public RowEdge withDirection(EdgeDirection direction) {
        return new FieldAlignEdge(baseEdge.withDirection(direction), fieldMapping);
    }

    @Override
    public RowEdge identityReverse() {
        return new FieldAlignEdge(baseEdge.identityReverse(), fieldMapping);
    }

    @Override
    public String getLabel() {
        return baseEdge.getLabel();
    }

    @Override
    public void setLabel(String label) {
        baseEdge.setLabel(label);
    }

    @Override
    public Object getSrcId() {
        return baseEdge.getSrcId();
    }

    @Override
    public void setSrcId(Object srcId) {
        baseEdge.setSrcId(srcId);
    }

    @Override
    public Object getTargetId() {
        return baseEdge.getTargetId();
    }

    @Override
    public void setTargetId(Object targetId) {
        baseEdge.setTargetId(targetId);
    }

    @Override
    public EdgeDirection getDirect() {
        return baseEdge.getDirect();
    }

    @Override
    public void setDirect(EdgeDirection direction) {
        baseEdge.setDirect(direction);
    }

    @Override
    public Row getValue() {
        return baseEdge.getValue();
    }

    @Override
    public IEdge<Object, Row> withValue(Row value) {
        return new FieldAlignEdge((RowEdge) baseEdge.withValue(value), fieldMapping);
    }

    @Override
    public IEdge<Object, Row> reverse() {
        return new FieldAlignEdge((RowEdge) baseEdge.reverse(), fieldMapping);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RowEdge)) {
            return false;
        }
        RowEdge that = (RowEdge) o;
        return Objects.equals(getSrcId(), that.getSrcId()) && Objects.equals(getTargetId(),
            that.getTargetId()) && getDirect() == that.getDirect() && Objects.equals(getBinaryLabel(), that.getBinaryLabel());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(baseEdge);
        result = 31 * result + Arrays.hashCode(fieldMapping);
        return result;
    }

    @Override
    public BinaryString getBinaryLabel() {
        return baseEdge.getBinaryLabel();
    }

    @Override
    public void setBinaryLabel(BinaryString label) {
        baseEdge.setBinaryLabel(label);
    }

    @Override
    public String toString() {
        return getSrcId() + "#" + getTargetId() + "#" + getBinaryLabel() + "#" + getDirect() + "#" + getValue();
    }

    public static RowEdge createFieldAlignedEdge(RowEdge baseEdge, int[] fieldMapping) {
        if (baseEdge instanceof LongEdge) {
            return new FieldAlignLongEdge((LongEdge) baseEdge, fieldMapping);
        } else if (baseEdge instanceof IntEdge) {
            return new FieldAlignIntEdge((IntEdge) baseEdge, fieldMapping);
        } else if (baseEdge instanceof DoubleEdge) {
            return new FieldAlignDoubleEdge((DoubleEdge) baseEdge, fieldMapping);
        }
        return new FieldAlignEdge(baseEdge, fieldMapping);
    }

    public static class FieldAlignEdgeSerializer extends Serializer<FieldAlignEdge> {

        @Override
        public void write(Kryo kryo, Output output, FieldAlignEdge object) {
            kryo.writeClassAndObject(output, object.baseEdge);
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
        public FieldAlignEdge read(Kryo kryo, Input input, Class<FieldAlignEdge> type) {
            RowEdge baseEdge = (RowEdge) kryo.readClassAndObject(input);
            int[] fieldMapping = new int[input.readInt(true)];
            for (int i = 0; i < fieldMapping.length; i++) {
                fieldMapping[i] = input.readInt();
            }
            return new FieldAlignEdge(baseEdge, fieldMapping);
        }

        @Override
        public FieldAlignEdge copy(Kryo kryo, FieldAlignEdge original) {
            return new FieldAlignEdge(kryo.copy(original.baseEdge), Arrays.copyOf(original.fieldMapping, original.fieldMapping.length));
        }

    }

}
