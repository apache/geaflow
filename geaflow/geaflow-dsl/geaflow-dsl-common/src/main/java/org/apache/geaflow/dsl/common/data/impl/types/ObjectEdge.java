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

package org.apache.geaflow.dsl.common.data.impl.types;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.util.BinaryUtil;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

public class ObjectEdge implements RowEdge, KryoSerializable {

    public static final Supplier<ObjectEdge> CONSTRUCTOR = new Constructor();

    private Object srcId;

    private Object targetId;

    private EdgeDirection direction = EdgeDirection.OUT;

    private BinaryString label;

    private Row value;

    public ObjectEdge() {

    }

    public ObjectEdge(Object srcId, Object targetId) {
        this.srcId = srcId;
        this.targetId = targetId;
    }

    public ObjectEdge(Object srcId, Object targetId, Row value) {
        this.srcId = srcId;
        this.targetId = targetId;
        this.value = value;
    }

    @Override
    public void setLabel(String label) {
        this.label = (BinaryString) BinaryUtil.toBinaryLabel(label);
    }

    @Override
    public Object getSrcId() {
        return srcId;
    }

    @Override
    public void setSrcId(Object srcId) {
        this.srcId = srcId;
    }

    @Override
    public Object getTargetId() {
        return targetId;
    }

    @Override
    public void setTargetId(Object targetId) {
        this.targetId = targetId;
    }

    @Override
    public String getLabel() {
        return label.toString();
    }

    @Override
    public EdgeDirection getDirect() {
        return direction;
    }

    @Override
    public void setDirect(EdgeDirection direction) {
        this.direction = direction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ObjectEdge)) {
            return false;
        }
        ObjectEdge that = (ObjectEdge) o;
        return Objects.equals(srcId, that.srcId) && Objects.equals(targetId,
            that.targetId) && direction == that.direction && Objects.equals(label, that.label);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcId, targetId, direction, label);
    }

    @Override
    public ObjectEdge reverse() {
        ObjectEdge edge = new ObjectEdge(targetId, srcId, value);
        edge.setBinaryLabel(label);
        edge.setDirect(direction);
        return edge;
    }

    @Override
    public Row getValue() {
        return value;
    }

    @Override
    public ObjectEdge withValue(Row value) {
        ObjectEdge edge = new ObjectEdge(srcId, targetId, value);
        edge.setBinaryLabel(label);
        edge.setDirect(direction);
        return edge;
    }

    @Override
    public String toString() {
        return srcId + "#" + targetId + "#" + label + "#" + direction + "#" + value;
    }

    @Override
    public Object getField(int i, IType<?> type) {
        switch (i) {
            case EdgeType.SRC_ID_FIELD_POSITION:
                return srcId;
            case EdgeType.TARGET_ID_FIELD_POSITION:
                return targetId;
            case EdgeType.LABEL_FIELD_POSITION:
                return label;
            default:
                return value.getField(i - 3, type);
        }
    }

    @Override
    public void setValue(Row value) {
        this.value = value;
    }

    @Override
    public RowEdge withDirection(EdgeDirection direction) {
        ObjectEdge edge = new ObjectEdge(srcId, targetId, value);
        edge.setBinaryLabel(label);
        edge.setDirect(direction);
        return edge;
    }

    @Override
    public RowEdge identityReverse() {
        ObjectEdge edge = new ObjectEdge(targetId, srcId, value);
        edge.setBinaryLabel(label);
        edge.setDirect(direction.reverse());
        return edge;
    }

    @Override
    public BinaryString getBinaryLabel() {
        return label;
    }

    @Override
    public void setBinaryLabel(BinaryString label) {
        this.label = label;
    }

    private static class Constructor implements Supplier<ObjectEdge> {

        @Override
        public ObjectEdge get() {
            return new ObjectEdge();
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        // serialize srcId, targetId, direction, label and value
        kryo.writeClassAndObject(output, this.getSrcId());
        kryo.writeClassAndObject(output, this.getTargetId());
        kryo.writeClassAndObject(output, this.getDirect());
        kryo.writeClassAndObject(output, this.getBinaryLabel());
        kryo.writeClassAndObject(output, this.getValue());
    }

    @Override
    public void read(Kryo kryo, Input input) {
        // deserialize srcId, targetId, direction, label and value
        Object srcId = kryo.readClassAndObject(input);
        this.srcId = srcId;
        Object targetId = kryo.readClassAndObject(input);
        this.targetId = targetId;
        EdgeDirection direction = (EdgeDirection) kryo.readClassAndObject(input);
        this.setDirect(direction);
        BinaryString label = (BinaryString) kryo.readClassAndObject(input);
        this.setBinaryLabel(label);
        Row value = (Row) kryo.readClassAndObject(input);
        this.setValue(value);
    }

}
