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

public class LongEdge implements RowEdge, KryoSerializable {

    public static final Supplier<LongEdge> CONSTRUCTOR = new Constructor();

    public long srcId;

    public long targetId;

    public EdgeDirection direction = EdgeDirection.OUT;

    private BinaryString label;

    private Row value;

    public LongEdge() {

    }

    public LongEdge(long srcId, long targetId) {
        this.srcId = srcId;
        this.targetId = targetId;
    }

    public LongEdge(long srcId, long targetId, Row value) {
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
        this.srcId = (long) srcId;
    }

    @Override
    public Object getTargetId() {
        return targetId;
    }

    @Override
    public void setTargetId(Object targetId) {
        this.targetId = (long) targetId;
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
        if (!(o instanceof RowEdge)) {
            return false;
        }
        if (o instanceof LongEdge) {
            LongEdge that = (LongEdge) o;
            return srcId == that.srcId && targetId == that.targetId
                && direction == that.direction && Objects.equals(label, that.label);
        } else {
            RowEdge that = (RowEdge) o;
            return that.equals(this);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcId, targetId, direction, label);
    }

    @Override
    public LongEdge reverse() {
        LongEdge edge = new LongEdge(targetId, srcId, value);
        edge.setBinaryLabel(label);
        edge.setDirect(direction);
        return edge;
    }

    @Override
    public Row getValue() {
        return value;
    }

    @Override
    public LongEdge withValue(Row value) {
        LongEdge edge = new LongEdge(srcId, targetId, value);
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
        LongEdge edge = new LongEdge(srcId, targetId, value);
        edge.setBinaryLabel(label);
        edge.setDirect(direction);
        return edge;
    }

    @Override
    public RowEdge identityReverse() {
        LongEdge edge = new LongEdge(targetId, srcId, value);
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

    private static class Constructor implements Supplier<LongEdge> {

        @Override
        public LongEdge get() {
            return new LongEdge();
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        // serialize srcId, targetId, direction, label and value
        output.writeLong(this.srcId);
        output.writeLong(this.targetId);
        kryo.writeClassAndObject(output, this.getDirect());
        byte[] labelBytes = this.getBinaryLabel().getBytes();
        output.writeInt(labelBytes.length);
        output.writeBytes(labelBytes);
        kryo.writeClassAndObject(output, this.getValue());
    }

    @Override
    public void read(Kryo kryo, Input input) {
        // deserialize srcId, targetId, direction, label and value
        long srcId = input.readLong();
        this.srcId = srcId;
        long targetId = input.readLong();
        this.targetId = targetId;
        EdgeDirection direction = (EdgeDirection) kryo.readClassAndObject(input);
        this.setDirect(direction);
        int labelLength = input.readInt();
        BinaryString label = BinaryString.fromBytes(input.readBytes(labelLength));
        this.setBinaryLabel(label);
        Row value = (Row) kryo.readClassAndObject(input);
        this.setValue(value);
    }

}
