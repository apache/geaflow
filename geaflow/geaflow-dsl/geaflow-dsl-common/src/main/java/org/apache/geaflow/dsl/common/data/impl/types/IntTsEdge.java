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
import org.apache.geaflow.model.graph.IGraphElementWithTimeField;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

public class IntTsEdge extends IntEdge implements IGraphElementWithTimeField, KryoSerializable {

    public static final Supplier<IntTsEdge> CONSTRUCTOR = new Constructor();

    private long time;

    public IntTsEdge() {

    }

    public IntTsEdge(int srcId, int targetId) {
        super(srcId, targetId);
    }

    public IntTsEdge(int srcId, int targetId, Row value) {
        super(srcId, targetId, value);
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getTime() {
        return time;
    }

    @Override
    public Object getField(int i, IType<?> type) {
        switch (i) {
            case EdgeType.SRC_ID_FIELD_POSITION:
                return getSrcId();
            case EdgeType.TARGET_ID_FIELD_POSITION:
                return getTargetId();
            case EdgeType.LABEL_FIELD_POSITION:
                return getBinaryLabel();
            case EdgeType.TIME_FIELD_POSITION:
                return time;
            default:
                return getValue().getField(i - 4, type);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IntTsEdge)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        IntTsEdge that = (IntTsEdge) o;
        return time == that.time;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), time);
    }

    @Override
    public IntTsEdge reverse() {
        IntTsEdge edge = new IntTsEdge((int) getTargetId(), (int) getSrcId(),
            getValue());
        edge.setBinaryLabel(getBinaryLabel());
        edge.setDirect(getDirect());
        edge.setTime(time);
        return edge;
    }

    @Override
    public IntTsEdge withValue(Row value) {
        IntTsEdge edge = new IntTsEdge((int) getSrcId(), (int) getTargetId(), value);
        edge.setBinaryLabel(getBinaryLabel());
        edge.setDirect(getDirect());
        edge.setTime(time);
        return edge;
    }

    @Override
    public RowEdge withDirection(EdgeDirection direction) {
        IntTsEdge edge = new IntTsEdge((int) getSrcId(), (int) getTargetId(), getValue());
        edge.setBinaryLabel(getBinaryLabel());
        edge.setDirect(direction);
        edge.setTime(time);
        return edge;
    }

    @Override
    public RowEdge identityReverse() {
        IntTsEdge edge = new IntTsEdge((int) getTargetId(), (int) getSrcId(),
            getValue());
        edge.setBinaryLabel(getBinaryLabel());
        edge.setDirect(getDirect().reverse());
        edge.setTime(time);
        return edge;
    }

    @Override
    public String toString() {
        return getSrcId() + "#" + getTargetId() + "#" + getBinaryLabel() + "#" + getDirect()
            + "#" + time + "#" + getValue();
    }

    private static class Constructor implements Supplier<IntTsEdge> {

        @Override
        public IntTsEdge get() {
            return new IntTsEdge();
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        // serialize srcId, targetId, direction, label and time
        output.writeInt((Integer) this.getSrcId());
        output.writeInt((Integer) this.getTargetId());
        kryo.writeClassAndObject(output, this.getDirect());
        byte[] labelBytes = this.getBinaryLabel().getBytes();
        output.writeInt(labelBytes.length);
        output.writeBytes(labelBytes);
        output.writeLong(this.getTime());
        // serialize value
        kryo.writeClassAndObject(output, this.getValue());
    }

    @Override
    public void read(Kryo kryo, Input input) {
        // deserialize srcId, targetId, direction, label and time
        int srcId = input.readInt();
        this.srcId = srcId;
        int targetId = input.readInt();
        this.targetId = targetId;
        EdgeDirection direction = (EdgeDirection) kryo.readClassAndObject(input);
        this.setDirect(direction);
        int labelLength = input.readInt();
        BinaryString label = BinaryString.fromBytes(input.readBytes(labelLength));
        this.setBinaryLabel(label);
        long time = input.readLong();
        this.setTime(time);
        // deserialize value
        Row value = (Row) kryo.readClassAndObject(input);
        this.setValue(value);
    }

}
