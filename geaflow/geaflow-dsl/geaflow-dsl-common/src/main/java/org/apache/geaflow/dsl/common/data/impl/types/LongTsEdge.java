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

public class LongTsEdge extends LongEdge implements IGraphElementWithTimeField, KryoSerializable {

    public static final Supplier<LongTsEdge> CONSTRUCTOR = new Constructor();

    private long time;

    public LongTsEdge() {

    }

    public LongTsEdge(long srcId, long targetId) {
        super(srcId, targetId);
    }

    public LongTsEdge(long srcId, long targetId, Row value) {
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
        if (!(o instanceof LongTsEdge)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LongTsEdge that = (LongTsEdge) o;
        return time == that.time;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), time);
    }

    @Override
    public LongTsEdge reverse() {
        LongTsEdge edge = new LongTsEdge((long) getTargetId(), (long) getSrcId(), getValue());
        edge.setBinaryLabel(getBinaryLabel());
        edge.setDirect(getDirect());
        edge.setTime(time);
        return edge;
    }

    @Override
    public LongTsEdge withValue(Row value) {
        LongTsEdge edge = new LongTsEdge((long) getSrcId(), (long) getTargetId(), value);
        edge.setBinaryLabel(getBinaryLabel());
        edge.setDirect(getDirect());
        edge.setTime(time);
        return edge;
    }

    @Override
    public RowEdge withDirection(EdgeDirection direction) {
        LongTsEdge edge = new LongTsEdge((long) getSrcId(), (long) getTargetId(), getValue());
        edge.setBinaryLabel(getBinaryLabel());
        edge.setDirect(direction);
        edge.setTime(time);
        return edge;
    }

    @Override
    public RowEdge identityReverse() {
        LongTsEdge edge = new LongTsEdge((long) getTargetId(), (long) getSrcId(), getValue());
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

    private static class Constructor implements Supplier<LongTsEdge> {

        @Override
        public LongTsEdge get() {
            return new LongTsEdge();
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        // serialize srcId, targetId, direction, label and time
        output.writeLong((Long) this.getSrcId());
        output.writeLong((Long) this.getTargetId());
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
        long srcId = input.readLong();
        this.srcId = srcId;
        long targetId = input.readLong();
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
