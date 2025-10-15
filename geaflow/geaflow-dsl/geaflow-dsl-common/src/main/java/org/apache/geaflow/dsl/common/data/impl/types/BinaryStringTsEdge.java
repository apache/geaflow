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

public class BinaryStringTsEdge extends BinaryStringEdge implements IGraphElementWithTimeField,
    KryoSerializable {

    public static final Supplier<BinaryStringTsEdge> CONSTRUCTOR = new Constructor();

    private long time;

    public BinaryStringTsEdge() {

    }

    public BinaryStringTsEdge(BinaryString srcId, BinaryString targetId) {
        super(srcId, targetId);
    }

    public BinaryStringTsEdge(BinaryString srcId, BinaryString targetId, Row value) {
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
        if (!(o instanceof BinaryStringTsEdge)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        BinaryStringTsEdge that = (BinaryStringTsEdge) o;
        return time == that.time;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), time);
    }

    @Override
    public BinaryStringTsEdge reverse() {
        BinaryStringTsEdge edge = new BinaryStringTsEdge((BinaryString) getTargetId(),
            (BinaryString) getSrcId(), getValue());
        edge.setBinaryLabel(getBinaryLabel());
        edge.setDirect(getDirect());
        edge.setTime(time);
        return edge;
    }

    @Override
    public BinaryStringTsEdge withValue(Row value) {
        BinaryStringTsEdge edge = new BinaryStringTsEdge((BinaryString) getSrcId(),
            (BinaryString) getTargetId(), value);
        edge.setBinaryLabel(getBinaryLabel());
        edge.setDirect(getDirect());
        edge.setTime(time);
        return edge;
    }

    @Override
    public RowEdge withDirection(EdgeDirection direction) {
        BinaryStringTsEdge edge = new BinaryStringTsEdge((BinaryString) getSrcId(),
            (BinaryString) getTargetId(), getValue());
        edge.setBinaryLabel(getBinaryLabel());
        edge.setDirect(direction);
        edge.setTime(time);
        return edge;
    }

    @Override
    public RowEdge identityReverse() {
        BinaryStringTsEdge edge = new BinaryStringTsEdge((BinaryString) getTargetId(),
            (BinaryString) getSrcId(), getValue());
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

    private static class Constructor implements Supplier<BinaryStringTsEdge> {

        @Override
        public BinaryStringTsEdge get() {
            return new BinaryStringTsEdge();
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        // serialize fields
        byte[] srcIdBytes = ((BinaryString) this.getSrcId()).getBytes();
        byte[] targetIdBytes = ((BinaryString) this.getTargetId()).getBytes();
        output.writeInt(srcIdBytes.length);
        output.writeBytes(srcIdBytes);
        output.writeInt(targetIdBytes.length);
        output.writeBytes(targetIdBytes);
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
        // deserialize fields
        BinaryString srcId = BinaryString.fromBytes(input.readBytes(input.readInt()));
        BinaryString targetId = BinaryString.fromBytes(input.readBytes(input.readInt()));
        EdgeDirection direction = (EdgeDirection) kryo.readClassAndObject(input);
        BinaryString label = BinaryString.fromBytes(input.readBytes(input.readInt()));
        long time = input.readLong();
        // deserialize value
        Row value = (Row) kryo.readClassAndObject(input);
        // create edge object
        this.setSrcId(srcId);
        this.setTargetId(targetId);
        this.setValue(value);
        this.setBinaryLabel(label);
        this.setDirect(direction);
        this.setTime(time);
    }

}
