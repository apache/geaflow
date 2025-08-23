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

public class BinaryStringEdge implements RowEdge, KryoSerializable {

    public static final Supplier<BinaryStringEdge> CONSTRUCTOR = new Constructor();

    private BinaryString srcId;

    private BinaryString targetId;

    private EdgeDirection direction = EdgeDirection.OUT;

    private BinaryString label;

    private Row value;

    public BinaryStringEdge() {

    }

    public BinaryStringEdge(BinaryString srcId, BinaryString targetId) {
        this.srcId = srcId;
        this.targetId = targetId;
    }

    public BinaryStringEdge(BinaryString srcId, BinaryString targetId, Row value) {
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
        this.srcId = (BinaryString) srcId;
    }

    @Override
    public Object getTargetId() {
        return targetId;
    }

    @Override
    public void setTargetId(Object targetId) {
        this.targetId = (BinaryString) targetId;
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
        if (!(o instanceof BinaryStringEdge)) {
            return false;
        }
        BinaryStringEdge that = (BinaryStringEdge) o;
        return Objects.equals(srcId, that.srcId) && Objects.equals(targetId,
            that.targetId) && direction == that.direction && Objects.equals(label, that.label);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcId, targetId, direction, label);
    }

    @Override
    public BinaryStringEdge reverse() {
        BinaryStringEdge edge = new BinaryStringEdge(targetId, srcId, value);
        edge.setBinaryLabel(label);
        edge.setDirect(direction);
        return edge;
    }

    @Override
    public Row getValue() {
        return value;
    }

    @Override
    public BinaryStringEdge withValue(Row value) {
        BinaryStringEdge edge = new BinaryStringEdge(srcId, targetId, value);
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
        BinaryStringEdge edge = new BinaryStringEdge(srcId, targetId, value);
        edge.setBinaryLabel(label);
        edge.setDirect(direction);
        return edge;
    }

    @Override
    public RowEdge identityReverse() {
        BinaryStringEdge edge = new BinaryStringEdge(targetId, srcId, value);
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

    private static class Constructor implements Supplier<BinaryStringEdge> {

        @Override
        public BinaryStringEdge get() {
            return new BinaryStringEdge();
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        byte[] srcIdBytes = this.srcId.getBytes();
        output.writeInt(srcIdBytes.length);
        output.writeBytes(srcIdBytes);
        byte[] targetIdBytes = this.targetId.getBytes();
        output.writeInt(targetIdBytes.length);
        output.writeBytes(targetIdBytes);
        kryo.writeClassAndObject(output, this.getDirect());
        byte[] labelBytes = this.getBinaryLabel().getBytes();
        output.writeInt(labelBytes.length);
        output.writeBytes(labelBytes);
        kryo.writeClassAndObject(output, this.getValue());
    }

    @Override
    public void read(Kryo kryo, Input input) {
        BinaryString srcId = BinaryString.fromBytes(input.readBytes(input.readInt()));
        this.srcId = srcId;
        BinaryString targetId = BinaryString.fromBytes(input.readBytes(input.readInt()));
        this.targetId = targetId;
        EdgeDirection direction = (EdgeDirection) kryo.readClassAndObject(input);
        this.setDirect(direction);
        BinaryString label = BinaryString.fromBytes(input.readBytes(input.readInt()));
        this.setBinaryLabel(label);
        Row value = (Row) kryo.readClassAndObject(input);
        this.value = value;
    }
}
