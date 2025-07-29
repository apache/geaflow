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

public class ObjectTsEdge extends ObjectEdge implements IGraphElementWithTimeField,
    KryoSerializable {

    public static final Supplier<ObjectTsEdge> CONSTRUCTOR = new Constructor();

    private long time;

    public ObjectTsEdge() {

    }

    public ObjectTsEdge(Object srcId, Object targetId) {
        super(srcId, targetId);
    }

    public ObjectTsEdge(Object srcId, Object targetId, Row value) {
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
        if (!(o instanceof ObjectTsEdge)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ObjectTsEdge that = (ObjectTsEdge) o;
        return time == that.time;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), time);
    }

    @Override
    public ObjectTsEdge reverse() {
        ObjectTsEdge edge = new ObjectTsEdge(getTargetId(), getSrcId(), getValue());
        edge.setBinaryLabel(getBinaryLabel());
        edge.setDirect(getDirect());
        edge.setTime(time);
        return edge;
    }

    @Override
    public RowEdge withDirection(EdgeDirection direction) {
        ObjectTsEdge edge = new ObjectTsEdge(getSrcId(), getTargetId(), getValue());
        edge.setBinaryLabel(getBinaryLabel());
        edge.setDirect(direction);
        edge.setTime(getTime());
        return edge;
    }

    @Override
    public ObjectEdge withValue(Row value) {
        ObjectTsEdge edge = new ObjectTsEdge(getSrcId(), getTargetId(), value);
        edge.setBinaryLabel(getBinaryLabel());
        edge.setDirect(getDirect());
        edge.setTime(time);
        return edge;
    }

    @Override
    public RowEdge identityReverse() {
        ObjectTsEdge edge = new ObjectTsEdge(getTargetId(), getSrcId(), getValue());
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

    private static class Constructor implements Supplier<ObjectTsEdge> {

        @Override
        public ObjectTsEdge get() {
            return new ObjectTsEdge();
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        // serialize srcId, targetId, direction, label, value and time
        kryo.writeClassAndObject(output, this.getSrcId());
        kryo.writeClassAndObject(output, this.getTargetId());
        kryo.writeClassAndObject(output, this.getDirect());
        kryo.writeClassAndObject(output, this.getBinaryLabel());
        kryo.writeClassAndObject(output, this.getValue());
        output.writeLong(this.getTime());
    }

    @Override
    public void read(Kryo kryo, Input input) {
        // deserialize srcId, targetId, direction, label, value and time
        Object srcId = kryo.readClassAndObject(input);
        Object targetId = kryo.readClassAndObject(input);
        EdgeDirection direction = (EdgeDirection) kryo.readClassAndObject(input);
        BinaryString label = (BinaryString) kryo.readClassAndObject(input);
        Row value = (Row) kryo.readClassAndObject(input);
        long time = input.readLong();
        this.setSrcId(srcId);
        this.setTargetId(targetId);
        this.setValue(value);
        this.setBinaryLabel(label);
        this.setDirect(direction);
        this.setTime(time);
    }

}
