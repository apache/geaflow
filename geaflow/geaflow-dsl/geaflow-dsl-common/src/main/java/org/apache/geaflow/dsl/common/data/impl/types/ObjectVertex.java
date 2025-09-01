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
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.VertexType;
import org.apache.geaflow.dsl.common.util.BinaryUtil;
import org.apache.geaflow.model.graph.vertex.IVertex;

public class ObjectVertex implements RowVertex, KryoSerializable {

    public static final Supplier<ObjectVertex> CONSTRUCTOR = new Constructor();

    private Object id;

    private BinaryString label;

    private Row value;

    public ObjectVertex() {

    }

    public ObjectVertex(Object id) {
        this.id = id;
    }

    public ObjectVertex(Object id, BinaryString label, Row value) {
        this.id = id;
        this.label = label;
        this.value = value;
    }

    @Override
    public String getLabel() {
        return label.toString();
    }

    @Override
    public void setLabel(String label) {
        this.label = (BinaryString) BinaryUtil.toBinaryLabel(label);
    }

    @Override
    public Object getId() {
        return id;
    }

    @Override
    public void setId(Object id) {
        this.id = Objects.requireNonNull(id);
    }

    @Override
    public Row getValue() {
        return value;
    }

    @Override
    public ObjectVertex withValue(Row value) {
        return new ObjectVertex(id, label, value);
    }

    @Override
    public ObjectVertex withLabel(String label) {
        return new ObjectVertex(id, (BinaryString) BinaryUtil.toBinaryLabel(label), value);
    }

    @Override
    public IVertex<Object, Row> withTime(long time) {
        throw new GeaFlowDSLException("Vertex not support timestamp");
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(Object o) {
        RowVertex vertex = (RowVertex) o;
        if (id instanceof Comparable) {
            return ((Comparable) id).compareTo(vertex.getId());
        }
        return Integer.compare(getId().hashCode(), vertex.getId().hashCode());
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

        return id.equals(that.getId()) && Objects.equals(label, that.getBinaryLabel());
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, label);
    }

    @Override
    public Object getField(int i, IType<?> type) {
        switch (i) {
            case VertexType.ID_FIELD_POSITION:
                return id;
            case VertexType.LABEL_FIELD_POSITION:
                return label;
            default:
                return value.getField(i - 2, type);
        }
    }

    @Override
    public void setValue(Row value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return id + "#" + label + "#" + value;
    }

    @Override
    public BinaryString getBinaryLabel() {
        return label;
    }

    @Override
    public void setBinaryLabel(BinaryString label) {
        this.label = label;
    }

    private static class Constructor implements Supplier<ObjectVertex> {

        @Override
        public ObjectVertex get() {
            return new ObjectVertex();
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        // serialize id, label, and value
        kryo.writeClassAndObject(output, this.getId());
        kryo.writeClassAndObject(output, this.getBinaryLabel());
        kryo.writeClassAndObject(output, this.getValue());
    }

    @Override
    public void read(Kryo kryo, Input input) {
        // deserialize id, label, and value
        Object id = kryo.readClassAndObject(input);
        BinaryString label = (BinaryString) kryo.readClassAndObject(input);
        Row value = (Row) kryo.readClassAndObject(input);
        this.id = id;
        this.setValue(value);
        this.setBinaryLabel(label);
    }


}
