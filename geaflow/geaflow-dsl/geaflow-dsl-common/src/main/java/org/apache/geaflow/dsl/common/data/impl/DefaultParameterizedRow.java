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

package org.apache.geaflow.dsl.common.data.impl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.ParameterizedRow;
import org.apache.geaflow.dsl.common.data.Row;

public class DefaultParameterizedRow implements ParameterizedRow {

    private final Row baseRow;

    private final Object requestId;

    private final Row parameterRow;

    private final Row systemVariableRow;

    public DefaultParameterizedRow(Row baseRow, Object requestId, Row parameterRow, Row systemVariableRow) {
        this.baseRow = baseRow;
        this.requestId = requestId;
        this.parameterRow = parameterRow;
        this.systemVariableRow = systemVariableRow;
    }

    public DefaultParameterizedRow(Row baseRow, Object requestId, Row parameterRow) {
        this(baseRow, requestId, parameterRow, null);
    }

    @Override
    public Object getField(int i, IType<?> type) {
        return baseRow.getField(i, type);
    }

    @Override
    public Row getParameter() {
        return parameterRow;
    }

    @Override
    public Row getSystemVariables() {
        return systemVariableRow;
    }

    @Override
    public Object getRequestId() {
        return requestId;
    }

    public static class DefaultParameterizedRowSerializer extends Serializer<DefaultParameterizedRow> {

        @Override
        public void write(Kryo kryo, Output output, DefaultParameterizedRow object) {
            kryo.writeClassAndObject(output, object.baseRow);
            kryo.writeClassAndObject(output, object.getRequestId());
            kryo.writeClassAndObject(output, object.getParameter());
            kryo.writeClassAndObject(output, object.getSystemVariables());
        }

        @Override
        public DefaultParameterizedRow read(Kryo kryo, Input input, Class<DefaultParameterizedRow> aClass) {
            Row baseRow = (Row) kryo.readClassAndObject(input);
            Object requestId = kryo.readClassAndObject(input);
            Row parameterRow = (Row) kryo.readClassAndObject(input);
            Row systemVariableRow = (Row) kryo.readClassAndObject(input);
            return new DefaultParameterizedRow(baseRow, requestId, parameterRow, systemVariableRow);
        }

        @Override
        public DefaultParameterizedRow copy(Kryo kryo, DefaultParameterizedRow original) {
            Row baseRow = kryo.copy(original.baseRow);
            Object requestId = kryo.copy(original.getRequestId());
            Row parameterRow = kryo.copy(original.getParameter());
            Row systemVariableRow = kryo.copy(original.getSystemVariables());
            return new DefaultParameterizedRow(baseRow, requestId, parameterRow, systemVariableRow);
        }
    }

}
