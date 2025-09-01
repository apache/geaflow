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

package org.apache.geaflow.dsl.runtime.expression.field;

import java.util.Collections;
import java.util.List;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.ParameterizedRow;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.runtime.expression.AbstractExpression;
import org.apache.geaflow.dsl.runtime.expression.Expression;

public class ParameterFieldExpression extends AbstractExpression {

    protected final int fieldIndex;

    protected final IType<?> outputType;

    public ParameterFieldExpression(int fieldIndex, IType<?> outputType) {
        this.fieldIndex = fieldIndex;
        this.outputType = outputType;
    }

    @Override
    public Object evaluate(Row row) {
        ParameterizedRow parameterizedRow = (ParameterizedRow) row;
        return parameterizedRow.getParameter().getField(fieldIndex, outputType);
    }

    @Override
    public String showExpression() {
        return "$$" + fieldIndex;
    }

    @Override
    public IType<?> getOutputType() {
        return outputType;
    }

    @Override
    public List<Expression> getInputs() {
        return Collections.emptyList();
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        assert inputs.size() == 0;
        return new ParameterFieldExpression(fieldIndex, outputType);
    }

    public int getFieldIndex() {
        return fieldIndex;
    }
}
