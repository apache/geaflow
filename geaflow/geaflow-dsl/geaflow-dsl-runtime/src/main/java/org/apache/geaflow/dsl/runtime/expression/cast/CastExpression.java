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

package org.apache.geaflow.dsl.runtime.expression.cast;

import java.util.Collections;
import java.util.List;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.util.TypeCastUtil;
import org.apache.geaflow.dsl.common.util.TypeCastUtil.ITypeCast;
import org.apache.geaflow.dsl.runtime.expression.AbstractNonLeafExpression;
import org.apache.geaflow.dsl.runtime.expression.Expression;

public class CastExpression extends AbstractNonLeafExpression {

    private final ITypeCast typeCast;

    public CastExpression(Expression input, IType<?> outputType) {
        super(Collections.singletonList(input), outputType);
        this.typeCast = TypeCastUtil.getTypeCast(input.getOutputType(), outputType);
    }

    @Override
    public Object evaluate(Row row) {
        Object inputValue = inputs.get(0).evaluate(row);
        return typeCast.castTo(inputValue);
    }

    @Override
    public String showExpression() {
        return "cast(" + inputs.get(0).showExpression() + " as " + getOutputType().getTypeClass().getSimpleName() + ")";
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        return new CastExpression(inputs.get(0), outputType);
    }
}
