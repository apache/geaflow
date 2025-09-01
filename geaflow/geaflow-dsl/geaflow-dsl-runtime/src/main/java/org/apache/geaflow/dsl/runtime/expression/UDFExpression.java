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

package org.apache.geaflow.dsl.runtime.expression;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.function.Description;

public class UDFExpression extends AbstractReflectCallExpression {

    public static final String METHOD_NAME = "eval";

    private final String udfName;

    public UDFExpression(List<Expression> inputs, IType<?> outputType, Class<?> implementClass) {
        super(inputs, outputType, implementClass, METHOD_NAME);
        this.udfName = implementClass.getAnnotation(Description.class).name();
    }

    @Override
    public String showExpression() {
        return udfName + "("
            + inputs.stream().map(Expression::showExpression)
            .collect(Collectors.joining(","))
            + ")";
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        return new UDFExpression(inputs, outputType, implementClass);
    }

    public Class<?> getUdfClass() {
        return implementClass;
    }
}
