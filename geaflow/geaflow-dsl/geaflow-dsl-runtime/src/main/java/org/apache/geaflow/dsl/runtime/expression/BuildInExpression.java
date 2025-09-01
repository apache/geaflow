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

public class BuildInExpression extends AbstractReflectCallExpression {

    public static final String FLOOR = "floor";

    public static final String TIMESTAMP_FLOOR = "timestampFloor";

    public static final String CEIL = "ceil";

    public static final String TIMESTAMP_CEIL = "timestampCeil";

    public static final String TRIM = "trim";

    public static final String SIMILAR = "similar";

    public static final String CONCAT = "concat";

    public static final String LENGTH = "length";

    public static final String UPPER = "upper";

    public static final String LOWER = "lower";

    public static final String POSITION = "position";

    public static final String OVERLAY = "overlay";

    public static final String SUBSTRING = "substring";

    public static final String INITCAP = "initcap";

    public static final String ABS = "abs";

    public static final String POWER = "power";

    public static final String SIN = "sin";

    public static final String COS = "cos";

    public static final String LN = "ln";

    public static final String LOG10 = "log10";

    public static final String EXP = "exp";

    public static final String TAN = "tan";

    public static final String COT = "cot";

    public static final String ASIN = "asin";

    public static final String ACOS = "acos";

    public static final String ATAN = "atan";

    public static final String DEGREES = "degrees";

    public static final String RADIANS = "radians";

    public static final String SIGN = "sign";

    public static final String RAND = "rand";

    public static final String RAND_INTEGER = "randInt";

    public static final String CURRENT_TIMESTAMP = "currentTimestamp";

    public BuildInExpression(List<Expression> inputs, IType<?> outputType,
                             Class<?> implementClass, String methodName) {
        super(inputs, outputType, implementClass, methodName);
    }

    @Override
    public String showExpression() {
        return methodName + "("
            + inputs.stream().map(Expression::showExpression).collect(Collectors.joining(","))
            + ")";
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        return new BuildInExpression(inputs, outputType, implementClass, methodName);
    }
}
