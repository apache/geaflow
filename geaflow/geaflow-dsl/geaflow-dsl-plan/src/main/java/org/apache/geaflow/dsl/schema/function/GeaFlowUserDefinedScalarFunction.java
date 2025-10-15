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

package org.apache.geaflow.dsl.schema.function;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.function.UDF;
import org.apache.geaflow.dsl.common.util.FunctionCallUtils;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.schema.GeaFlowFunction;
import org.apache.geaflow.dsl.util.FunctionUtil;

public class GeaFlowUserDefinedScalarFunction extends SqlUserDefinedFunction {

    private final Class<? extends UDF> implementClass;

    private GeaFlowUserDefinedScalarFunction(String name, Class<? extends UDF> clazz,
                                             GQLJavaTypeFactory typeFactory) {
        super(new SqlIdentifier(name, SqlParserPos.ZERO),
            getReturnTypeInference(clazz),
            FunctionUtil.getSqlOperandTypeInference(clazz, typeFactory),
            FunctionUtil.getSqlOperandTypeChecker(name, clazz, typeFactory),
            null, null);
        this.implementClass = clazz;
    }

    public static GeaFlowUserDefinedScalarFunction create(String name, Class<? extends UDF> clazz,
                                                          GQLJavaTypeFactory typeFactory) {
        return new GeaFlowUserDefinedScalarFunction(name, clazz, typeFactory);
    }

    @SuppressWarnings("unchecked")
    public static GeaFlowUserDefinedScalarFunction create(GeaFlowFunction function, GQLJavaTypeFactory typeFactory) {
        String name = function.getName();
        String className = function.getClazz().get(0);
        try {
            Class<? extends UDF> clazz = (Class<? extends UDF>) Thread.currentThread()
                .getContextClassLoader().loadClass(className);
            return new GeaFlowUserDefinedScalarFunction(name, clazz, typeFactory);
        } catch (ClassNotFoundException e) {
            throw new GeaFlowDSLException(e);
        }
    }

    private static SqlReturnTypeInference getReturnTypeInference(final Class<?> clazz) {
        return FunctionUtil.getSqlReturnTypeInference(clazz, FunctionCallUtils.UDF_EVAL_METHOD_NAME);
    }

    public Class<? extends UDF> getImplementClass() {
        return implementClass;
    }
}
