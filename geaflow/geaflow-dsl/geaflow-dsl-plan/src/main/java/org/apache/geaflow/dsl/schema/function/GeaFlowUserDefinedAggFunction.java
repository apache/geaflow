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

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.util.Optionality;
import org.apache.geaflow.dsl.common.function.UDAF;
import org.apache.geaflow.dsl.common.util.FunctionCallUtils;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.util.SqlTypeUtil;

public class GeaFlowUserDefinedAggFunction extends SqlUserDefinedAggFunction implements Serializable {

    private List<Class<? extends UDAF<?, ?, ?>>> udafClasses;

    private GeaFlowUserDefinedAggFunction(String name, List<Class<? extends UDAF<?, ?, ?>>> udafClasses, GQLJavaTypeFactory typeFactory) {
        super(new SqlIdentifier(name, SqlParserPos.ZERO),
            getReturnTypeInference(name, udafClasses, typeFactory),
            getOperandTypeInference(name, udafClasses, typeFactory),
            getSqlOperandTypeChecker(name, udafClasses, typeFactory),
            null, false, false,
            Optionality.FORBIDDEN,
            typeFactory);
        this.udafClasses = Objects.requireNonNull(udafClasses);
    }

    public static GeaFlowUserDefinedAggFunction create(String name,
                                                       List<Class<? extends UDAF<?, ?, ?>>> clazzs,
                                                       GQLJavaTypeFactory typeFactory) {
        return new GeaFlowUserDefinedAggFunction(name, clazzs, typeFactory);
    }

    private static SqlReturnTypeInference getReturnTypeInference(String name,
                                                                 List<Class<? extends UDAF<?, ?, ?>>> clazzs,
                                                                 GQLJavaTypeFactory typeFactory) {
        return opBinding -> {
            List<Class<?>> callParamTypes = SqlTypeUtil
                .convertToJavaTypes(opBinding.collectOperandTypes(), typeFactory);
            Class<?> clazz = FunctionCallUtils.findMatchUDAF(name, clazzs, callParamTypes);

            Type[] genericTypes = FunctionCallUtils.getUDAFGenericTypes(clazz);
            Type aggOutputType = genericTypes[2];
            return typeFactory.createType(aggOutputType);
        };
    }

    private static SqlOperandTypeInference getOperandTypeInference(String name,
                                                                   List<Class<? extends UDAF<?, ?, ?>>> clazzs,
                                                                   GQLJavaTypeFactory typeFactory) {
        return (callBinding, returnType, operandTypes) -> {
            List<Class<?>> callParamTypes = SqlTypeUtil
                .convertToJavaTypes(callBinding.collectOperandTypes(), typeFactory);
            Class<?> clazz = FunctionCallUtils.findMatchUDAF(name, clazzs, callParamTypes);
            List<Class<?>> aggInputTypes = FunctionCallUtils.getUDAFInputTypes(clazz);
            for (int i = 0; i < operandTypes.length; i++) {
                operandTypes[i] = typeFactory.createType(aggInputTypes.get(i));
            }
        };
    }

    private static SqlOperandTypeChecker getSqlOperandTypeChecker(String name, List<Class<? extends UDAF<?, ?, ?>>> clazzs,
                                                                  GQLJavaTypeFactory typeFactory) {
        return new SqlOperandTypeChecker() {
            @Override
            public boolean checkOperandTypes(SqlCallBinding callBinding,
                                             boolean throwOnFailure) {
                List<Class<?>> callParamTypes = SqlTypeUtil
                    .convertToJavaTypes(callBinding.collectOperandTypes(), typeFactory);
                FunctionCallUtils.findMatchUDAF(name, clazzs, callParamTypes);
                return true;
            }

            @Override
            public SqlOperandCountRange getOperandCountRange() {
                int max = -1;
                int min = 255;

                for (Class<?> clazz : clazzs) {
                    List<Class<?>> inputTypes = FunctionCallUtils.getUDAFInputTypes(clazz);
                    int size = inputTypes.size();
                    if (size > max) {
                        max = size;
                    }
                    if (size < min) {
                        min = size;
                    }
                }
                return SqlOperandCountRanges.between(min, max);
            }

            @Override
            public String getAllowedSignatures(SqlOperator op, String opName) {
                return opName + clazzs.toString();
            }

            @Override
            public Consistency getConsistency() {
                return Consistency.NONE;
            }

            @Override
            public boolean isOptional(int i) {
                return false;
            }
        };
    }

    @Override
    public List<RelDataType> getParamTypes() {
        return null;
    }

    public List<Class<? extends UDAF<?, ?, ?>>> getUdafClasses() {
        return udafClasses;
    }
}
