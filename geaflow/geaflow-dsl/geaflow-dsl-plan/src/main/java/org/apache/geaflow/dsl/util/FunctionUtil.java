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

package org.apache.geaflow.dsl.util;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.geaflow.common.utils.ArrayUtil;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.function.UDAF;
import org.apache.geaflow.dsl.common.function.UDF;
import org.apache.geaflow.dsl.common.function.UDTF;
import org.apache.geaflow.dsl.common.util.FunctionCallUtils;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.schema.GeaFlowFunction;
import org.apache.geaflow.dsl.schema.GeaFlowFunction.FunctionType;
import org.apache.geaflow.dsl.schema.function.GeaFlowUserDefinedAggFunction;
import org.apache.geaflow.dsl.schema.function.GeaFlowUserDefinedGraphAlgorithm;
import org.apache.geaflow.dsl.schema.function.GeaFlowUserDefinedScalarFunction;
import org.apache.geaflow.dsl.schema.function.GeaFlowUserDefinedTableFunction;
import org.apache.geaflow.dsl.udf.table.other.GraphMetaFieldAccessFunction;

public class FunctionUtil {

    @SuppressWarnings("unchecked")
    public static SqlFunction createSqlFunction(GeaFlowFunction function, GQLJavaTypeFactory typeFactory) {
        String name = function.getName();
        List<Class<?>> functionClazzs = new ArrayList<>();
        try {
            for (String className : function.getClazz()) {
                Class reflectClass = Thread.currentThread().getContextClassLoader().loadClass(className);
                functionClazzs.add(reflectClass);
            }
        } catch (Exception e) {
            throw new GeaFlowDSLException(e);
        }

        FunctionType type;
        if (UDF.class.isAssignableFrom(functionClazzs.get(0))) {
            type = FunctionType.UDF;
        } else if (UDAF.class.isAssignableFrom(functionClazzs.get(0))) {
            type = FunctionType.UDAF;
        } else if (UDTF.class.isAssignableFrom(functionClazzs.get(0))) {
            type = FunctionType.UDTF;
        } else if (AlgorithmUserFunction.class.isAssignableFrom(functionClazzs.get(0))) {
            type = FunctionType.UDGA;
        } else {
            throw new GeaFlowDSLException("UnKnow function type of function " + functionClazzs);
        }
        switch (type) {
            case UDF:
                return GeaFlowUserDefinedScalarFunction.create(name,
                    (Class<? extends UDF>) functionClazzs.get(0), typeFactory);
            case UDGA:
                return GeaFlowUserDefinedGraphAlgorithm.create(name,
                    (Class<? extends AlgorithmUserFunction>) functionClazzs.get(0), typeFactory);
            case UDTF:
                return GeaFlowUserDefinedTableFunction.create(name,
                    (Class<? extends UDTF>) functionClazzs.get(0), typeFactory);
            case UDAF:
                return GeaFlowUserDefinedAggFunction.create(name,
                    ArrayUtil.castList(functionClazzs), typeFactory);
            default:
                throw new GeaFlowDSLException("should never run here");
        }

    }


    public static SqlOperandTypeChecker getSqlOperandTypeChecker(String name, Class<?> udfClass,
                                                                 GQLJavaTypeFactory typeFactory) {
        final List<Class[]> types = FunctionCallUtils.getAllEvalParamTypes(udfClass);
        return new SqlOperandTypeChecker() {
            @Override
            public boolean checkOperandTypes(SqlCallBinding callBinding,
                                             boolean throwOnFailure) {
                List<Class<?>> callParamTypes =
                    SqlTypeUtil.convertToJavaTypes(callBinding.collectOperandTypes(), typeFactory);
                FunctionCallUtils.findMatchMethod(udfClass, callParamTypes);
                return true;
            }

            @Override
            public SqlOperandCountRange getOperandCountRange() {
                int min = 255;
                int max = -1;

                for (Class[] ts : types) {
                    int paramLength = ts.length;
                    if (paramLength > 0 && ts[ts.length - 1].isArray()) {
                        max = 254;
                        paramLength = ts.length - 1;
                    }
                    max = Math.max(paramLength, max);
                    min = Math.min(paramLength, min);
                }
                return SqlOperandCountRanges.between(min, max);
            }

            @Override
            public String getAllowedSignatures(SqlOperator op, String opName) {
                return opName + types;
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

    public static SqlOperandTypeInference getSqlOperandTypeInference(Class<?> udfClass,
                                                                     GQLJavaTypeFactory typeFactory) {
        return (callBinding, returnType, operandTypes) -> {
            List<Class<?>> callParamJavaTypes =
                SqlTypeUtil.convertToJavaTypes(callBinding.collectOperandTypes(), typeFactory);
            Method method = FunctionCallUtils.findMatchMethod(udfClass, callParamJavaTypes);

            Class<?>[] realTypes = method.getParameterTypes();
            RelDataType[] realParamTypes = new RelDataType[realTypes.length];

            for (int i = 0; i < realTypes.length; i++) {
                realParamTypes[i] = typeFactory.createType(realTypes[i]);
            }

            int varIndex = -1;
            for (int i = 0; i < operandTypes.length; i++) {
                if (i < realParamTypes.length
                    && realParamTypes[i].getComponentType() != null) {
                    varIndex = i;
                }
                if (varIndex >= 0) {
                    operandTypes[i] = realParamTypes[varIndex].getComponentType();
                } else {
                    operandTypes[i] = realParamTypes[i];
                }
            }
        };
    }

    public static SqlReturnTypeInference getSqlReturnTypeInference(Class<?> clazz, String functionName) {
        return opBinding -> {
            final JavaTypeFactoryImpl typeFactory = (JavaTypeFactoryImpl) opBinding.getTypeFactory();
            List<Class<?>> paramJavaTypes =
                SqlTypeUtil.convertToJavaTypes(opBinding.collectOperandTypes(), typeFactory);

            Method method = FunctionCallUtils.findMatchMethod(clazz, functionName, paramJavaTypes);
            if (GraphMetaFieldAccessFunction.class.isAssignableFrom(clazz)) {
                Class<?> returnClazz = method.getReturnType();
                if (returnClazz.equals(Object.class)) {
                    try {
                        GraphMetaFieldAccessFunction func =
                            ((GraphMetaFieldAccessFunction) clazz.newInstance());
                        return func.getReturnRelDataType((GQLJavaTypeFactory) typeFactory);
                    } catch (Exception e) {
                        throw new GeaFlowDSLException(e,
                            "Cannot get instance of {}", clazz.getName());
                    }
                }
            }
            return typeFactory.createType(method.getReturnType());
        };
    }

}
