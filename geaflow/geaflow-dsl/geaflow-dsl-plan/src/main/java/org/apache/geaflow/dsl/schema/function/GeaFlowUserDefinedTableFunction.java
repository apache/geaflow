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

import static org.apache.geaflow.dsl.util.FunctionUtil.getSqlOperandTypeChecker;
import static org.apache.geaflow.dsl.util.FunctionUtil.getSqlOperandTypeInference;

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.IRichTableFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.function.UDTF;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.util.SqlTypeUtil;

public class GeaFlowUserDefinedTableFunction extends SqlUserDefinedTableFunction implements Serializable {

    private final Class<? extends UDTF> implementClass;

    private GeaFlowUserDefinedTableFunction(String name, Class<? extends UDTF> clazz, IRichTableFunction tableFunction,
                                            GQLJavaTypeFactory typeFactory) {
        super(new SqlIdentifier(name, SqlParserPos.ZERO),
            ReturnTypes.CURSOR,
            getSqlOperandTypeInference(clazz, typeFactory),
            getSqlOperandTypeChecker(name, clazz, typeFactory),
            null, tableFunction);
        this.implementClass = clazz;
    }

    public static GeaFlowUserDefinedTableFunction create(String name, Class<? extends UDTF> clazz,
                                                         GQLJavaTypeFactory typeFactory) {
        try {
            GeaflowTableFunction function = new GeaflowTableFunction(clazz, typeFactory);
            return new GeaFlowUserDefinedTableFunction(name, clazz, function, typeFactory);
        } catch (Exception e) {
            throw new GeaFlowDSLException(e);
        }
    }

    public Class<? extends UDTF> getImplementClass() {
        return implementClass;
    }

    public static class GeaflowTableFunction implements IRichTableFunction, Serializable {

        private final GQLJavaTypeFactory typeFactory;
        private UDTF functionInstance;

        public GeaflowTableFunction(Class<?> clazz, GQLJavaTypeFactory typeFactory) {
            this.typeFactory = typeFactory;
            try {
                functionInstance = (UDTF) clazz.newInstance();
            } catch (Exception e) {
                throw new GeaFlowDSLException("Failed to create instance for " + clazz);
            }
        }

        @Override
        public List<FunctionParameter> getParameters() {
            return new ArrayList<>();
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory, List<Object> arguments) {
            return null;
        }

        @Override
        public Type getElementType(List<Object> arguments) {
            return Object[].class;
        }


        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory, List<Object> args, List<RelDataType> paramTypes,
                                      List<String> outFieldNames) {

            List<RelDataType> fieldTypes = Lists.newArrayList();
            List<Class<?>> callJavaTypes =
                SqlTypeUtil.convertToJavaTypes(paramTypes, (GQLJavaTypeFactory) typeFactory);

            List<Class<?>> returnTypes = functionInstance.getReturnType(callJavaTypes, outFieldNames);
            for (int i = 0; i < returnTypes.size(); i++) {
                fieldTypes.add(this.typeFactory.createType(returnTypes.get(i)));
            }
            if (outFieldNames.size() != fieldTypes.size()) {
                throw new GeaFlowDSLException(String.format("Output fields size[%d] should equal to return type "
                        + "size[%d] defined in class: %s", outFieldNames.size(), fieldTypes.size(),
                    functionInstance.getClass().toString()));
            }
            return this.typeFactory.createStructType(fieldTypes, outFieldNames);
        }
    }
}
