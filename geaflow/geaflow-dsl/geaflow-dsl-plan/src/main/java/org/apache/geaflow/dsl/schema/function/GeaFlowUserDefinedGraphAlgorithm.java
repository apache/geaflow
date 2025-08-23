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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.util.FunctionUtil;
import org.apache.geaflow.dsl.util.SqlTypeUtil;

public class GeaFlowUserDefinedGraphAlgorithm extends SqlUserDefinedFunction {

    private final Class<? extends AlgorithmUserFunction> implementClass;

    private GeaFlowUserDefinedGraphAlgorithm(String name, Class<? extends AlgorithmUserFunction> clazz,
                                             GQLJavaTypeFactory typeFactory) {
        super(new SqlIdentifier(name, SqlParserPos.ZERO),
            getReturnTypeInference(clazz),
            FunctionUtil.getSqlOperandTypeInference(clazz, typeFactory),
            FunctionUtil.getSqlOperandTypeChecker(name, clazz, typeFactory),
            null, null);
        this.implementClass = clazz;
    }

    public static GeaFlowUserDefinedGraphAlgorithm create(String name, Class<? extends AlgorithmUserFunction> clazz,
                                                          GQLJavaTypeFactory typeFactory) {
        return new GeaFlowUserDefinedGraphAlgorithm(name, clazz, typeFactory);
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.GQL_ALGORITHM;
    }

    private static SqlReturnTypeInference getReturnTypeInference(final Class<?> clazz) {
        return opBinding -> {
            final GQLJavaTypeFactory typeFactory = (GQLJavaTypeFactory) opBinding.getTypeFactory();
            AlgorithmUserFunction algorithm;
            try {
                algorithm = (AlgorithmUserFunction) clazz.getConstructor().newInstance();
            } catch (Exception e) {
                throw new GeaFlowDSLException("Cannot new instance for class: " + clazz.getName(), e);
            }
            final StructType outputType =
                algorithm.getOutputType(typeFactory.getCurrentGraph().getGraphSchema(typeFactory));
            return SqlTypeUtil.convertToRelType(outputType, true, typeFactory);
        };
    }

    public Class<? extends AlgorithmUserFunction> getImplementClass() {
        return implementClass;
    }

}
