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

package org.apache.geaflow.dsl.rel;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.geaflow.common.utils.ClassUtil;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.util.SqlTypeUtil;

public abstract class GraphAlgorithm extends SingleRel {

    protected final Class<? extends AlgorithmUserFunction> userFunctionClass;

    protected final Object[] params;

    protected GraphAlgorithm(RelOptCluster cluster, RelTraitSet traits,
                             RelNode input,
                             Class<? extends AlgorithmUserFunction> userFunctionClass,
                             Object[] params) {
        super(cluster, traits, input);
        this.userFunctionClass = Objects.requireNonNull(userFunctionClass);
        this.params = Objects.requireNonNull(params);
        this.rowType = getFunctionOutputType(userFunctionClass, cluster.getTypeFactory());
    }

    public Class<? extends AlgorithmUserFunction> getUserFunctionClass() {
        return userFunctionClass;
    }

    public Object[] getParams() {
        return params;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        List<String> paraClassNames = Arrays.stream(params).map(
            para -> para.getClass().getSimpleName()
        ).collect(Collectors.toList());
        return super.explainTerms(pw)
            .item("algo", userFunctionClass.getSimpleName())
            .item("params", paraClassNames)
            .item("outputType", getFunctionOutputType(userFunctionClass,
                getCluster().getTypeFactory()));
    }

    public static RelDataType getFunctionOutputType(Class<? extends AlgorithmUserFunction> userFunctionClass,
                                                    RelDataTypeFactory typeFactory) {
        AlgorithmUserFunction<?, ?> userFunction = ClassUtil.newInstance(userFunctionClass);
        GQLJavaTypeFactory factory = (GQLJavaTypeFactory) typeFactory;
        return SqlTypeUtil.convertToRelType(userFunction.getOutputType(
                factory.getCurrentGraph().getGraphSchema(factory)),
            false, typeFactory);
    }


    public abstract GraphAlgorithm copy(RelTraitSet traitSet, RelNode input,
                                        Class<? extends AlgorithmUserFunction> userFunctionClass,
                                        Object[] params);

    @Override
    public GraphAlgorithm copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 1;
        return copy(traitSet, inputs.get(0), userFunctionClass, params);
    }

}
