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

package org.apache.geaflow.dsl.runtime.function.graph;

import static org.apache.geaflow.dsl.common.util.FunctionCallUtils.getUDAFGenericTypes;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.utils.ClassUtil;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.function.FunctionContext;
import org.apache.geaflow.dsl.common.function.UDAF;
import org.apache.geaflow.dsl.common.function.UDAFArguments;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.plan.PhysicAggregateRelNode.DistinctUDAF;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepCollector;
import org.apache.geaflow.dsl.runtime.traversal.data.ObjectSingleValue;
import org.apache.geaflow.dsl.runtime.traversal.data.SingleValue;

public class StepAggExpressionFunctionImpl implements StepAggregateFunction {

    private final StepAggCall[] aggFunctionCalls;

    private final IType<?>[] aggOutputTypes;

    private UDAF<Object, Object, Object>[] udafs;

    private final int[] pathPruneIndices;

    private final IType<?>[] inputPathTypes;

    private final IType<?>[] pathPruneTypes;

    public StepAggExpressionFunctionImpl(int[] pathPruneIndices,
                                         IType<?>[] pathPruneTypes,
                                         IType<?>[] inputPathTypes,
                                         List<StepAggCall> aggFunctionCalls,
                                         List<IType<?>> aggOutputTypes) {
        this.pathPruneIndices = Objects.requireNonNull(pathPruneIndices);
        this.pathPruneTypes = Objects.requireNonNull(pathPruneTypes);
        this.inputPathTypes = Objects.requireNonNull(inputPathTypes);
        this.aggFunctionCalls = Objects.requireNonNull(aggFunctionCalls).toArray(new StepAggCall[]{});
        this.aggOutputTypes = Objects.requireNonNull(aggOutputTypes).toArray(new IType<?>[]{});
    }

    public static class Accumulator implements Serializable {

        private final Object[] accumulators;

        public Accumulator(Object[] accumulators) {
            this.accumulators = accumulators;
        }

        public Object getAcc(int index) {
            return accumulators[index];
        }
    }

    public static class StepAggCall implements Serializable {

        /**
         * The name of the agg function, e.g. SUM、COUNT.
         */
        private final String name;

        /**
         * The argument field expressions.
         */
        private final Expression[] argExpressions;

        /**
         * The argument field type.
         */
        private final IType<?>[] argFieldTypes;

        /**
         * The UDAF implement class.
         */
        private final Class<? extends UDAF<?, ?, ?>> udafClass;

        /**
         * The UDAF input class.
         */
        private final Class<?> udafInputClass;
        /**
         * The distinct flag.
         */
        private final boolean isDistinct;

        public StepAggCall(String name, Expression[] argExpressions, IType<?>[] argFieldTypes,
                           Class<? extends UDAF<?, ?, ?>> udafClass, boolean isDistinct) {
            this.name = name;
            this.argExpressions = argExpressions;
            this.argFieldTypes = argFieldTypes;
            this.udafClass = udafClass;
            Type[] genericTypes = getUDAFGenericTypes(udafClass);
            this.udafInputClass = (Class<?>) genericTypes[0];
            this.isDistinct = isDistinct;
        }

        public String getName() {
            return name;
        }

        public Expression[] getArgExpressions() {
            return argExpressions;
        }

        public IType<?>[] getArgFieldTypes() {
            return argFieldTypes;
        }

        public Class<? extends UDAF<?, ?, ?>> getUdafClass() {
            return udafClass;
        }

        public boolean isDistinct() {
            return isDistinct;
        }
    }

    @Override
    public void open(TraversalRuntimeContext context, FunctionSchemas schemas) {
        udafs = new UDAF[aggFunctionCalls.length];
        for (int i = 0; i < aggFunctionCalls.length; i++) {
            try {
                udafs[i] = (UDAF<Object, Object, Object>) aggFunctionCalls[i].getUdafClass().newInstance();
                if (aggFunctionCalls[i].isDistinct) {
                    udafs[i] = new DistinctUDAF(udafs[i]);
                }
                udafs[i].open(FunctionContext.of(context.getConfig()));
            } catch (InstantiationException | IllegalAccessException e) {
                throw new GeaFlowDSLException("Error in create UDAF: " + aggFunctionCalls[i].getUdafClass());
            }
        }
    }

    @Override
    public void finish(StepCollector<StepRecord> collector) {

    }

    public IType<?>[] getAggOutputTypes() {
        return aggOutputTypes;
    }

    public int[] getPathPruneIndices() {
        return pathPruneIndices;
    }

    public IType<?>[] getPathPruneTypes() {
        return pathPruneTypes;
    }

    public IType<?>[] getInputPathTypes() {
        return inputPathTypes;
    }

    @Override
    public List<Expression> getExpressions() {
        return Collections.emptyList();
    }

    @Override
    public StepFunction copy(List<Expression> expressions) {
        return new StepAggExpressionFunctionImpl(pathPruneIndices, pathPruneTypes, inputPathTypes,
            Arrays.stream(aggFunctionCalls).collect(Collectors.toList()),
            Arrays.stream(aggOutputTypes).collect(Collectors.toList()));
    }

    @Override
    public Object createAccumulator() {
        Object[] accumulators = new Object[udafs.length];
        for (int i = 0; i < accumulators.length; i++) {
            accumulators[i] = udafs[i].createAccumulator();
        }
        return new Accumulator(accumulators);
    }

    @Override
    public void add(Row row, Object accumulator) {
        for (int i = 0; i < udafs.length; i++) {
            StepAggCall aggInfo = aggFunctionCalls[i];
            Object argValue;
            if (aggInfo.argExpressions.length == 0) { // for count() without input parameter.
                argValue = row;
            } else if (aggInfo.argExpressions.length == 1) {
                argValue = aggInfo.argExpressions[0].evaluate(row);
            } else { // for agg with multi-parameters
                assert UDAFArguments.class.isAssignableFrom(aggInfo.udafInputClass);
                Object[] parameters = new Object[aggInfo.argExpressions.length];
                for (int p = 0; p < parameters.length; p++) {
                    parameters[p] = aggInfo.argExpressions[p].evaluate(row);
                }
                argValue = ClassUtil.newInstance(aggInfo.udafInputClass);
                ((UDAFArguments) argValue).setParams(parameters);
            }
            udafs[i].accumulate(((Accumulator) accumulator).accumulators[i], argValue);
        }
    }

    @Override
    public void merge(Object acc, Object otherAcc) {
        for (int i = 0; i < udafs.length; i++) {
            udafs[i].merge(
                ((Accumulator) acc).getAcc(i),
                Collections.singletonList(((Accumulator) otherAcc).getAcc(i)));
        }
    }

    @Override
    public SingleValue getValue(Object accumulator) {
        Object[] aggValues = new Object[udafs.length];
        for (int i = 0; i < udafs.length; i++) {
            aggValues[i] = udafs[i].getValue(((Accumulator) accumulator).accumulators[i]);
        }
        return ObjectSingleValue.of(ObjectRow.create(aggValues));
    }
}
