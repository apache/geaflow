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

package org.apache.geaflow.dsl.runtime.plan;

import static org.apache.geaflow.dsl.common.util.FunctionCallUtils.getUDAFGenericTypes;

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.utils.ArrayUtil;
import org.apache.geaflow.common.utils.ClassUtil;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.function.FunctionContext;
import org.apache.geaflow.dsl.common.function.UDAF;
import org.apache.geaflow.dsl.common.function.UDAFArguments;
import org.apache.geaflow.dsl.common.util.FunctionCallUtils;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.RDataView;
import org.apache.geaflow.dsl.runtime.RDataView.ViewType;
import org.apache.geaflow.dsl.runtime.RuntimeGraph;
import org.apache.geaflow.dsl.runtime.RuntimeTable;
import org.apache.geaflow.dsl.runtime.function.table.AggFunction;
import org.apache.geaflow.dsl.runtime.function.table.GroupByFunction;
import org.apache.geaflow.dsl.runtime.function.table.GroupByFunctionImpl;
import org.apache.geaflow.dsl.schema.function.GeaFlowUserDefinedAggFunction;
import org.apache.geaflow.dsl.udf.table.agg.AvgDouble;
import org.apache.geaflow.dsl.udf.table.agg.AvgInteger;
import org.apache.geaflow.dsl.udf.table.agg.AvgLong;
import org.apache.geaflow.dsl.udf.table.agg.Count;
import org.apache.geaflow.dsl.udf.table.agg.MaxBinaryString;
import org.apache.geaflow.dsl.udf.table.agg.MaxDouble;
import org.apache.geaflow.dsl.udf.table.agg.MaxInteger;
import org.apache.geaflow.dsl.udf.table.agg.MaxLong;
import org.apache.geaflow.dsl.udf.table.agg.MinBinaryString;
import org.apache.geaflow.dsl.udf.table.agg.MinDouble;
import org.apache.geaflow.dsl.udf.table.agg.MinInteger;
import org.apache.geaflow.dsl.udf.table.agg.MinLong;
import org.apache.geaflow.dsl.udf.table.agg.PercentileDouble;
import org.apache.geaflow.dsl.udf.table.agg.PercentileInteger;
import org.apache.geaflow.dsl.udf.table.agg.PercentileLong;
import org.apache.geaflow.dsl.udf.table.agg.StdDevSampDouble;
import org.apache.geaflow.dsl.udf.table.agg.StdDevSampInteger;
import org.apache.geaflow.dsl.udf.table.agg.StdDevSampLong;
import org.apache.geaflow.dsl.udf.table.agg.SumDouble;
import org.apache.geaflow.dsl.udf.table.agg.SumInteger;
import org.apache.geaflow.dsl.udf.table.agg.SumLong;
import org.apache.geaflow.dsl.util.SqlTypeUtil;

public class PhysicAggregateRelNode extends Aggregate implements PhysicRelNode<RuntimeTable> {

    public static final String UDAF_COUNT = "COUNT";

    public static final String UDAF_SUM = "SUM";

    public static final String UDAF_AVG = "AVG";

    public static final String UDAF_MAX = "MAX";

    public static final String UDAF_MIN = "MIN";

    public static final String UDAF_STDDEV_SAMP = "STDDEV_SAMP";

    public static final String UDAF_PERCENTILE = "PERCENTILE";


    public PhysicAggregateRelNode(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        boolean indicator,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls) {
        super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public Aggregate copy(
        RelTraitSet traitSet,
        RelNode input,
        boolean indicator,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls) {
        return new PhysicAggregateRelNode(getCluster(), traitSet, input, indicator,
            groupSet, groupSets, aggCalls);
    }

    @Override
    public RuntimeTable translate(QueryContext context) {
        int[] keyFieldIndices = ArrayUtil.toIntArray(Lists.newArrayList(BitSets.toIter(groupSet)));
        IType<?>[] keyFieldTypes = getInputFieldTypes(keyFieldIndices);
        GroupByFunction groupByFn = new GroupByFunctionImpl(keyFieldIndices, keyFieldTypes);

        List<AggFunctionCall> aggFnCalls = buildAggFunctionCalls();
        List<IType<?>> aggOutputTypes = aggCalls.stream()
            .map(call -> SqlTypeUtil.convertType(call.getType()))
            .collect(Collectors.toList());
        AggFunction aggFn = new AggFunctionImpl(aggFnCalls, aggOutputTypes);

        // translate input node.
        RDataView dataView = ((PhysicRelNode<?>) getInput()).translate(context);

        if (dataView.getType() == ViewType.TABLE) {
            RuntimeTable runtimeTable = (RuntimeTable) dataView;
            return runtimeTable.aggregate(groupByFn, aggFn);
        } else if (dataView.getType() == ViewType.GRAPH) {
            RuntimeGraph runtimeGraph = (RuntimeGraph) dataView;
            return runtimeGraph.getPathTable().aggregate(groupByFn, aggFn);
        }
        throw new GeaFlowDSLException("DataView: " + dataView.getType() + " cannot support Aggregate");
    }

    private static class AggFunctionImpl implements AggFunction {

        private final AggFunctionCall[] aggFunctionCalls;

        private final IType<?>[] aggOutputTypes;

        private UDAF<Object, Object, Object>[] udafs;

        public AggFunctionImpl(List<AggFunctionCall> aggFunctionCalls, List<IType<?>> aggOutputTypes) {
            this.aggFunctionCalls = Objects.requireNonNull(aggFunctionCalls).toArray(new AggFunctionCall[]{});
            this.aggOutputTypes = Objects.requireNonNull(aggOutputTypes).toArray(new IType<?>[]{});
        }

        @SuppressWarnings("unchecked")
        @Override
        public void open(FunctionContext context) {
            udafs = new UDAF[aggFunctionCalls.length];
            for (int i = 0; i < aggFunctionCalls.length; i++) {
                try {
                    udafs[i] = (UDAF<Object, Object, Object>) aggFunctionCalls[i].getUdafClass().newInstance();
                    if (aggFunctionCalls[i].isDistinct) {
                        udafs[i] = new DistinctUDAF(udafs[i]);
                    }
                    udafs[i].open(context);
                } catch (InstantiationException | IllegalAccessException e) {
                    throw new GeaFlowDSLException("Error in create UDAF: " + aggFunctionCalls[i].getUdafClass());
                }
            }
        }

        @Override
        public Accumulator createAccumulator() {
            Object[] accumulators = new Object[udafs.length];
            for (int i = 0; i < accumulators.length; i++) {
                accumulators[i] = udafs[i].createAccumulator();
            }
            return new Accumulator(accumulators);
        }

        @Override
        public void add(Row row, Object accumulator) {
            for (int i = 0; i < udafs.length; i++) {
                AggFunctionCall aggInfo = aggFunctionCalls[i];
                Object argValue;
                if (aggInfo.argFieldIndices.length == 0) { // for count() without input parameter.
                    argValue = row;
                } else if (aggInfo.argFieldIndices.length == 1) {
                    argValue = row.getField(aggInfo.argFieldIndices[0], aggInfo.argFieldTypes[0]);
                } else { // for agg with multi-parameters
                    assert UDAFArguments.class.isAssignableFrom(aggInfo.udafInputClass);
                    Object[] parameters = new Object[aggInfo.argFieldIndices.length];
                    for (int p = 0; p < parameters.length; p++) {
                        parameters[p] = row.getField(aggInfo.argFieldIndices[p],
                            aggInfo.argFieldTypes[p]);
                    }
                    argValue = ClassUtil.newInstance(aggInfo.udafInputClass);
                    ((UDAFArguments) argValue).setParams(parameters);
                }
                udafs[i].accumulate(((Accumulator) accumulator).accumulators[i], argValue);
            }
        }

        @Override
        public void reset(Row row, Object accumulator) {
            Accumulator acc = (Accumulator) accumulator;
            for (int i = 0; i < acc.accumulators.length; i++) {
                udafs[i].resetAccumulator(acc.accumulators[i]);
            }
        }

        @Override
        public Row getValue(Object accumulator) {
            Object[] aggValues = new Object[udafs.length];
            for (int i = 0; i < udafs.length; i++) {
                aggValues[i] = udafs[i].getValue(((Accumulator) accumulator).accumulators[i]);
            }
            return ObjectRow.create(aggValues);
        }

        @Override
        public void merge(Object accA, Object accB) {
            for (int i = 0; i < udafs.length; i++) {
                udafs[i].merge(
                    ((Accumulator) accA).getAcc(i),
                    Arrays.asList(((Accumulator) accB).getAcc(i))
                );
            }
        }

        @Override
        public IType<?>[] getValueTypes() {
            return aggOutputTypes;
        }

        private static class Accumulator implements Serializable {

            private final Object[] accumulators;

            public Accumulator(Object[] accumulators) {
                this.accumulators = accumulators;
            }

            public Object getAcc(int index) {
                return accumulators[index];
            }
        }
    }

    /**
     * A wrapper for distinct aggregate function.
     */
    public static class DistinctUDAF extends UDAF<Object, Object, Object> {

        private final UDAF<Object, Object, Object> baseUDAF;

        public DistinctUDAF(UDAF<Object, Object, Object> baseUDAF) {
            this.baseUDAF = baseUDAF;
        }

        @Override
        public void open(FunctionContext context) {
            baseUDAF.open(context);
        }

        @Override
        public Object createAccumulator() {
            return new HashSet<>();
        }

        @SuppressWarnings("unchecked")
        @Override
        public void accumulate(Object accumulator, Object input) {
            if (input != null) {
                ((Set) accumulator).add(input);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void merge(Object accumulator, Iterable<Object> its) {
            Set<Object> setAcc = (Set<Object>) accumulator;
            for (Object it : its) {
                Set<Object> otherSet = (Set<Object>) it;
                setAcc.addAll(otherSet);
            }
        }

        @Override
        public void resetAccumulator(Object accumulator) {
            ((Set) accumulator).clear();
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object getValue(Object accumulator) {
            Set<Object> setAcc = (Set<Object>) accumulator;
            Object acc = baseUDAF.createAccumulator();
            for (Object input : setAcc) {
                baseUDAF.accumulate(acc, input);
            }
            return baseUDAF.getValue(acc);
        }
    }

    private IType<?>[] getInputFieldTypes(int[] fieldIndices) {
        RelDataType inputType = getInput().getRowType();
        IType<?>[] fieldTypes = new IType<?>[fieldIndices.length];

        for (int i = 0; i < fieldTypes.length; i++) {
            RelDataType relType = inputType.getFieldList().get(fieldIndices[i]).getType();
            IType<?> type = SqlTypeUtil.convertType(relType);
            fieldTypes[i] = type;
        }
        return fieldTypes;
    }

    private static class AggFunctionCall implements Serializable {

        /**
         * The name of the agg function, e.g. SUM, COUNT.
         */
        private final String name;

        /**
         * The argument field index.
         */
        private final int[] argFieldIndices;

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

        public AggFunctionCall(String name, int[] argFieldIndices, IType<?>[] argFieldTypes,
                               Class<? extends UDAF<?, ?, ?>> udafClass, boolean isDistinct) {
            this.name = name;
            this.argFieldIndices = argFieldIndices;
            this.argFieldTypes = argFieldTypes;
            this.udafClass = udafClass;
            Type[] genericTypes = getUDAFGenericTypes(udafClass);
            this.udafInputClass = (Class<?>) genericTypes[0];
            this.isDistinct = isDistinct;
        }

        public String getName() {
            return name;
        }

        public int[] getArgFieldIndices() {
            return argFieldIndices;
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

    private List<AggFunctionCall> buildAggFunctionCalls() {
        List<AggFunctionCall> aggFunctionCalls = new ArrayList<>();
        for (AggregateCall aggCall : aggCalls) {
            String name = aggCall.getName();
            int[] argFieldIndices = ArrayUtil.toIntArray(aggCall.getArgList());
            IType<?>[] argFieldTypes = getInputFieldTypes(argFieldIndices);
            Class<? extends UDAF<?, ?, ?>> udafClass = findUDAF(aggCall.getAggregation(), argFieldTypes);

            AggFunctionCall functionCall = new AggFunctionCall(name, argFieldIndices, argFieldTypes, udafClass,
                aggCall.isDistinct());
            aggFunctionCalls.add(functionCall);
        }
        return aggFunctionCalls;
    }

    public static Class<? extends UDAF<?, ?, ?>> findUDAF(SqlAggFunction aggFunction, IType<?>[] argFieldTypes) {
        List<Class<? extends UDAF<?, ?, ?>>> aggClasses = new ArrayList<>();
        String aggName = aggFunction.getName().toUpperCase(Locale.ROOT);
        // User-defined aggregate function
        if (aggFunction instanceof GeaFlowUserDefinedAggFunction) {
            GeaFlowUserDefinedAggFunction function = (GeaFlowUserDefinedAggFunction) aggFunction;
            aggClasses = function.getUdafClasses();
        } else {
            // Build-in aggregate function
            switch (aggName) {
                case UDAF_COUNT:
                    aggClasses.add(Count.class);
                    break;
                case UDAF_SUM:
                    aggClasses.add(SumLong.class);
                    aggClasses.add(SumDouble.class);
                    aggClasses.add(SumInteger.class);
                    break;
                case UDAF_AVG:
                    aggClasses.add(AvgDouble.class);
                    aggClasses.add(AvgLong.class);
                    aggClasses.add(AvgInteger.class);
                    break;
                case UDAF_MAX:
                    aggClasses.add(MaxLong.class);
                    aggClasses.add(MaxDouble.class);
                    aggClasses.add(MaxInteger.class);
                    aggClasses.add(MaxBinaryString.class);
                    break;
                case UDAF_MIN:
                    aggClasses.add(MinLong.class);
                    aggClasses.add(MinDouble.class);
                    aggClasses.add(MinInteger.class);
                    aggClasses.add(MinBinaryString.class);
                    break;
                case UDAF_STDDEV_SAMP:
                    aggClasses.add(StdDevSampLong.class);
                    aggClasses.add(StdDevSampDouble.class);
                    aggClasses.add(StdDevSampInteger.class);
                    break;
                case UDAF_PERCENTILE:
                    aggClasses.add(PercentileLong.class);
                    aggClasses.add(PercentileInteger.class);
                    aggClasses.add(PercentileDouble.class);
                    break;
                default:
                    throw new GeaFlowDSLException("Not support aggregate function " + aggName);
            }
        }

        Class<? extends UDAF<?, ?, ?>> aggClass;
        if (UDAF_COUNT.equals(aggName)) { // process case for count(*)
            assert aggClasses.size() == 1;
            aggClass = aggClasses.get(0);
        } else {
            List<Class<?>> argTypes = Arrays.stream(argFieldTypes)
                .map(IType::getTypeClass)
                .collect(Collectors.toList());
            // Find the best udaf implement class according to the argument types.
            aggClass = FunctionCallUtils.findMatchUDAF(aggName, aggClasses, argTypes);
        }
        return aggClass;
    }

    @Override
    public String showSQL() {
        StringBuilder sql = new StringBuilder();
        RelDataType inputRowType = getInput().getRowType();
        sql.append("group by ");
        boolean first = true;
        for (int groupByIndex : BitSets.toIter(groupSet)) {
            if (first) {
                first = false;
            } else {
                sql.append(",");
            }
            sql.append(inputRowType.getFieldNames().get(groupByIndex));
        }
        sql.append("\n");
        sql.append("aggFunction[ ");
        for (int i = 0; i < aggCalls.size(); i++) {
            if (i > 0) {
                sql.append(",");
            }
            AggregateCall call = aggCalls.get(i);
            String funcName = call.getAggregation().getName();
            sql.append(funcName).append("(");
            if (call.isDistinct()) {
                sql.append("distinct ");
            }
            for (int k = 0; k < call.getArgList().size(); k++) {
                if (k > 0) {
                    sql.append(",");
                }
                sql.append(
                    inputRowType.getFieldNames().get(call.getArgList().get(k)));
            }
            sql.append(")");
        }
        sql.append(" ]");
        return sql.toString();
    }
}
