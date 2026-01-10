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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.function.UDAF;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.schema.GeaFlowFunction;
import org.apache.geaflow.dsl.udf.graph.AllSourceShortestPath;
import org.apache.geaflow.dsl.udf.graph.ClosenessCentrality;
import org.apache.geaflow.dsl.udf.graph.ClusterCoefficient;
import org.apache.geaflow.dsl.udf.graph.CommonNeighbors;
import org.apache.geaflow.dsl.udf.graph.ConnectedComponents;
import org.apache.geaflow.dsl.udf.graph.IncKHopAlgorithm;
import org.apache.geaflow.dsl.udf.graph.IncMinimumSpanningTree;
import org.apache.geaflow.dsl.udf.graph.IncWeakConnectedComponents;
import org.apache.geaflow.dsl.udf.graph.IncrementalKCore;
import org.apache.geaflow.dsl.udf.graph.JaccardSimilarity;
import org.apache.geaflow.dsl.udf.graph.JaccardSimilarityAlgorithm;
import org.apache.geaflow.dsl.udf.graph.KCore;
import org.apache.geaflow.dsl.udf.graph.KHop;
import org.apache.geaflow.dsl.udf.graph.LabelPropagation;
import org.apache.geaflow.dsl.udf.graph.PageRank;
import org.apache.geaflow.dsl.udf.graph.SingleSourceShortestPath;
import org.apache.geaflow.dsl.udf.graph.TriangleCount;
import org.apache.geaflow.dsl.udf.graph.WeakConnectedComponents;
import org.apache.geaflow.dsl.udf.table.agg.PercentileDouble;
import org.apache.geaflow.dsl.udf.table.agg.PercentileInteger;
import org.apache.geaflow.dsl.udf.table.agg.PercentileLong;
import org.apache.geaflow.dsl.udf.table.array.ArrayAppend;
import org.apache.geaflow.dsl.udf.table.array.ArrayContains;
import org.apache.geaflow.dsl.udf.table.array.ArrayDistinct;
import org.apache.geaflow.dsl.udf.table.array.ArrayUnion;
import org.apache.geaflow.dsl.udf.table.date.AddMonths;
import org.apache.geaflow.dsl.udf.table.date.DateAdd;
import org.apache.geaflow.dsl.udf.table.date.DateDiff;
import org.apache.geaflow.dsl.udf.table.date.DateFormat;
import org.apache.geaflow.dsl.udf.table.date.DatePart;
import org.apache.geaflow.dsl.udf.table.date.DateSub;
import org.apache.geaflow.dsl.udf.table.date.DateTrunc;
import org.apache.geaflow.dsl.udf.table.date.Day;
import org.apache.geaflow.dsl.udf.table.date.DayOfMonth;
import org.apache.geaflow.dsl.udf.table.date.FromUnixTime;
import org.apache.geaflow.dsl.udf.table.date.FromUnixTimeMillis;
import org.apache.geaflow.dsl.udf.table.date.Hour;
import org.apache.geaflow.dsl.udf.table.date.IsDate;
import org.apache.geaflow.dsl.udf.table.date.LastDay;
import org.apache.geaflow.dsl.udf.table.date.Minute;
import org.apache.geaflow.dsl.udf.table.date.Month;
import org.apache.geaflow.dsl.udf.table.date.Now;
import org.apache.geaflow.dsl.udf.table.date.Second;
import org.apache.geaflow.dsl.udf.table.date.UnixTimeStamp;
import org.apache.geaflow.dsl.udf.table.date.UnixTimeStampMillis;
import org.apache.geaflow.dsl.udf.table.date.WeekDay;
import org.apache.geaflow.dsl.udf.table.date.WeekOfYear;
import org.apache.geaflow.dsl.udf.table.date.Year;
import org.apache.geaflow.dsl.udf.table.math.E;
import org.apache.geaflow.dsl.udf.table.math.Log2;
import org.apache.geaflow.dsl.udf.table.math.Round;
import org.apache.geaflow.dsl.udf.table.other.Direction;
import org.apache.geaflow.dsl.udf.table.other.EdgeSrcId;
import org.apache.geaflow.dsl.udf.table.other.EdgeTargetId;
import org.apache.geaflow.dsl.udf.table.other.EdgeTimestamp;
import org.apache.geaflow.dsl.udf.table.other.If;
import org.apache.geaflow.dsl.udf.table.other.IsDecimal;
import org.apache.geaflow.dsl.udf.table.other.IsDestinationOf;
import org.apache.geaflow.dsl.udf.table.other.IsNotDestinationOf;
import org.apache.geaflow.dsl.udf.table.other.IsNotSourceOf;
import org.apache.geaflow.dsl.udf.table.other.IsSourceOf;
import org.apache.geaflow.dsl.udf.table.other.Label;
import org.apache.geaflow.dsl.udf.table.other.PropertyExists;
import org.apache.geaflow.dsl.udf.table.other.VertexId;
import org.apache.geaflow.dsl.udf.table.string.Ascii2String;
import org.apache.geaflow.dsl.udf.table.string.Base64Decode;
import org.apache.geaflow.dsl.udf.table.string.Base64Encode;
import org.apache.geaflow.dsl.udf.table.string.Concat;
import org.apache.geaflow.dsl.udf.table.string.ConcatWS;
import org.apache.geaflow.dsl.udf.table.string.GetJsonObject;
import org.apache.geaflow.dsl.udf.table.string.Hash;
import org.apache.geaflow.dsl.udf.table.string.IndexOf;
import org.apache.geaflow.dsl.udf.table.string.Instr;
import org.apache.geaflow.dsl.udf.table.string.IsBlank;
import org.apache.geaflow.dsl.udf.table.string.KeyValue;
import org.apache.geaflow.dsl.udf.table.string.LTrim;
import org.apache.geaflow.dsl.udf.table.string.Length;
import org.apache.geaflow.dsl.udf.table.string.Like;
import org.apache.geaflow.dsl.udf.table.string.RTrim;
import org.apache.geaflow.dsl.udf.table.string.RegExp;
import org.apache.geaflow.dsl.udf.table.string.RegExpExtract;
import org.apache.geaflow.dsl.udf.table.string.RegExpReplace;
import org.apache.geaflow.dsl.udf.table.string.RegexpCount;
import org.apache.geaflow.dsl.udf.table.string.Repeat;
import org.apache.geaflow.dsl.udf.table.string.Replace;
import org.apache.geaflow.dsl.udf.table.string.Reverse;
import org.apache.geaflow.dsl.udf.table.string.Space;
import org.apache.geaflow.dsl.udf.table.string.SplitEx;
import org.apache.geaflow.dsl.udf.table.string.Substr;
import org.apache.geaflow.dsl.udf.table.string.UrlDecode;
import org.apache.geaflow.dsl.udf.table.string.UrlEncode;
import org.apache.geaflow.dsl.util.FunctionUtil;

/**
 * SQL build-in {@link SqlFunction}s.
 */
public class BuildInSqlFunctionTable extends ListSqlOperatorTable {

    private final GQLJavaTypeFactory typeFactory;

    private final ImmutableList<GeaFlowFunction> buildInSqlFunctions =
        new ImmutableList.Builder<GeaFlowFunction>()
            // ~ build in UDF --------------------------------
            // udf.table.date
            .add(GeaFlowFunction.of(AddMonths.class))
            .add(GeaFlowFunction.of(DateAdd.class))
            .add(GeaFlowFunction.of(DateDiff.class))
            .add(GeaFlowFunction.of(DateFormat.class))
            .add(GeaFlowFunction.of(DatePart.class))
            .add(GeaFlowFunction.of(DateSub.class))
            .add(GeaFlowFunction.of(DateTrunc.class))
            .add(GeaFlowFunction.of(Day.class))
            .add(GeaFlowFunction.of(DayOfMonth.class))
            .add(GeaFlowFunction.of(FromUnixTime.class))
            .add(GeaFlowFunction.of(FromUnixTimeMillis.class))
            .add(GeaFlowFunction.of(Hour.class))
            .add(GeaFlowFunction.of(IsDate.class))
            .add(GeaFlowFunction.of(LastDay.class))
            .add(GeaFlowFunction.of(Minute.class))
            .add(GeaFlowFunction.of(Month.class))
            .add(GeaFlowFunction.of(Now.class))
            .add(GeaFlowFunction.of(Second.class))
            .add(GeaFlowFunction.of(UnixTimeStamp.class))
            .add(GeaFlowFunction.of(UnixTimeStampMillis.class))
            .add(GeaFlowFunction.of(WeekDay.class))
            .add(GeaFlowFunction.of(WeekOfYear.class))
            .add(GeaFlowFunction.of(Year.class))

            // udf.table.array
            .add(GeaFlowFunction.of(ArrayAppend.class))
            .add(GeaFlowFunction.of(ArrayContains.class))
            .add(GeaFlowFunction.of(ArrayDistinct.class))
            .add(GeaFlowFunction.of(ArrayUnion.class))

            // udf.table.math
            .add(GeaFlowFunction.of(E.class))
            .add(GeaFlowFunction.of(Log2.class))
            .add(GeaFlowFunction.of(Round.class))

            // udf.table.string
            .add(GeaFlowFunction.of(Ascii2String.class))
            .add(GeaFlowFunction.of(Base64Decode.class))
            .add(GeaFlowFunction.of(Base64Encode.class))
            .add(GeaFlowFunction.of(Concat.class))
            .add(GeaFlowFunction.of(ConcatWS.class))
            .add(GeaFlowFunction.of(Hash.class))
            .add(GeaFlowFunction.of(IndexOf.class))
            .add(GeaFlowFunction.of(Instr.class))
            .add(GeaFlowFunction.of(IsBlank.class))
            .add(GeaFlowFunction.of(KeyValue.class))
            .add(GeaFlowFunction.of(Length.class))
            .add(GeaFlowFunction.of(Like.class))
            .add(GeaFlowFunction.of(LTrim.class))
            .add(GeaFlowFunction.of(RegExp.class))
            .add(GeaFlowFunction.of(RegexpCount.class))
            .add(GeaFlowFunction.of(RegExpExtract.class))
            .add(GeaFlowFunction.of(RegExpReplace.class))
            .add(GeaFlowFunction.of(Repeat.class))
            .add(GeaFlowFunction.of(Replace.class))
            .add(GeaFlowFunction.of(Reverse.class))
            .add(GeaFlowFunction.of(RTrim.class))
            .add(GeaFlowFunction.of(Space.class))
            .add(GeaFlowFunction.of(SplitEx.class))
            .add(GeaFlowFunction.of(Substr.class))
            .add(GeaFlowFunction.of(UrlDecode.class))
            .add(GeaFlowFunction.of(UrlEncode.class))
            .add(GeaFlowFunction.of(GetJsonObject.class))

            // udf.table.other
            .add(GeaFlowFunction.of(If.class))
            .add(GeaFlowFunction.of(Direction.class))
            .add(GeaFlowFunction.of(Label.class))
            .add(GeaFlowFunction.of(VertexId.class))
            .add(GeaFlowFunction.of(EdgeSrcId.class))
            .add(GeaFlowFunction.of(EdgeTargetId.class))
            .add(GeaFlowFunction.of(EdgeTimestamp.class))
            .add(GeaFlowFunction.of(IsDecimal.class))
            // ISO-GQL source/destination predicates
            .add(GeaFlowFunction.of(IsSourceOf.class))
            .add(GeaFlowFunction.of(IsNotSourceOf.class))
            .add(GeaFlowFunction.of(IsDestinationOf.class))
            .add(GeaFlowFunction.of(IsNotDestinationOf.class))
            // ISO-GQL property exists predicate
            .add(GeaFlowFunction.of(PropertyExists.class))
            // UDAF
            .add(GeaFlowFunction.of(PercentileLong.class))
            .add(GeaFlowFunction.of(PercentileInteger.class))
            .add(GeaFlowFunction.of(PercentileDouble.class))
            // UDGA
            .add(GeaFlowFunction.of(SingleSourceShortestPath.class))
            .add(GeaFlowFunction.of(AllSourceShortestPath.class))
            .add(GeaFlowFunction.of(PageRank.class))
            .add(GeaFlowFunction.of(KHop.class))
            .add(GeaFlowFunction.of(KCore.class))
            .add(GeaFlowFunction.of(IncrementalKCore.class))
            .add(GeaFlowFunction.of(IncMinimumSpanningTree.class))
            .add(GeaFlowFunction.of(ClosenessCentrality.class))
            .add(GeaFlowFunction.of(WeakConnectedComponents.class))
            .add(GeaFlowFunction.of(TriangleCount.class))
            .add(GeaFlowFunction.of(ClusterCoefficient.class))
            .add(GeaFlowFunction.of(IncWeakConnectedComponents.class))
            .add(GeaFlowFunction.of(CommonNeighbors.class))
            .add(GeaFlowFunction.of(JaccardSimilarityAlgorithm.class))
            .add(GeaFlowFunction.of(JaccardSimilarity.class))
            .add(GeaFlowFunction.of(IncKHopAlgorithm.class))
            .add(GeaFlowFunction.of(LabelPropagation.class))
            .add(GeaFlowFunction.of(ConnectedComponents.class))
            .build();

    public BuildInSqlFunctionTable(GQLJavaTypeFactory typeFactory) {
        this.typeFactory = typeFactory;
        this.register();
    }

    private void register() {
        for (GeaFlowFunction function : functions()) {
            SqlFunction sqlFunction;
            try {
                sqlFunction = FunctionUtil.createSqlFunction(function, typeFactory);
            } catch (GeaFlowDSLException e) {
                throw new GeaFlowDSLException(
                    "Error in register SqlFunction " + function, e);
            }
            super.add(sqlFunction);
        }
    }

    public void registerFunction(Class<?> functionClass) {
        try {
            SqlFunction sqlFunction = FunctionUtil.createSqlFunction(
                GeaFlowFunction.of(functionClass), typeFactory);
            super.add(sqlFunction);
        } catch (GeaFlowDSLException e) {
            throw new GeaFlowDSLException(
                "Error in register SqlFunction " + functionClass, e);
        }
    }

    private List<GeaFlowFunction> functions() {
        Map<String, GeaFlowFunction> combines = new HashMap<>();
        for (GeaFlowFunction function : buildInSqlFunctions) {
            GeaFlowFunction old = combines.get(function.getName());
            if (old == null) {
                old = function;
            } else {
                // As UDAF, for example:'max', it has multiple class impl.
                // MaxLong
                // MaxDouble
                // MaxString
                if (isUDAF(function)) {
                    List<String> clazz = old.getClazz();
                    clazz.addAll(function.getClazz());
                    old = GeaFlowFunction.of(function.getName(), clazz);
                } else {
                    throw new RuntimeException("function " + function.getName()
                        + " cannot have more than one implement class");
                }
            }

            String name = old.getName();
            combines.put(name, old);
        }
        return Lists.newArrayList(combines.values());
    }


    private boolean isUDAF(GeaFlowFunction function) {
        try {
            String reflectClassName = function.getClazz().get(0);
            Class<?> reflectClass = Thread.currentThread().getContextClassLoader()
                .loadClass(reflectClassName);
            return UDAF.class.isAssignableFrom(reflectClass);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void lookupOperatorOverloads(SqlIdentifier opName,
                                        SqlFunctionCategory category,
                                        SqlSyntax syntax,
                                        List<SqlOperator> operatorList) {
        for (SqlOperator operator : getOperatorList()) {
            if (!opName.isSimple()
                || !operator.getName().equalsIgnoreCase(opName.getSimple())) {
                continue;
            }
            if (operator.getSyntax() != syntax) {
                continue;
            }
            operatorList.add(operator);
        }
    }
}
