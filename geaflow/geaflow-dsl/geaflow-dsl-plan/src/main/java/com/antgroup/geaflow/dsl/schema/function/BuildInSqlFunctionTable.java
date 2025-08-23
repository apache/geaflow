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

package com.antgroup.geaflow.dsl.schema.function;

import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.function.UDAF;
import com.antgroup.geaflow.dsl.planner.GQLJavaTypeFactory;
import com.antgroup.geaflow.dsl.schema.GeaFlowFunction;
import com.antgroup.geaflow.dsl.udf.graph.AllSourceShortestPath;
import com.antgroup.geaflow.dsl.udf.graph.ClosenessCentrality;
import com.antgroup.geaflow.dsl.udf.graph.CommonNeighbors;
import com.antgroup.geaflow.dsl.udf.graph.IncKHopAlgorithm;
import com.antgroup.geaflow.dsl.udf.graph.IncMinimumSpanningTree;
import com.antgroup.geaflow.dsl.udf.graph.IncWeakConnectedComponents;
import com.antgroup.geaflow.dsl.udf.graph.KCore;
import com.antgroup.geaflow.dsl.udf.graph.KHop;
import com.antgroup.geaflow.dsl.udf.graph.PageRank;
import com.antgroup.geaflow.dsl.udf.graph.SingleSourceShortestPath;
import com.antgroup.geaflow.dsl.udf.graph.TriangleCount;
import com.antgroup.geaflow.dsl.udf.graph.WeakConnectedComponents;
import com.antgroup.geaflow.dsl.udf.table.agg.PercentileDouble;
import com.antgroup.geaflow.dsl.udf.table.agg.PercentileInteger;
import com.antgroup.geaflow.dsl.udf.table.agg.PercentileLong;
import com.antgroup.geaflow.dsl.udf.table.array.ArrayAppend;
import com.antgroup.geaflow.dsl.udf.table.array.ArrayContains;
import com.antgroup.geaflow.dsl.udf.table.array.ArrayDistinct;
import com.antgroup.geaflow.dsl.udf.table.array.ArrayUnion;
import com.antgroup.geaflow.dsl.udf.table.date.AddMonths;
import com.antgroup.geaflow.dsl.udf.table.date.DateAdd;
import com.antgroup.geaflow.dsl.udf.table.date.DateDiff;
import com.antgroup.geaflow.dsl.udf.table.date.DateFormat;
import com.antgroup.geaflow.dsl.udf.table.date.DatePart;
import com.antgroup.geaflow.dsl.udf.table.date.DateSub;
import com.antgroup.geaflow.dsl.udf.table.date.DateTrunc;
import com.antgroup.geaflow.dsl.udf.table.date.Day;
import com.antgroup.geaflow.dsl.udf.table.date.DayOfMonth;
import com.antgroup.geaflow.dsl.udf.table.date.FromUnixTime;
import com.antgroup.geaflow.dsl.udf.table.date.FromUnixTimeMillis;
import com.antgroup.geaflow.dsl.udf.table.date.Hour;
import com.antgroup.geaflow.dsl.udf.table.date.IsDate;
import com.antgroup.geaflow.dsl.udf.table.date.LastDay;
import com.antgroup.geaflow.dsl.udf.table.date.Minute;
import com.antgroup.geaflow.dsl.udf.table.date.Month;
import com.antgroup.geaflow.dsl.udf.table.date.Now;
import com.antgroup.geaflow.dsl.udf.table.date.Second;
import com.antgroup.geaflow.dsl.udf.table.date.UnixTimeStamp;
import com.antgroup.geaflow.dsl.udf.table.date.UnixTimeStampMillis;
import com.antgroup.geaflow.dsl.udf.table.date.WeekDay;
import com.antgroup.geaflow.dsl.udf.table.date.WeekOfYear;
import com.antgroup.geaflow.dsl.udf.table.date.Year;
import com.antgroup.geaflow.dsl.udf.table.math.E;
import com.antgroup.geaflow.dsl.udf.table.math.Log2;
import com.antgroup.geaflow.dsl.udf.table.math.Round;
import com.antgroup.geaflow.dsl.udf.table.other.EdgeSrcId;
import com.antgroup.geaflow.dsl.udf.table.other.EdgeTargetId;
import com.antgroup.geaflow.dsl.udf.table.other.EdgeTimestamp;
import com.antgroup.geaflow.dsl.udf.table.other.If;
import com.antgroup.geaflow.dsl.udf.table.other.IsDecimal;
import com.antgroup.geaflow.dsl.udf.table.other.Label;
import com.antgroup.geaflow.dsl.udf.table.other.VertexId;
import com.antgroup.geaflow.dsl.udf.table.string.Ascii2String;
import com.antgroup.geaflow.dsl.udf.table.string.Base64Decode;
import com.antgroup.geaflow.dsl.udf.table.string.Base64Encode;
import com.antgroup.geaflow.dsl.udf.table.string.Concat;
import com.antgroup.geaflow.dsl.udf.table.string.ConcatWS;
import com.antgroup.geaflow.dsl.udf.table.string.GetJsonObject;
import com.antgroup.geaflow.dsl.udf.table.string.Hash;
import com.antgroup.geaflow.dsl.udf.table.string.IndexOf;
import com.antgroup.geaflow.dsl.udf.table.string.Instr;
import com.antgroup.geaflow.dsl.udf.table.string.IsBlank;
import com.antgroup.geaflow.dsl.udf.table.string.KeyValue;
import com.antgroup.geaflow.dsl.udf.table.string.LTrim;
import com.antgroup.geaflow.dsl.udf.table.string.Length;
import com.antgroup.geaflow.dsl.udf.table.string.Like;
import com.antgroup.geaflow.dsl.udf.table.string.RTrim;
import com.antgroup.geaflow.dsl.udf.table.string.RegExp;
import com.antgroup.geaflow.dsl.udf.table.string.RegExpExtract;
import com.antgroup.geaflow.dsl.udf.table.string.RegExpReplace;
import com.antgroup.geaflow.dsl.udf.table.string.RegexpCount;
import com.antgroup.geaflow.dsl.udf.table.string.Repeat;
import com.antgroup.geaflow.dsl.udf.table.string.Replace;
import com.antgroup.geaflow.dsl.udf.table.string.Reverse;
import com.antgroup.geaflow.dsl.udf.table.string.Space;
import com.antgroup.geaflow.dsl.udf.table.string.SplitEx;
import com.antgroup.geaflow.dsl.udf.table.string.Substr;
import com.antgroup.geaflow.dsl.udf.table.string.UrlDecode;
import com.antgroup.geaflow.dsl.udf.table.string.UrlEncode;
import com.antgroup.geaflow.dsl.util.FunctionUtil;
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
            .add(GeaFlowFunction.of(Label.class))
            .add(GeaFlowFunction.of(VertexId.class))
            .add(GeaFlowFunction.of(EdgeSrcId.class))
            .add(GeaFlowFunction.of(EdgeTargetId.class))
            .add(GeaFlowFunction.of(EdgeTimestamp.class))
            .add(GeaFlowFunction.of(IsDecimal.class))
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
            .add(GeaFlowFunction.of(ClosenessCentrality.class))
            .add(GeaFlowFunction.of(WeakConnectedComponents.class))
            .add(GeaFlowFunction.of(TriangleCount.class))
            .add(GeaFlowFunction.of(IncWeakConnectedComponents.class))
            .add(GeaFlowFunction.of(CommonNeighbors.class))
            .add(GeaFlowFunction.of(IncKHopAlgorithm.class))
            .add(GeaFlowFunction.of(IncMinimumSpanningTree.class))
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
                    old =  GeaFlowFunction.of(function.getName(), clazz);
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
