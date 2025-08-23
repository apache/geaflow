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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.dsl.calcite.GraphRecordType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.PathType;
import org.apache.geaflow.dsl.common.types.VoidType;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.function.graph.FunctionSchemas;
import org.apache.geaflow.dsl.runtime.function.graph.MatchEdgeFunctionImpl;
import org.apache.geaflow.dsl.runtime.function.graph.MatchVertexFunctionImpl;
import org.apache.geaflow.dsl.runtime.function.graph.StepAggregateFunction;
import org.apache.geaflow.dsl.runtime.function.graph.StepFunction;
import org.apache.geaflow.dsl.runtime.traversal.DagGroupBuilder;
import org.apache.geaflow.dsl.runtime.traversal.DagTopology;
import org.apache.geaflow.dsl.runtime.traversal.DagTopologyGroup;
import org.apache.geaflow.dsl.runtime.traversal.StepLogicalPlan;
import org.apache.geaflow.dsl.runtime.traversal.StepLogicalPlanSet;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepCollector;
import org.apache.geaflow.dsl.runtime.traversal.data.SingleValue;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepSourceOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepSubQueryStartOperator;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.sqlnode.SqlMatchEdge.EdgeDirection;
import org.apache.geaflow.dsl.util.SqlTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DagTopologyGroupTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DagTopologyGroupTest.class);

    @Test
    public void testDagTopologyGroup() {
        StepLogicalPlan.clearCounter();
        StepLogicalPlan mainPlan =
            StepLogicalPlan.start()
                .withInputPathSchema(PathType.EMPTY)
                .withOutputPathSchema(PathType.EMPTY)
                .withOutputType(VoidType.INSTANCE)
                .withGraphSchema(createGraph())
                .vertexMatch(new MatchVertexFunctionImpl(Sets.newHashSet(BinaryString.fromString(
                    "person")), "a"))
                .withInputPathSchema(PathType.EMPTY)
                .withOutputPathSchema(PathType.EMPTY)
                .withOutputType(VoidType.INSTANCE)
                .withGraphSchema(createGraph())
                .edgeMatch(new MatchEdgeFunctionImpl(EdgeDirection.OUT, Sets.newHashSet(), "e"))
                .withInputPathSchema(PathType.EMPTY)
                .withOutputPathSchema(PathType.EMPTY)
                .withOutputType(VoidType.INSTANCE)
                .withGraphSchema(createGraph())
                .vertexMatch(new MatchVertexFunctionImpl(Sets.newHashSet(BinaryString.fromString(
                    "person")), "b"))
                .withInputPathSchema(PathType.EMPTY)
                .withOutputPathSchema(PathType.EMPTY)
                .withOutputType(VoidType.INSTANCE)
                .withGraphSchema(createGraph());

        StepLogicalPlan subPlan =
            StepLogicalPlan.subQueryStart("SubQuery1")
                .withInputPathSchema(PathType.EMPTY)
                .withOutputPathSchema(PathType.EMPTY)
                .withOutputType(VoidType.INSTANCE)
                .withGraphSchema(createGraph())
                .vertexMatch(new MatchVertexFunctionImpl(Sets.newHashSet(BinaryString.fromString(
                    "person")), "b"))
                .withInputPathSchema(PathType.EMPTY)
                .withOutputPathSchema(PathType.EMPTY)
                .withOutputType(VoidType.INSTANCE)
                .withGraphSchema(createGraph())
                .edgeMatch(new MatchEdgeFunctionImpl(EdgeDirection.OUT, Sets.newHashSet(), "e1"))
                .withInputPathSchema(PathType.EMPTY)
                .withOutputPathSchema(PathType.EMPTY)
                .withOutputType(VoidType.INSTANCE)
                .withGraphSchema(createGraph())
                .aggregate(new TestStepAggFunction());

        StepLogicalPlanSet logicalPlanSet = new StepLogicalPlanSet(mainPlan);
        logicalPlanSet.addSubLogicalPlan(subPlan);
        DagGroupBuilder builder = new DagGroupBuilder();
        DagTopologyGroup dagGroup = builder.buildDagGroup(logicalPlanSet);

        String planSetDesc = logicalPlanSet.getPlanSetDesc();
        LOGGER.info("Plan Desc:\n{}", planSetDesc);
        Assert.assertEquals(planSetDesc, "digraph G {\n" + "3 -> 10 [label= \"\"]\n"
            + "2 -> 3 [label= \"chain = false\"]\n" + "1 -> 2 [label= \"\"]\n"
            + "0 -> 1 [label= \"\"]\n" + "10 [label= \"StepEnd-10\"]\n"
            + "3 [label= \"MatchVertex-3 [b]\"]\n" + "2 [label= \"MatchEdge-2(OUT) [e]\"]\n"
            + "1 [label= \"MatchVertex-1 [a]\"]\n" + "0 [label= \"StepSource-0()\"]\n" + "\n"
            + "8 -> 9 [label= \"chain = false\"]\n" + "7 -> 8 [label= \"\"]\n"
            + "6 -> 7 [label= \"\"]\n" + "5 -> 6 [label= \"\"]\n" + "4 -> 5 [label= \"\"]\n"
            + "9 [label= \"StepGlobalSingleValueAggregate-9\"]\n"
            + "8 [label= \"StepExchange-8\"]\n" + "7 [label= \"StepLocalSingleValueAggregate-7\"]\n"
            + "6 [label= \"MatchEdge-6(OUT) [e1]\"]\n" + "5 [label= \"MatchVertex-5 [b]\"]\n"
            + "4 [label= \"StepSubQueryStart-4(name=SubQuery1)\"]\n" + "}");
        DagTopology mainDag = dagGroup.getMainDag();
        Assert.assertTrue(mainDag.getEntryOperator().getClass().isAssignableFrom(StepSourceOperator.class));
        Assert.assertTrue(mainDag.isChained(0, 1));
        Assert.assertTrue(mainDag.isChained(1, 2));
        Assert.assertFalse(mainDag.isChained(2, 3));
        Assert.assertTrue(mainDag.isChained(3, 10));

        DagTopology subDag = dagGroup.getSubDagTopologies().get(0);
        Assert.assertTrue(subDag.getEntryOperator().getClass().isAssignableFrom(StepSubQueryStartOperator.class));
        Assert.assertEquals(subDag.getInputIds(5), Lists.newArrayList(4L));
        Assert.assertEquals(subDag.getOutputIds(5), Lists.newArrayList(6L));
        Assert.assertEquals(subDag.getOutputIds(11), Lists.newArrayList());

        Assert.assertEquals(dagGroup.isChained(0, 1), mainDag.isChained(0, 1));
        Assert.assertEquals(dagGroup.isChained(4, 5), subDag.isChained(4, 5));
        Assert.assertEquals(dagGroup.getInputIds(4), subDag.getInputIds(4));
    }

    private GraphSchema createGraph() {
        GeaFlowGraph graph = new GeaFlowGraph("default", "test", new ArrayList<>(),
            new ArrayList<>(), new HashMap<>(), new HashMap<>(), false, false);
        GraphRecordType graphRecordType = (GraphRecordType) graph.getRowType(GQLJavaTypeFactory.create());
        return (GraphSchema) SqlTypeUtil.convertType(graphRecordType);
    }

    private static class TestStepAggFunction implements StepAggregateFunction {

        @Override
        public Object createAccumulator() {
            return null;
        }

        @Override
        public void add(Row row, Object accumulator) {

        }

        @Override
        public void merge(Object acc, Object otherAcc) {

        }

        @Override
        public SingleValue getValue(Object accumulator) {
            return null;
        }

        @Override
        public void open(TraversalRuntimeContext context, FunctionSchemas schemas) {

        }

        @Override
        public void finish(StepCollector<StepRecord> collector) {

        }

        @Override
        public List<Expression> getExpressions() {
            return Collections.emptyList();
        }

        @Override
        public StepFunction copy(List<Expression> expressions) {
            return new TestStepAggFunction();
        }
    }
}
