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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.RDataView;
import org.apache.geaflow.dsl.runtime.RDataView.ViewType;
import org.apache.geaflow.dsl.runtime.RuntimeGraph;
import org.apache.geaflow.dsl.runtime.RuntimeTable;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.expression.ExpressionTranslator;
import org.apache.geaflow.dsl.runtime.expression.UDTFExpression;
import org.apache.geaflow.dsl.runtime.function.table.CorrelateFunction;
import org.apache.geaflow.dsl.runtime.function.table.CorrelateFunctionImpl;
import org.apache.geaflow.dsl.util.ExpressionUtil;
import org.apache.geaflow.dsl.util.GQLRelUtil;
import org.apache.geaflow.dsl.util.SqlTypeUtil;

public class PhysicCorrelateRelNode extends Correlate implements PhysicRelNode<RuntimeTable> {

    public PhysicCorrelateRelNode(RelOptCluster cluster,
                                  RelTraitSet traits,
                                  RelNode left,
                                  RelNode right,
                                  CorrelationId correlationId,
                                  ImmutableBitSet requiredColumns,
                                  SemiJoinType joinType) {
        super(cluster, traits, left, right, correlationId, requiredColumns, joinType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public Correlate copy(RelTraitSet traitSet,
                          RelNode left,
                          RelNode right,
                          CorrelationId correlationId,
                          ImmutableBitSet requiredColumns,
                          SemiJoinType joinType) {
        return new PhysicCorrelateRelNode(
            super.getCluster(),
            traitSet,
            left,
            right,
            correlationId,
            requiredColumns,
            joinType);
    }

    @Override
    public RuntimeTable translate(QueryContext context) {
        PhysicRelNode rightInput = ((PhysicRelNode<?>) getInput(1));
        RelNode right = GQLRelUtil.toRel(rightInput);
        Filter filterRelNode = null;
        while (!(right instanceof TableFunctionScan)) {
            Preconditions.checkArgument(rightInput.getInputs().size() == 1);
            Preconditions.checkArgument(filterRelNode == null);
            Preconditions.checkArgument(rightInput instanceof Filter);
            filterRelNode = (Filter) right;
            right = GQLRelUtil.toRel(right.getInput(0));
        }

        Preconditions.checkArgument(right instanceof PhysicTableFunctionScanRelNode);
        Expression tableExpression = ExpressionTranslator.of(right.getRowType())
            .translate(((PhysicTableFunctionScanRelNode) right).getCall());
        Preconditions.checkArgument(tableExpression instanceof UDTFExpression);
        UDTFExpression udtfExpression = (UDTFExpression) tableExpression;
        List<IType<?>> correlateLeftOutputTypes = getLeft().getRowType().getFieldList().stream()
            .map(field -> SqlTypeUtil.convertType(field.getType())).collect(Collectors.toList());
        List<IType<?>> correlateRightOutputTypes = getRight().getRowType().getFieldList().stream()
            .map(field -> SqlTypeUtil.convertType(field.getType())).collect(Collectors.toList());

        Expression condition = null;
        if (filterRelNode != null) {
            ExpressionTranslator translator = ExpressionTranslator.of(this.getRowType());
            condition = translator.translate(filterRelNode.getCondition());
        }
        CorrelateFunction correlateFunction = new CorrelateFunctionImpl(udtfExpression,
            condition, correlateLeftOutputTypes, correlateRightOutputTypes);
        RDataView input = ((PhysicRelNode<?>) getInput(0)).translate(context);
        if (input.getType() == ViewType.TABLE) {
            return ((RuntimeTable) input).correlate(correlateFunction);
        } else if (input.getType() == ViewType.GRAPH) {
            RuntimeGraph runtimeGraph = (RuntimeGraph) input;
            return runtimeGraph.getPathTable().correlate(correlateFunction);
        }
        throw new GeaFlowDSLException("DataView: " + input.getType() + " cannot support "
            + "correlate");
    }

    @SuppressWarnings("unchecked")
    @Override
    public String showSQL() {
        StringBuilder sql = new StringBuilder();

        PhysicRelNode<RuntimeTable> left = (PhysicRelNode<RuntimeTable>) getLeft();
        PhysicRelNode<RuntimeTable> right = (PhysicRelNode<RuntimeTable>) getRight();
        if (left instanceof TableScan) {
            String tableName = Joiner.on('.').join(left.getTable().getQualifiedName());
            sql.append(tableName);
        } else {
            sql.append("SubQuery[").append(left.showSQL()).append("]");
        }
        sql.append(" ").append(joinType.name().toLowerCase()).append(" join ");

        if (right instanceof TableFunctionScan) {
            TableFunctionScan tableFunctionScan = (TableFunctionScan) right;
            RexNode call = tableFunctionScan.getCall();
            sql.append(ExpressionUtil.showExpression(call, null, left.getRowType()));
        } else {
            sql.append(right.showSQL());
        }
        return sql.toString();
    }
}
