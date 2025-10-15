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

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.util.TypeCastUtil;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.RDataView;
import org.apache.geaflow.dsl.runtime.RDataView.ViewType;
import org.apache.geaflow.dsl.runtime.RuntimeGraph;
import org.apache.geaflow.dsl.runtime.RuntimeTable;
import org.apache.geaflow.dsl.runtime.expression.ExpressionTranslator;
import org.apache.geaflow.dsl.runtime.expression.field.FieldExpression;
import org.apache.geaflow.dsl.runtime.function.table.OrderByFunction;
import org.apache.geaflow.dsl.runtime.function.table.OrderByHeapSort;
import org.apache.geaflow.dsl.runtime.function.table.OrderByRadixSort;
import org.apache.geaflow.dsl.runtime.function.table.OrderByTimSort;
import org.apache.geaflow.dsl.runtime.function.table.order.OrderByField;
import org.apache.geaflow.dsl.runtime.function.table.order.OrderByField.ORDER;
import org.apache.geaflow.dsl.runtime.function.table.order.SortInfo;
import org.apache.geaflow.dsl.util.SqlTypeUtil;

public class PhysicSortRelNode extends Sort implements PhysicRelNode<RuntimeTable> {

    public PhysicSortRelNode(RelOptCluster cluster,
                             RelTraitSet traits,
                             RelNode child,
                             RelCollation collation,
                             RexNode offset,
                             RexNode fetch) {
        super(cluster, traits, child, collation, offset, fetch);
    }

    @Override
    public Sort copy(RelTraitSet traitSet,
                     RelNode newInput,
                     RelCollation newCollation,
                     RexNode offset,
                     RexNode fetch) {
        return new PhysicSortRelNode(super.getCluster(), traitSet, newInput,
            newCollation, offset, fetch);
    }

    @Override
    public RuntimeTable translate(QueryContext context) {
        SortInfo sortInfo = buildSortInfo();
        RDataView dataView = ((PhysicRelNode<?>) getInput()).translate(context);
        
        OrderByFunction orderByFunction;
        if (sortInfo.fetch > 0) {
            orderByFunction = new OrderByHeapSort(sortInfo);
        } else if (sortInfo.isRadixSortable()) {
            orderByFunction = new OrderByRadixSort(sortInfo);
        } else {
            orderByFunction = new OrderByTimSort(sortInfo);
        }
        if (dataView.getType() == ViewType.TABLE) {
            return ((RuntimeTable) dataView).orderBy(orderByFunction);
        } else {
            assert dataView instanceof RuntimeGraph;
            RuntimeGraph runtimeGraph = (RuntimeGraph) dataView;
            return runtimeGraph.getPathTable().orderBy(orderByFunction);
        }
    }

    @Override
    public String showSQL() {
        StringBuilder sql = new StringBuilder();
        sql.append("Order By ");
        collation.getFieldCollations().forEach(fc -> {
            String name = getInput().getRowType().getFieldNames().get(fc.getFieldIndex());
            sql.append(name).append(" ").append(fc.direction.shortString);
        });
        return sql.toString();
    }

    private SortInfo buildSortInfo() {
        SortInfo sortInfo = new SortInfo();
        for (RelFieldCollation fc : collation.getFieldCollations()) {
            List<RelDataTypeField> fieldList = getRowType().getFieldList();
            IType<?> fieldType = SqlTypeUtil.convertType(fieldList.get(fc.getFieldIndex()).getType());

            OrderByField orderByField = new OrderByField();
            orderByField.expression = new FieldExpression(fc.getFieldIndex(), fieldType);
            switch (fc.getDirection()) {
                case ASCENDING:
                    orderByField.order = ORDER.ASC;
                    break;
                case DESCENDING:
                    orderByField.order = ORDER.DESC;
                    break;
                default:
                    throw new UnsupportedOperationException(
                        "UnSupport sort type: " + fc.getDirection());
            }
            sortInfo.orderByFields.add(orderByField);
        }
        ExpressionTranslator translator = ExpressionTranslator.of(getInput().getRowType());
        sortInfo.fetch = fetch == null ? -1 :
            (int) TypeCastUtil.cast(
                translator.translate(fetch).evaluate(null),
                Integer.class);
        return sortInfo;
    }
}
