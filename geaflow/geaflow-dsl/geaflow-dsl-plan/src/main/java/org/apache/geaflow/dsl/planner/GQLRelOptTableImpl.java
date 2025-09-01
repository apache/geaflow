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

package org.apache.geaflow.dsl.planner;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Table;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.types.VertexType;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.EdgeTable;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.VertexTable;

public class GQLRelOptTableImpl extends RelOptTableImpl {

    protected GQLRelOptTableImpl(RelOptSchema schema,
                                 RelDataType rowType,
                                 List<String> names, Table table,
                                 Function<Class, Expression> expressionFunction,
                                 Double rowCount) {
        super(schema, rowType, names, table, expressionFunction, rowCount);
    }

    public static RelOptTableImpl create(RelOptSchema schema,
                                         RelDataType rowType, Table table, ImmutableList<String> names) {
        return new GQLRelOptTableImpl(schema, rowType, names, table, null, null);
    }

    @Override
    public List<ColumnStrategy> getColumnStrategies() {
        List<ColumnStrategy> columnStrategies = super.getColumnStrategies();
        if (table instanceof VertexTable) {
            List<ColumnStrategy> vertexColumnStrategies = new ArrayList<>(columnStrategies);
            vertexColumnStrategies.set(VertexType.LABEL_FIELD_POSITION, ColumnStrategy.VIRTUAL);
            return vertexColumnStrategies;
        } else if (table instanceof EdgeTable) {
            List<ColumnStrategy> edgeColumnStrategies = new ArrayList<>(columnStrategies);
            edgeColumnStrategies.set(EdgeType.LABEL_FIELD_POSITION, ColumnStrategy.VIRTUAL);
            return edgeColumnStrategies;
        }
        return columnStrategies;
    }
}
