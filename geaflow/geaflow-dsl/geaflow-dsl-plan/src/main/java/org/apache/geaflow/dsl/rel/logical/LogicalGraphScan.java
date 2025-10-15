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

package org.apache.geaflow.dsl.rel.logical;

import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.geaflow.dsl.calcite.GraphRecordType;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.rel.GraphScan;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;

public class LogicalGraphScan extends GraphScan {

    protected LogicalGraphScan(RelOptCluster cluster, RelTraitSet traitSet,
                               RelOptTable table) {
        super(cluster, traitSet, table);
    }

    @Override
    public GraphScan copy(RelTraitSet traitSet, RelOptTable table) {
        return new LogicalGraphScan(getCluster(), traitSet, table);
    }

    public static LogicalGraphScan create(RelOptCluster cluster, RelOptTable relOptTable) {
        return new LogicalGraphScan(cluster, cluster.traitSet(), relOptTable);
    }

    public static LogicalGraphScan create(RelOptCluster cluster, GeaFlowGraph graph) {
        RelOptTable table = new RelOptTable() {

            @Override
            public <C> C unwrap(Class<C> aClass) {
                return (C) graph;
            }

            @Override
            public List<String> getQualifiedName() {
                return null;
            }

            @Override
            public double getRowCount() {
                return 0;
            }

            @Override
            public RelDataType getRowType() {
                return graph.getRowType(GQLJavaTypeFactory.create());
            }

            @Override
            public RelOptSchema getRelOptSchema() {
                return null;
            }

            @Override
            public RelNode toRel(ToRelContext context) {
                return null;
            }

            @Override
            public List<RelCollation> getCollationList() {
                return null;
            }

            @Override
            public RelDistribution getDistribution() {
                return null;
            }

            @Override
            public boolean isKey(ImmutableBitSet columns) {
                return false;
            }

            @Override
            public List<RelReferentialConstraint> getReferentialConstraints() {
                return null;
            }

            @Override
            public Expression getExpression(Class clazz) {
                return null;
            }

            @Override
            public RelOptTable extend(List<RelDataTypeField> extendedFields) {
                return null;
            }

            @Override
            public List<ColumnStrategy> getColumnStrategies() {
                return null;
            }
        };
        return create(cluster, table);
    }

    public static LogicalGraphScan emptyScan(RelOptCluster cluster, GraphRecordType graphRecordType) {
        return create(cluster, empty(graphRecordType));
    }


    private static RelOptTable empty(GraphRecordType graphRecordType) {
        return new RelOptTable() {

            @Override
            public <C> C unwrap(Class<C> aClass) {
                return null;
            }

            @Override
            public List<String> getQualifiedName() {
                return null;
            }

            @Override
            public double getRowCount() {
                return 0;
            }

            @Override
            public RelDataType getRowType() {
                return graphRecordType;
            }

            @Override
            public RelOptSchema getRelOptSchema() {
                return null;
            }

            @Override
            public RelNode toRel(ToRelContext context) {
                return null;
            }

            @Override
            public List<RelCollation> getCollationList() {
                return null;
            }

            @Override
            public RelDistribution getDistribution() {
                return null;
            }

            @Override
            public boolean isKey(ImmutableBitSet columns) {
                return false;
            }

            @Override
            public List<RelReferentialConstraint> getReferentialConstraints() {
                return null;
            }

            @Override
            public Expression getExpression(Class clazz) {
                return null;
            }

            @Override
            public RelOptTable extend(List<RelDataTypeField> extendedFields) {
                return null;
            }

            @Override
            public List<ColumnStrategy> getColumnStrategies() {
                return null;
            }
        };
    }

}
