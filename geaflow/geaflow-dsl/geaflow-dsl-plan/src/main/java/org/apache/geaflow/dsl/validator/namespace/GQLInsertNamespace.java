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

package org.apache.geaflow.dsl.validator.namespace;

import com.google.common.collect.ImmutableList;
import java.util.Objects;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.IdentifierNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.geaflow.dsl.calcite.GraphRecordType;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.planner.GQLRelOptTableImpl;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.validator.GQLValidatorImpl;

public class GQLInsertNamespace extends GQLBaseNamespace {

    private final SqlInsert insert;

    private final IdentifierNamespace idNamespace;

    public GQLInsertNamespace(GQLValidatorImpl validator, SqlInsert insert, SqlValidatorScope parentScope) {
        super(validator, insert);
        this.insert = Objects.requireNonNull(insert);
        SqlIdentifier targetTable = (SqlIdentifier) insert.getTargetTable();
        SqlIdentifier targetId;
        int size = targetTable.names.size();
        if (size >= 3) { // for instance.g.v use instance.g for validate
            targetId = new SqlIdentifier(targetTable.names.subList(0, 2), targetTable.getParserPosition());
        } else {
            targetId = targetTable;
        }
        this.idNamespace = new IdentifierNamespace(validator, targetId, insert.getTargetTable(), parentScope);
    }

    @Override
    protected RelDataType validateImpl(RelDataType targetRowType) {
        RelDataType type = idNamespace.validateImpl(targetRowType);
        if (type instanceof GraphRecordType) {
            SqlIdentifier targetTable = (SqlIdentifier) insert.getTargetTable();
            if (targetTable.names.size() == 2) { // insert instance.g
                return type;
            } else if (targetTable.names.size() == 3) { // insert instance.g.v
                String vertexTableName = targetTable.names.get(2);
                RelDataTypeField field = type.getField(vertexTableName, isCaseSensitive(), false);
                if (field == null) {
                    throw new GeaFlowDSLException("Field:{} is not found, graph type is:{}", vertexTableName, type);
                }
                type = field.getType();
            } else {
                throw new GeaFlowDSLException(targetTable.getParserPosition().toString(),
                    "Illegal target table name size: {}", targetTable.names.size());
            }
            return type;
        }
        return type;
    }

    @Override
    public SqlValidatorTable getTable() {
        SqlValidatorTable validatorTable = idNamespace.resolve().getTable();
        GeaFlowGraph graph = validatorTable.unwrap(GeaFlowGraph.class);
        SqlIdentifier targetTable = (SqlIdentifier) insert.getTargetTable();

        if (graph != null && targetTable.names.size() == 3) { // for insert into instance.g.v
            String tableName = targetTable.names.get(2);
            Table table = graph.getTable(tableName);
            RelOptSchema optSchema = getValidator().getCatalogReader().unwrap(RelOptSchema.class);
            return GQLRelOptTableImpl.create(optSchema, getRowType(), table, ImmutableList.of());
        }
        return validatorTable;
    }

    @Override
    public SqlNode getNode() {
        return insert;
    }
}
