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

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.geaflow.dsl.catalog.Catalog;
import org.apache.geaflow.dsl.schema.GeaFlowFunction;
import org.apache.geaflow.dsl.util.FunctionUtil;

/**
 * An operator table for look up SQL operators and functions.
 */
public class GQLOperatorTable extends ChainedSqlOperatorTable {

    private final Catalog catalog;

    private final GQLJavaTypeFactory typeFactory;

    private final GQLContext gqlContext;

    public GQLOperatorTable(Catalog catalog, GQLJavaTypeFactory typeFactory,
                            GQLContext gqlContext,
                            SqlOperatorTable... tables) {
        super(Lists.newArrayList(tables));
        this.catalog = catalog;
        this.gqlContext = gqlContext;
        this.typeFactory = typeFactory;
    }

    /**
     * Add a {@code SqlFunction} to operator table.
     */
    public void registerSqlFunction(String instance, GeaFlowFunction function) {
        catalog.createFunction(instance, function);

        SqlFunction sqlFunction = FunctionUtil.createSqlFunction(function, typeFactory);
        for (SqlOperatorTable operatorTable : tableList) {
            if (operatorTable instanceof ListSqlOperatorTable) {
                ((ListSqlOperatorTable) operatorTable).add(sqlFunction);
                return;
            }
        }
    }

    public SqlFunction getSqlFunction(String instance, String name) {
        for (SqlOperator operator : getOperatorList()) {
            if (operator.getName().equalsIgnoreCase(name)) {
                if (operator instanceof SqlFunction) {
                    return (SqlFunction) operator;
                }
            }
        }
        GeaFlowFunction function = catalog.getFunction(instance, name);
        if (function == null) {
            return null;
        }
        return FunctionUtil.createSqlFunction(function, typeFactory);
    }

    @Override
    public void lookupOperatorOverloads(SqlIdentifier opName,
                                        SqlFunctionCategory category, SqlSyntax syntax,
                                        List<SqlOperator> operatorList) {
        super.lookupOperatorOverloads(opName, category, syntax, operatorList);
        if (operatorList.isEmpty() && category == SqlFunctionCategory.USER_DEFINED_FUNCTION) {
            GeaFlowFunction function = catalog.getFunction(gqlContext.getCurrentInstance(), opName.getSimple());
            if (function != null) {
                operatorList.add(FunctionUtil.createSqlFunction(function, typeFactory));
            }
        }
    }
}