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

package org.apache.geaflow.dsl.runtime.command;

import org.apache.calcite.sql.SqlNode;
import org.apache.geaflow.dsl.planner.GQLContext;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.QueryResult;
import org.apache.geaflow.dsl.schema.GeaFlowFunction;
import org.apache.geaflow.dsl.sqlnode.SqlCreateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateFunctionCommand implements IQueryCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateFunctionCommand.class);

    private final SqlCreateFunction createFunction;

    public CreateFunctionCommand(SqlCreateFunction createFunction) {
        this.createFunction = createFunction;
    }

    @Override
    public QueryResult execute(QueryContext context) {
        GQLContext gqlContext = context.getGqlContext();
        GeaFlowFunction function = GeaFlowFunction.toFunction(createFunction);
        // register function to catalog.
        gqlContext.registerFunction(function);
        LOGGER.info("Success to create function: \n{}", function);
        return new QueryResult(true);
    }

    @Override
    public SqlNode getSqlNode() {
        return createFunction;
    }
}
