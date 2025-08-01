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
import org.apache.geaflow.dsl.catalog.exception.ObjectNotExistException;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.QueryResult;
import org.apache.geaflow.dsl.sqlnode.SqlUseInstance;

public class UseInstanceCommand implements IQueryCommand {

    private final SqlUseInstance useInstance;

    public UseInstanceCommand(SqlUseInstance useInstance) {
        this.useInstance = useInstance;
    }

    @Override
    public QueryResult execute(QueryContext context) {
        String instance = useInstance.getInstance().getSimple();
        if (!context.getGqlContext().getCatalog().isInstanceExists(instance)) {
            throw new ObjectNotExistException("Instance: '" + instance + "' is not exists");
        }
        context.getGqlContext().setCurrentInstance(useInstance.getInstance().getSimple());
        return new QueryResult(true);
    }

    @Override
    public SqlNode getSqlNode() {
        return useInstance;
    }
}
