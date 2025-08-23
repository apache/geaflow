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

package org.apache.geaflow.dsl.validator.scope;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.geaflow.dsl.sqlnode.SqlReturnStatement;

public class GQLReturnGroupByScope extends GQLScope {

    private final SqlReturnStatement returnStatement;

    public GQLReturnGroupByScope(SqlValidatorScope parent, SqlReturnStatement returnStatement,
                                 SqlNodeList groupBy) {
        super(parent, groupBy);
        this.returnStatement = returnStatement;
    }

    @Override
    public void validateExpr(SqlNode expr) {
        parent.validateExpr(expr);
    }

    public SqlReturnStatement getReturnStmt() {
        return returnStatement;
    }
}
