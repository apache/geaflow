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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.ListScope;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.geaflow.dsl.validator.GQLValidatorImpl;

public class GQLScope extends ListScope {

    protected final SqlNode node;

    public GQLScope(SqlValidatorScope parent, SqlNode node) {
        super(parent);
        this.node = node;
    }

    @Override
    public SqlNode getNode() {
        return node;
    }

    @Override
    public SqlValidatorScope getOperandScope(SqlCall call) {
        SqlValidatorScope scope = ((GQLValidatorImpl) validator).getScopes(call);
        if (scope != null) {
            return scope;
        }
        return super.getOperandScope(call);
    }
}
