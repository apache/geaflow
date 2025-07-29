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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

public class GQLMatchNodeWhereNamespace extends GQLBaseNamespace {

    private final SqlNode matchNode;

    private final GQLMatchNodeNamespace nodeNamespace;

    public GQLMatchNodeWhereNamespace(SqlValidatorImpl validator, SqlNode matchNode,
                                      GQLMatchNodeNamespace nodeNamespace) {
        super(validator, matchNode);
        this.matchNode = matchNode;
        this.nodeNamespace = nodeNamespace;
    }

    @Override
    protected RelDataType validateImpl(RelDataType targetRowType) {
        return nodeNamespace.getNodeType();
    }

    @Override
    public SqlNode getNode() {
        return matchNode;
    }
}
