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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.calcite.UnionPathRecordType;
import org.apache.geaflow.dsl.sqlnode.SqlPathPattern;
import org.apache.geaflow.dsl.sqlnode.SqlUnionPathPattern;
import org.apache.geaflow.dsl.validator.namespace.GQLMatchNodeNamespace.MatchNodeContext;

public class GQLUnionPathPatternNamespace extends GQLBaseNamespace {

    private final SqlUnionPathPattern unionPathPattern;

    private MatchNodeContext matchNodeContext;

    public GQLUnionPathPatternNamespace(SqlValidatorImpl validator, SqlUnionPathPattern unionPathPattern) {
        super(validator, unionPathPattern);
        this.unionPathPattern = unionPathPattern;
    }

    public void setMatchNodeContext(MatchNodeContext matchNodeContext) {
        this.matchNodeContext = matchNodeContext;
    }

    @Override
    protected RelDataType validateImpl(RelDataType targetRowType) {
        SqlValidatorScope scope = getValidator().getScopes(unionPathPattern);
        List<PathRecordType> pathRecordTypes = new ArrayList<>();
        validatePathPatternRecursive(unionPathPattern, matchNodeContext, scope, pathRecordTypes);
        PathRecordType patternType = new UnionPathRecordType(pathRecordTypes,
            this.getValidator().getTypeFactory());
        return patternType;
    }

    private void validatePathPatternRecursive(SqlNode pathPatternNode,
                                              MatchNodeContext matchNodeContext,
                                              SqlValidatorScope scope,
                                              List<PathRecordType> pathPatternTypes) {
        if (pathPatternNode instanceof SqlUnionPathPattern) {
            SqlUnionPathPattern unionPathPattern = (SqlUnionPathPattern) pathPatternNode;
            validatePathPatternRecursive(unionPathPattern.getLeft(), matchNodeContext, scope,
                pathPatternTypes);
            validatePathPatternRecursive(unionPathPattern.getRight(), matchNodeContext, scope,
                pathPatternTypes);
        }
        if (pathPatternNode instanceof SqlPathPattern) {
            SqlPathPattern pathPattern = (SqlPathPattern) pathPatternNode;
            GQLPathPatternNamespace pathPatternNs =
                (GQLPathPatternNamespace) validator.getNamespace(
                    pathPatternNode);
            pathPatternNs.setMatchNodeContext(matchNodeContext);

            pathPattern.validate(validator, scope);
            RelDataType pathType = validator.getValidatedNodeType(pathPattern);
            if (!(pathType instanceof PathRecordType)) {
                throw new IllegalStateException("PathPattern should return PathRecordType");
            }
            matchNodeContext.addResolvedPathPatternType((PathRecordType) pathType);
            pathPatternTypes.add((PathRecordType) pathType);
        }
    }


    @Override
    public SqlNode getNode() {
        return unionPathPattern;
    }
}
