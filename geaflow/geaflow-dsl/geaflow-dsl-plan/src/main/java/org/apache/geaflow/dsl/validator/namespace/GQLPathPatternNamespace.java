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

import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.sqlnode.SqlMatchNode;
import org.apache.geaflow.dsl.sqlnode.SqlPathPattern;
import org.apache.geaflow.dsl.validator.namespace.GQLMatchNodeNamespace.MatchNodeContext;
import org.apache.geaflow.dsl.validator.scope.GQLPathPatternScope;
import org.apache.geaflow.dsl.validator.scope.GQLSubQueryScope;

public class GQLPathPatternNamespace extends GQLBaseNamespace {

    private final SqlPathPattern pathPattern;

    private MatchNodeContext matchNodeContext;

    public GQLPathPatternNamespace(SqlValidatorImpl validator, SqlPathPattern pathPattern) {
        super(validator, pathPattern);
        this.pathPattern = pathPattern;
    }

    public void setMatchNodeContext(MatchNodeContext matchNodeContext) {
        this.matchNodeContext = matchNodeContext;
    }

    @Override
    protected RelDataType validateImpl(RelDataType targetRowType) {
        assert matchNodeContext != null : "matchNodeContext has not set";
        GQLPathPatternScope scope = (GQLPathPatternScope) getValidator().getScopes(pathPattern);
        matchNodeContext.setPathPatternScope(scope);

        // for match in sub-query, the parentPathType is the output type of the parent match.
        PathRecordType parentPathType = PathRecordType.EMPTY;
        if (scope.getParent() instanceof GQLSubQueryScope) {
            parentPathType = ((GQLSubQueryScope) scope.getParent()).getInputPathType();
        }
        PathRecordType outputPathType = null;
        Set<String> matchNodeAlias = new HashSet<>();
        boolean isFirstNode = true;
        // init the input path type.
        matchNodeContext.setInputPathType(parentPathType);
        for (SqlNode pathNode : pathPattern.getPathNodes()) {
            SqlMatchNode matchNode = (SqlMatchNode) pathNode;
            if (matchNodeAlias.contains(matchNode.getName())) {
                throw new GeaFlowDSLException(matchNode.getParserPosition(),
                    "Duplicated node label: {} in the path pattern.", matchNode.getName());
            } else {
                matchNodeAlias.add(matchNode.getName());
            }
            GQLMatchNodeNamespace nodeNs = (GQLMatchNodeNamespace) validator.getNamespace(matchNode);
            matchNodeContext.setFirstNode(isFirstNode);
            nodeNs.setMatchNodeContext(matchNodeContext);

            matchNode.validate(validator, scope);
            outputPathType = (PathRecordType) validator.getValidatedNodeType(matchNode);

            isFirstNode = false;
        }
        assert outputPathType != null;
        // concat output path type with parent match's path type if current path pattern is in sub-query.
        return concatParentPathType(parentPathType, outputPathType);
    }

    private PathRecordType concatParentPathType(PathRecordType parentPathType,
                                                PathRecordType pathRecordType) {
        if (parentPathType == null) {
            return pathRecordType;
        }
        return parentPathType.concat(pathRecordType, isCaseSensitive());
    }

    @Override
    public SqlNode getNode() {
        return pathPattern;
    }
}
