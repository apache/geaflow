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

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.sqlnode.SqlMatchNode;
import org.apache.geaflow.dsl.sqlnode.SqlPathPattern;
import org.apache.geaflow.dsl.sqlnode.SqlPathPatternSubQuery;
import org.apache.geaflow.dsl.validator.namespace.GQLMatchNodeNamespace.MatchNodeContext;
import org.apache.geaflow.dsl.validator.scope.GQLScope;

public class GQLSubQueryNamespace extends GQLBaseNamespace {

    private final SqlPathPatternSubQuery subQuery;

    public GQLSubQueryNamespace(SqlValidatorImpl validator,
                                SqlPathPatternSubQuery subQuery) {
        super(validator, subQuery);
        this.subQuery = subQuery;
    }

    @Override
    protected RelDataType validateImpl(RelDataType targetRowType) {
        GQLScope scope = (GQLScope) getValidator().getScopes(subQuery);
        assert scope.children.size() == 1;
        SqlValidatorNamespace fromNs = scope.children.get(0).getNamespace();
        assert fromNs.getType() instanceof PathRecordType;
        PathRecordType inputPathType = (PathRecordType) fromNs.getType();
        SqlPathPattern pathPattern = subQuery.getPathPattern();
        List<SqlNode> pathNodes = pathPattern.getPathNodes().getList();
        assert pathNodes.size() > 0;

        SqlMatchNode firstNode = (SqlMatchNode) pathNodes.get(0);
        validateFirstNode(firstNode, inputPathType);
        for (int i = 1; i < pathNodes.size(); i++) {
            validateOtherNode((SqlMatchNode) pathNodes.get(i), inputPathType);
        }

        GQLPathPatternNamespace pathPatternNs =
            validator.getNamespace(pathPattern).unwrap(GQLPathPatternNamespace.class);
        MatchNodeContext context = new MatchNodeContext();
        pathPatternNs.setMatchNodeContext(context);

        SqlNode returnValue = subQuery.getReturnValue();
        if (returnValue != null) {
            GQLScope returnValueScope = (GQLScope) getValidator().getScopes(returnValue);
            returnValue = validator.expand(returnValue, returnValueScope);
            subQuery.setReturnValue(returnValue);

            returnValue.validate(validator, returnValueScope);
            return validator.deriveType(returnValueScope, returnValue);
        }
        return pathPatternNs.getType();
    }

    private void validateFirstNode(SqlMatchNode firstNode, PathRecordType inputPathType) {
        RelDataTypeField field = inputPathType.getField(firstNode.getName(),
            isCaseSensitive(), false);
        if (field == null) {
            throw new GeaFlowDSLException(firstNode.getParserPosition(),
                "Label:{} is not exists in the input match statement", firstNode.getName());
        }
        if (firstNode.getKind() != SqlKind.GQL_MATCH_NODE
            || field.getType().getSqlTypeName() != SqlTypeName.VERTEX) {
            throw new GeaFlowDSLException(firstNode.getParserPosition(),
                "SubQuery should start from a vertex, current start label:{} is an edge", firstNode.getName());
        }
    }

    private void validateOtherNode(SqlMatchNode otherNode, PathRecordType inputPathType) {
        RelDataTypeField field = inputPathType.getField(otherNode.getName(),
            isCaseSensitive(), false);
        if (field != null) {
            throw new GeaFlowDSLException(otherNode.getParserPosition(),
                "Label:{} in SubQuery is already exists in the input match statement", otherNode.getName());
        }
    }

    @Override
    public SqlNode getNode() {
        return subQuery;
    }
}
