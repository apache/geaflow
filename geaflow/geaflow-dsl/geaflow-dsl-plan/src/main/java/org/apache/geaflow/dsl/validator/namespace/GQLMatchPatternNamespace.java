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

import static org.apache.calcite.util.Static.RESOURCE;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.geaflow.dsl.calcite.GraphRecordType;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.calcite.UnionPathRecordType;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.sqlnode.SqlMatchPattern;
import org.apache.geaflow.dsl.sqlnode.SqlPathPattern;
import org.apache.geaflow.dsl.sqlnode.SqlUnionPathPattern;
import org.apache.geaflow.dsl.validator.namespace.GQLMatchNodeNamespace.MatchNodeContext;
import org.apache.geaflow.dsl.validator.scope.GQLScope;

public class GQLMatchPatternNamespace extends GQLBaseNamespace {

    private final SqlMatchPattern matchPattern;

    public GQLMatchPatternNamespace(SqlValidatorImpl validator, SqlMatchPattern matchPattern) {
        super(validator, matchPattern);
        this.matchPattern = matchPattern;
    }

    @Override
    protected RelDataType validateImpl(RelDataType targetRowType) {
        SqlValidatorNamespace fromNs = validator.getNamespace(matchPattern.getFrom());
        fromNs.validate(targetRowType);
        RelDataType fromType = fromNs.getType();

        if (!(fromType instanceof GraphRecordType) && !(fromType instanceof PathRecordType)) {
            throw new GeaFlowDSLException(matchPattern.getParserPosition(),
                "Only can match from a graph or match statement");
        }

        SqlNodeList pathPatternNodes = matchPattern.getPathPatterns();
        SqlValidatorScope scope = getValidator().getScopes(matchPattern);

        MatchNodeContext matchNodeContext = new MatchNodeContext();

        List<PathRecordType> pathPatternTypes = new ArrayList<>();
        for (SqlNode pathPatternNode : pathPatternNodes) {
            RelDataType pathType;
            if (pathPatternNode instanceof SqlPathPattern) {
                SqlPathPattern pathPattern = (SqlPathPattern) pathPatternNode;
                GQLPathPatternNamespace pathPatternNs =
                    (GQLPathPatternNamespace) validator.getNamespace(pathPatternNode);
                pathPatternNs.setMatchNodeContext(matchNodeContext);
                pathPattern.validate(validator, scope);
                pathType = validator.getValidatedNodeType(pathPattern);
            } else {
                SqlUnionPathPattern pathPattern = (SqlUnionPathPattern) pathPatternNode;
                GQLUnionPathPatternNamespace pathPatternNs =
                    (GQLUnionPathPatternNamespace) validator.getNamespace(pathPatternNode);
                pathPatternNs.setMatchNodeContext(matchNodeContext);
                pathPattern.validate(validator, scope);
                pathType = validator.getValidatedNodeType(pathPattern);
            }
            if (!(pathType instanceof PathRecordType)) {
                throw new IllegalStateException("PathPattern should return PathRecordType");
            }
            matchNodeContext.addResolvedPathPatternType((PathRecordType) pathType);
            pathPatternTypes.add((PathRecordType) pathType);
        }
        PathRecordType matchType = createMatchType(pathPatternTypes);
        // join from path with current match path for continue match.
        if (fromType instanceof PathRecordType) {
            if (matchPattern.isSinglePattern()
                && ((PathRecordType) fromType).canConcat(matchType)) {
                matchType = ((PathRecordType) fromType).concat(matchType, isCaseSensitive());
            } else {
                matchType = ((PathRecordType) fromType).join(matchType, getValidator().getTypeFactory());
            }
        }

        if (matchPattern.getWhere() != null) {
            setType(matchType);
            SqlNode where = matchPattern.getWhere();
            GQLScope whereScope = (GQLScope) getValidator().getScopes(where);
            where = validator.expand(where, whereScope);
            matchPattern.setWhere(where);
            RelDataType boolType = validator.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
            getValidator().inferUnknownTypes(boolType, whereScope, where);
            RelDataType conditionType = validator.deriveType(whereScope, where);
            if (!SqlTypeUtil.inBooleanFamily(conditionType)) {
                throw validator.newValidationError(where, RESOURCE.condMustBeBoolean("Filter"));
            }
        }
        setType(matchType);
        validateOrderList();
        return matchType;
    }

    private PathRecordType createMatchType(List<PathRecordType> pathPatternTypes) {
        List<PathRecordType> concatPathTypes = new ArrayList<>();

        for (PathRecordType pathType : pathPatternTypes) {
            if (pathType instanceof UnionPathRecordType) {
                concatPathTypes.add(pathType);
            } else {
                assert pathType.firstFieldName().isPresent() && pathType.lastFieldName().isPresent();
                String firstField = pathType.firstFieldName().get();
                String lastField = pathType.lastFieldName().get();
                int i;
                for (i = 0; i < concatPathTypes.size(); i++) {
                    PathRecordType pathType2 = concatPathTypes.get(i);
                    if (pathType2 instanceof UnionPathRecordType) {
                        continue;
                    }
                    assert pathType2.firstFieldName().isPresent() && pathType2.lastFieldName().isPresent();
                    String firstField2 = pathType2.firstFieldName().get();
                    String lastField2 = pathType2.lastFieldName().get();
                    // pathType2 is "(a) - (b) - (c)", while pathType is "(c) - (d)"
                    // concat pathType to pathType2 and return "(a) - (b) - (c) - (d)"
                    if (getValidator().nameMatcher().matches(lastField2, firstField)) {
                        PathRecordType concatPathType = pathType2.concat(pathType, isCaseSensitive());
                        concatPathTypes.set(i, concatPathType);
                        break;
                    } else if (getValidator().nameMatcher().matches(lastField, firstField2)) {
                        // pathType2 is "(c) - (d)", while pathType is "(a) - (b) - (c)"
                        // concat pathType2 to pathType and return "(a) - (b) - (c) - (d)"
                        PathRecordType concatPathType = pathType.concat(pathType2, isCaseSensitive());
                        concatPathTypes.set(i, concatPathType);
                        break;
                    }
                }
                // If cannot concat, just add to list.
                if (i == concatPathTypes.size()) {
                    concatPathTypes.add(pathType);
                }
            }
        }

        PathRecordType joinPathType = concatPathTypes.get(0);
        for (int i = 1; i < concatPathTypes.size(); i++) {
            joinPathType = joinPathType.join(concatPathTypes.get(i), getValidator().getTypeFactory());
        }
        return joinPathType;
    }

    protected void validateOrderList() {
        SqlNodeList orderList = matchPattern.getOrderBy();
        if (orderList == null) {
            return;
        }
        final SqlValidatorScope scope = getValidator().getScopes(orderList);
        Objects.requireNonNull(scope);
        getValidator().inferUnknownTypes(
            validator.getUnknownType(), scope, orderList);

        List<SqlNode> expandList = new ArrayList<>();
        for (SqlNode orderItem : orderList) {
            SqlNode expandedOrderItem =
                getValidator().expand(orderItem, scope);
            expandList.add(expandedOrderItem);
        }

        SqlNodeList expandedOrderList = new SqlNodeList(
            expandList,
            orderList.getParserPosition());

        matchPattern.setOrderBy(expandedOrderList);
        getValidator().registerScope(expandedOrderList, scope);

        for (SqlNode orderItem : expandedOrderList) {
            validator.deriveType(scope, orderItem);
        }
    }

    @Override
    public SqlNode getNode() {
        return matchPattern;
    }
}
