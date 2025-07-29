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

import static org.apache.calcite.sql.SqlKind.DESCENDING;
import static org.apache.calcite.util.Static.RESOURCE;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Pair;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.sqlnode.SqlReturnStatement;
import org.apache.geaflow.dsl.validator.scope.GQLReturnScope;

public class GQLReturnNamespace extends GQLBaseNamespace {

    private final SqlReturnStatement returnStatement;

    public GQLReturnNamespace(SqlValidatorImpl validator, SqlNode enclosingNode,
                              SqlReturnStatement returnStatement) {
        super(validator, enclosingNode);
        this.returnStatement = returnStatement;
    }

    @Override
    protected RelDataType validateImpl(RelDataType targetRowType) {
        assert targetRowType != null;

        GQLReturnNamespace ns = validator.getNamespace(returnStatement).unwrap(GQLReturnNamespace.class);
        assert ns.rowType == null;
        // validate from
        SqlValidatorNamespace fromNs = validator.getNamespace(returnStatement.getFrom());
        fromNs.validate(targetRowType);
        //return * are not support
        final SqlNodeList selectItems = returnStatement.getReturnList();
        if (selectItems.size() == 1) {
            final SqlNode selectItem = selectItems.get(0);
            if (selectItem instanceof SqlIdentifier) {
                SqlIdentifier id = (SqlIdentifier) selectItem;
                if (id.isStar() && (id.names.size() == 1)) {
                    throw new GeaFlowDSLException("'Return * ' is not support at " + id.getParserPosition());
                }
            }
        }

        final RelDataType rowType =
            validateReturnList(selectItems, returnStatement, targetRowType);
        ns.setType(rowType);
        validateGroupClause(returnStatement);
        handleOffsetFetch(returnStatement.getOffset(), returnStatement.getFetch());

        GQLReturnScope scope = (GQLReturnScope) getValidator().getScopes(returnStatement);
        for (SqlNode returnItem : returnStatement.getReturnList()) {
            returnItem.validate(getValidator(), scope);
            scope.validateExpr(returnItem);
        }
        validateOrderList(returnStatement);
        return rowType;
    }

    protected RelDataType validateReturnList(
        final SqlNodeList returnItems,
        SqlReturnStatement returnStmt,
        RelDataType targetRowType) {
        GQLReturnScope scope = (GQLReturnScope) getValidator().getScopes(returnStatement);

        final List<SqlNode> expandedReturnItems = new ArrayList<>();
        final Set<String> aliases = new HashSet<>();
        final List<Map.Entry<String, RelDataType>> fieldList = new ArrayList<>();

        for (int i = 0; i < returnItems.size(); i++) {
            SqlNode returnItem = returnItems.get(i);
            if (returnItem instanceof SqlSelect) {
                throw new GeaFlowDSLException("SubQuery in Return statement is not support");
            } else {
                //expand select items
                SqlNode expanded = validator.expand(returnItem, scope);
                final String alias =
                    SqlValidatorUtil.getAlias(returnItem, aliases.size());
                // If expansion has altered the natural alias, supply an explicit 'AS'.
                if (expanded != returnItem) {
                    String newAlias =
                        SqlValidatorUtil.getAlias(expanded, aliases.size());
                    if (!newAlias.equals(alias)) {
                        expanded =
                            SqlStdOperatorTable.AS.createCall(
                                returnItem.getParserPosition(),
                                expanded,
                                new SqlIdentifier(alias, SqlParserPos.ZERO));
                        validator.deriveType(scope, expanded);
                    }
                }
                expandedReturnItems.add(expanded);
                if (aliases.contains(alias)) {
                    throw new GeaFlowDSLException("Duplicated alias in an Return statement at "
                        + returnItem.getParserPosition());
                }
                aliases.add(alias);
                RelDataType targetType = targetRowType.isStruct()
                    && targetRowType.getFieldCount() - 1 >= i
                    ? targetRowType.getFieldList().get(i).getType()
                    : validator.getUnknownType();

                getValidator().inferUnknownTypes(targetType, scope, expanded);
                final RelDataType type = validator.deriveType(scope, expanded);
                validator.setValidatedNodeType(expanded, type);
                fieldList.add(Pair.of(alias, type));
            }
        }
        // Create the new select list with expanded items.  Pass through
        // the original parser position so that any overall failures can
        // still reference the original input text.
        SqlNodeList newReturnList =
            new SqlNodeList(
                expandedReturnItems,
                returnItems.getParserPosition());
        if (validator.shouldExpandIdentifiers()) {
            returnStmt.setReturnList(newReturnList);
        }
        scope.setExpandedReturnList(expandedReturnItems);
        getValidator().inferUnknownTypes(targetRowType, scope, newReturnList);

        return validator.getTypeFactory().createStructType(fieldList);
    }

    protected void validateGroupClause(SqlReturnStatement returnStmt) {
        SqlNodeList groupList = returnStmt.getGroupBy();
        if (groupList == null) {
            return;
        }
        final SqlValidatorScope scope = getValidator().getScopes(groupList);
        Objects.requireNonNull(scope);
        getValidator().inferUnknownTypes(
            validator.getUnknownType(), scope, groupList);

        // expand the expression in group list.
        List<SqlNode> expandedList = new ArrayList<>();
        for (SqlNode groupItem : groupList) {
            SqlNode expandedGroupItem =
                getValidator().expandReturnGroupOrderExpr(returnStmt,
                    scope, groupItem);
            expandedList.add(expandedGroupItem);
        }
        groupList = new SqlNodeList(expandedList, groupList.getParserPosition());
        //GROUPING SETS, ROLLUP or CUBE are not support
        for (SqlNode node : groupList) {
            if (node.getKind() != SqlKind.IDENTIFIER) {
                throw new GeaFlowDSLException("Group by non-identifier is not support, actually " + node.getKind());
            }
        }
        returnStatement.setGroupBy(groupList);
        getValidator().registerScope(groupList, scope);

        for (SqlNode groupItem : expandedList) {
            scope.validateExpr(groupItem);
        }
        if (getValidator().getAggregate(groupList) != null) {
            throw new GeaFlowDSLException("Aggregation in Group By is not support.");
        }
    }

    private void handleOffsetFetch(SqlNode offset, SqlNode fetch) {
        if (offset instanceof SqlDynamicParam) {
            validator.setValidatedNodeType(offset,
                validator.getTypeFactory().createSqlType(SqlTypeName.INTEGER));
        }
        if (fetch instanceof SqlDynamicParam) {
            validator.setValidatedNodeType(fetch,
                validator.getTypeFactory().createSqlType(SqlTypeName.INTEGER));
        }
    }

    protected void validateOrderList(SqlReturnStatement returnStmt) {
        SqlNodeList orderList = returnStmt.getOrderList();
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
                getValidator().expandReturnGroupOrderExpr(returnStmt,
                    scope, orderItem);
            expandList.add(expandedOrderItem);
        }

        SqlNodeList expandedOrderList = new SqlNodeList(
            expandList,
            orderList.getParserPosition());

        returnStatement.setOrderBy(expandedOrderList);

        getValidator().registerScope(expandedOrderList, scope);

        for (SqlNode orderItem : expandedOrderList) {
            if (orderItem.getKind() == DESCENDING) {
                assert RESOURCE.sQLConformance_OrderByDesc()
                    .getProperties().get("FeatureDefinition") != null;
                scope.validateExpr(((SqlCall) orderItem).operand(0));
            } else {
                scope.validateExpr(orderItem);
            }
        }
    }

    @Override
    public SqlNode getNode() {
        return returnStatement;
    }
}
