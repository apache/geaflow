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

package org.apache.geaflow.dsl.validator;

import static org.apache.calcite.util.Static.RESOURCE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.*;
import org.apache.geaflow.dsl.calcite.EdgeRecordType;
import org.apache.geaflow.dsl.calcite.GraphRecordType;
import org.apache.geaflow.dsl.calcite.VertexRecordType;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.types.VertexType;
import org.apache.geaflow.dsl.planner.GQLContext;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.schema.function.GeaFlowOverwriteSqlOperators;
import org.apache.geaflow.dsl.sqlnode.SqlFilterStatement;
import org.apache.geaflow.dsl.sqlnode.SqlGraphAlgorithmCall;
import org.apache.geaflow.dsl.sqlnode.SqlLetStatement;
import org.apache.geaflow.dsl.sqlnode.SqlMatchNode;
import org.apache.geaflow.dsl.sqlnode.SqlMatchPattern;
import org.apache.geaflow.dsl.sqlnode.SqlPathPattern;
import org.apache.geaflow.dsl.sqlnode.SqlPathPatternSubQuery;
import org.apache.geaflow.dsl.sqlnode.SqlReturnStatement;
import org.apache.geaflow.dsl.sqlnode.SqlSamePredicatePattern;
import org.apache.geaflow.dsl.sqlnode.SqlUnionPathPattern;
import org.apache.geaflow.dsl.util.GQLNodeUtil;
import org.apache.geaflow.dsl.validator.namespace.GQLAlgorithmNamespace;
import org.apache.geaflow.dsl.validator.namespace.GQLFilterNamespace;
import org.apache.geaflow.dsl.validator.namespace.GQLInsertNamespace;
import org.apache.geaflow.dsl.validator.namespace.GQLLetNamespace;
import org.apache.geaflow.dsl.validator.namespace.GQLMatchNodeNamespace;
import org.apache.geaflow.dsl.validator.namespace.GQLMatchNodeWhereNamespace;
import org.apache.geaflow.dsl.validator.namespace.GQLMatchPatternNamespace;
import org.apache.geaflow.dsl.validator.namespace.GQLPathPatternNamespace;
import org.apache.geaflow.dsl.validator.namespace.GQLReturnNamespace;
import org.apache.geaflow.dsl.validator.namespace.GQLSubQueryNamespace;
import org.apache.geaflow.dsl.validator.namespace.GQLSamePredicateNamespace;
import org.apache.geaflow.dsl.validator.namespace.GQLUnionPathPatternNamespace;
import org.apache.geaflow.dsl.validator.namespace.IdentifierCompleteNamespace;
import org.apache.geaflow.dsl.validator.scope.GQLPathPatternScope;
import org.apache.geaflow.dsl.validator.scope.GQLReturnGroupByScope;
import org.apache.geaflow.dsl.validator.scope.GQLReturnOrderByScope;
import org.apache.geaflow.dsl.validator.scope.GQLReturnScope;
import org.apache.geaflow.dsl.validator.scope.GQLScope;
import org.apache.geaflow.dsl.validator.scope.GQLSubQueryScope;
import org.apache.geaflow.dsl.validator.scope.GQLWithBodyScope;

public class GQLValidatorImpl extends SqlValidatorImpl {

    private final GQLContext gContext;
    private final GQLJavaTypeFactory typeFactory;
    private static final String ANONYMOUS_COLUMN_PREFIX = "col_";
    private static final String RECURRING_COLUMN_SUFFIX = "_rcr";
    private final Map<SqlMatchNode, RelDataType> matchNodeTypes = new HashMap<>();

    private final Map<SqlLetStatement, GraphRecordType> let2ModifyGraphType = new HashMap<>();

    private final Map<SqlMatchNode, SqlMatchNode> renamedMatchNodes = new HashMap<>();

    private QueryNodeContext currentQueryNodeContext;

    public GQLValidatorImpl(GQLContext gContext, SqlOperatorTable opTab,
                            SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory,
                            SqlConformance conformance) {
        super(opTab, catalogReader, typeFactory, conformance);
        this.setCallRewrite(false);
        this.gContext = gContext;
        this.typeFactory = (GQLJavaTypeFactory) typeFactory;
    }

    public SqlNode validate(SqlNode sqlNode, QueryNodeContext queryNodeContext) {
        this.currentQueryNodeContext = queryNodeContext;
        return super.validate(sqlNode);
    }

    @Override
    public RelDataType getLogicalSourceRowType(RelDataType sourceRawType,
                                               SqlInsert insert) {
        return typeFactory.toSql(sourceRawType);
    }

    @Override
    protected void registerQuery(
        SqlValidatorScope parentScope,
        SqlValidatorScope usingScope,
        SqlNode node,
        SqlNode enclosingNode,
        String alias,
        boolean forceNullable) {
        if (node.getKind() == SqlKind.INSERT) {
            SqlInsert insertCall = (SqlInsert) node;
            GQLInsertNamespace insertNs =
                new GQLInsertNamespace(
                    this,
                    insertCall,
                    parentScope);
            registerNamespace(usingScope, null, insertNs, forceNullable);
            registerQuery(
                parentScope,
                usingScope,
                insertCall.getSource(),
                enclosingNode,
                null,
                false);
        } else {
            super.registerQuery(parentScope, usingScope, node, enclosingNode, alias, forceNullable);
        }
    }

    @Override
    protected void registerNamespace(SqlValidatorScope usingScope, String alias,
                                     SqlValidatorNamespace ns, boolean forceNullable) {
        SqlValidatorNamespace newNs = ns;
        // auto complete the instance name
        if (ns instanceof IdentifierNamespace) {
            IdentifierNamespace idNs = (IdentifierNamespace) ns;
            newNs = new IdentifierCompleteNamespace(idNs);
        }
        super.registerNamespace(usingScope, alias, newNs, forceNullable);
    }

    @Override
    protected void registerOtherKindQuery(SqlValidatorScope parentScope,
                                          SqlValidatorScope usingScope, SqlNode node,
                                          SqlNode enclosingNode, String alias,
                                          boolean forceNullable, boolean checkUpdate) {
        switch (node.getKind()) {
            case GQL_RETURN:
                SqlReturnStatement returnStatement = (SqlReturnStatement) node;
                GQLReturnScope returnScope = new GQLReturnScope(parentScope, returnStatement);
                GQLReturnNamespace returnNs = new GQLReturnNamespace(this, enclosingNode,
                    returnStatement);
                registerNamespace(usingScope, alias, returnNs, forceNullable);
                if (returnStatement.getGroupBy() != null
                    || getAggregate(returnStatement.getReturnList()) != null) {
                    returnScope.setAggMode();
                }
                // register from
                String matchPatternNsAlias = deriveAlias(returnStatement.getFrom());
                registerQuery(parentScope, returnScope, returnStatement.getFrom(),
                    returnStatement.getFrom(), matchPatternNsAlias, forceNullable);
                scopes.put(returnStatement, returnScope);

                String returnNsAlias = deriveAlias(returnStatement);
                if (returnStatement.getGroupBy() != null) {
                    GQLReturnGroupByScope groupByScope = new GQLReturnGroupByScope(returnScope,
                        returnStatement, returnStatement.getGroupBy());
                    registerNamespace(groupByScope, returnNsAlias, returnNs, forceNullable);
                    scopes.put(returnStatement.getGroupBy(), groupByScope);
                }
                if (returnStatement.getOrderBy() != null) {
                    GQLReturnOrderByScope orderByScope = new GQLReturnOrderByScope(returnScope,
                        returnStatement.getOrderBy());
                    registerNamespace(orderByScope, returnNsAlias, returnNs, forceNullable);
                    scopes.put(returnStatement.getOrderBy(), orderByScope);
                }
                break;
            case GQL_FILTER:
                SqlFilterStatement filterStatement = (SqlFilterStatement) node;
                GQLFilterNamespace filterNs = new GQLFilterNamespace(this, enclosingNode,
                    filterStatement);
                registerNamespace(usingScope, alias, filterNs, forceNullable);

                GQLScope filterScope = new GQLScope(parentScope, filterStatement);
                registerQuery(parentScope, filterScope, filterStatement.getFrom(),
                    filterStatement.getFrom(), deriveAlias(filterStatement.getFrom()), forceNullable);
                scopes.put(filterStatement, filterScope);
                break;
            case GQL_MATCH_PATTERN:
                //register MatchPattern
                SqlMatchPattern matchPattern = (SqlMatchPattern) node;
                GQLScope matchPatternScope = new GQLScope(parentScope, matchPattern);
                GQLMatchPatternNamespace matchNamespace = new GQLMatchPatternNamespace(this,
                    matchPattern);
                registerNamespace(usingScope, alias, matchNamespace, forceNullable);
                // performUnconditionalRewrites will set current graph node to the
                // matchPattern#from, so it cannot be null.
                assert matchPattern.getFrom() != null;

                registerQuery(parentScope, matchPatternScope, matchPattern.getFrom(), matchPattern,
                    deriveAlias(matchPattern.getFrom()), forceNullable);
                scopes.put(matchPattern, matchPatternScope);

                SqlNodeList pathPatterns = matchPattern.getPathPatterns();
                SqlValidatorNamespace fromNs = namespaces.get(matchPattern.getFrom());
                for (SqlNode sqlNode : pathPatterns) {
                    registerPathPattern(sqlNode, parentScope, fromNs, alias, forceNullable);
                    if (sqlNode instanceof SqlUnionPathPattern) {
                        SqlUnionPathPattern unionPathPattern = (SqlUnionPathPattern) sqlNode;
                        scopes.put(unionPathPattern, matchPatternScope);
                    }
                }
                if (matchPattern.getWhere() != null) {
                    SqlNode where = matchPattern.getWhere();
                    GQLScope whereScope = new GQLScope(parentScope, where);
                    registerNamespace(whereScope, alias, matchNamespace, forceNullable);
                    scopes.put(where, whereScope);
                    registerGqlSubQuery(whereScope, alias, matchNamespace, where);
                }
                if (matchPattern.getOrderBy() != null) {
                    GQLReturnOrderByScope orderByScope = new GQLReturnOrderByScope(matchPatternScope,
                        matchPattern.getOrderBy());
                    registerNamespace(orderByScope, alias, matchNamespace, forceNullable);
                    scopes.put(matchPattern.getOrderBy(), orderByScope);
                }
                break;
            case IDENTIFIER:
                SqlIdentifier identifier = (SqlIdentifier) node;
                IdentifierNamespace ns = new IdentifierNamespace(this, identifier, null, identifier,
                    parentScope);
                registerNamespace(usingScope, gContext.getCurrentGraph(), ns, forceNullable);
                break;
            case GQL_LET:
                SqlLetStatement letStatement = (SqlLetStatement) node;
                GQLLetNamespace letNamespace = new GQLLetNamespace(this, letStatement);
                registerNamespace(usingScope, alias, letNamespace, forceNullable);

                GQLScope letScope = new GQLScope(parentScope, node);
                registerQuery(parentScope, letScope, letStatement.getFrom(), letStatement.getFrom(),
                    deriveAlias(letStatement.getFrom()), forceNullable);
                // register sub query in let expression.
                SqlValidatorNamespace letFromNs = namespaces.get(letStatement.getFrom());
                registerGqlSubQuery(letScope, alias, letFromNs, letStatement.getExpression());
                scopes.put(letStatement, letScope);
                break;
            case GQL_ALGORITHM:
                SqlGraphAlgorithmCall algorithmCall = (SqlGraphAlgorithmCall) node;
                GQLAlgorithmNamespace algorithmNamespace = new GQLAlgorithmNamespace(this,
                    algorithmCall);
                registerNamespace(usingScope, alias, algorithmNamespace, forceNullable);
                GQLScope algorithmScope = new GQLScope(parentScope, node);
                scopes.put(algorithmCall, algorithmScope);
                break;
            case OTHER:
                // Handle same predicate pattern
                if (node instanceof SqlSamePredicatePattern) {
                    SqlSamePredicatePattern samePredicatePattern = (SqlSamePredicatePattern) node;
                    GQLScope samePredicateScope = new GQLScope(parentScope, samePredicatePattern);
                    GQLSamePredicateNamespace samePredicateNamespace = new GQLSamePredicateNamespace(this, samePredicatePattern);
                    registerNamespace(usingScope, alias, samePredicateNamespace, forceNullable);
                    
                    // Register left and right path patterns
                    SqlValidatorNamespace samePredicateFromNs = namespaces.get(enclosingNode);
                    registerPathPattern(samePredicatePattern.getLeft(), parentScope, samePredicateFromNs, alias, forceNullable);
                    registerPathPattern(samePredicatePattern.getRight(), parentScope, samePredicateFromNs, alias, forceNullable);
                    
                    scopes.put(samePredicatePattern, samePredicateScope);
                    break;
                }
            default:
                super.registerOtherKindQuery(parentScope, usingScope, node, enclosingNode, alias,
                    forceNullable, checkUpdate);
        }
    }

    private SqlValidatorNamespace registerPathPattern(SqlNode sqlNode,
                                                      SqlValidatorScope parentScope,
                                                      SqlValidatorNamespace fromNs, String alias,
                                                      boolean forceNullable) {
        GQLPathPatternScope pathPatternScope = new GQLPathPatternScope(parentScope, (SqlCall) sqlNode);

        if (sqlNode instanceof SqlSamePredicatePattern) {
            SqlSamePredicatePattern samePredicatePattern = (SqlSamePredicatePattern) sqlNode;
            GQLSamePredicateNamespace pathNs = new GQLSamePredicateNamespace(this, samePredicatePattern);
            registerNamespace(null, alias, pathNs, forceNullable);
            
            // Register left and right path patterns
            registerPathPattern(samePredicatePattern.getLeft(), parentScope, fromNs, alias, forceNullable);
            registerPathPattern(samePredicatePattern.getRight(), parentScope, fromNs, alias, forceNullable);
            
            scopes.put(samePredicatePattern, pathPatternScope);
            return pathNs;
        }
        
        if (sqlNode instanceof SqlUnionPathPattern) {
            SqlUnionPathPattern unionPathPattern = (SqlUnionPathPattern) sqlNode;
            GQLUnionPathPatternNamespace pathNs =
                new GQLUnionPathPatternNamespace(this, unionPathPattern);
            registerNamespace(null, alias, pathNs, forceNullable);
            registerPathPattern(unionPathPattern.getLeft(), parentScope,
                fromNs, alias, forceNullable);
            registerPathPattern(unionPathPattern.getRight(), parentScope,
                fromNs, alias, forceNullable);

            scopes.put(unionPathPattern, pathPatternScope);
            return pathNs;
        }
        SqlPathPattern pathPattern = (SqlPathPattern) sqlNode;
        GQLPathPatternNamespace pathNs = new GQLPathPatternNamespace(this, pathPattern);
        registerNamespace(null, null, pathNs, forceNullable);

        String pathPatternAlias = alias == null ? deriveAlias(pathPattern) : alias;
        pathPatternScope.addChild(fromNs, pathPatternAlias, forceNullable);
        scopes.put(pathPattern, pathPatternScope);
        //register MatchNode
        for (SqlNode matchNode : pathPattern.getPathNodes()) {
            SqlMatchNode sqlMatchNode = (SqlMatchNode) matchNode;
            GQLScope nodeScope = new GQLScope(pathPatternScope, sqlMatchNode);
            GQLMatchNodeNamespace nodeNs = new GQLMatchNodeNamespace(this, sqlMatchNode);
            registerNamespace(nodeScope, deriveAlias(matchNode), nodeNs, forceNullable);
            scopes.put(matchNode, nodeScope);
            if (sqlMatchNode.getWhere() != null) {
                SqlNode nodeWhere = sqlMatchNode.getWhere();
                //Where condition can only access NodeScope, not MatchPatternScope
                GQLScope nodeWhereScope = new GQLScope(parentScope, nodeWhere);

                GQLMatchNodeWhereNamespace nodeWhereNs =
                    new GQLMatchNodeWhereNamespace(this, matchNode, nodeNs);
                nodeWhereScope.addChild(nodeWhereNs, sqlMatchNode.getName(), forceNullable);
                scopes.put(nodeWhere, nodeWhereScope);
            }
        }
        return pathNs;
    }

    @Override
    protected SqlNode registerOtherFrom(SqlValidatorScope parentScope,
                                        SqlValidatorScope usingScope,
                                        boolean register,
                                        final SqlNode node,
                                        SqlNode enclosingNode,
                                        String alias,
                                        SqlNodeList extendList,
                                        boolean forceNullable,
                                        final boolean lateral) {
        switch (node.getKind()) {
            case GQL_RETURN:
            case GQL_FILTER:
            case GQL_MATCH_PATTERN:
            case GQL_LET:
                if (alias == null) {
                    alias = deriveAlias(node);
                }
                registerQuery(
                    parentScope,
                    register ? usingScope : null,
                    node,
                    enclosingNode,
                    alias,
                    forceNullable);
                return node;
            default:
                return super.registerOtherFrom(parentScope, usingScope, register, node,
                    enclosingNode, alias, extendList, forceNullable, lateral);
        }
    }

    @Override
    protected SqlValidatorScope getWithBodyScope(SqlValidatorScope parentScope, SqlWith with) {
        if (GQLNodeUtil.containMatch(with)) {
            GQLScope withBodyScope = new GQLWithBodyScope(parentScope, with.withList);
            if (with.withList.size() != 1) {
                throw new GeaFlowDSLException(with.getParserPosition().toString(), "Only support one with item");
            }
            for (SqlNode withItem : with.withList) {
                SqlValidatorNamespace withItemNs = getNamespace(withItem);
                String withName = ((SqlWithItem) withItem).name.getSimple();
                withBodyScope.addChild(withItemNs, withName, false);
            }
            scopes.put(with.withList, withBodyScope);
            return withBodyScope;
        }
        return super.getWithBodyScope(parentScope, with);
    }

    public void registerScope(SqlNode sqlNode, SqlValidatorScope scope) {
        scopes.put(sqlNode, scope);
    }

    @Override
    public void inferUnknownTypes(RelDataType inferredType, SqlValidatorScope scope, SqlNode node) {
        super.inferUnknownTypes(inferredType, scope, node);
    }

    @Override
    public void validateNamespace(final SqlValidatorNamespace namespace,
                                  RelDataType targetRowType) {
        super.validateNamespace(namespace, targetRowType);
    }

    @Override
    protected void checkFieldCount(SqlNode node, SqlValidatorTable table,
                                   SqlNode source, RelDataType logicalSourceRowType,
                                   RelDataType logicalTargetRowType) {
        if (!(logicalTargetRowType instanceof GraphRecordType)) {
            super.checkFieldCount(node, table, source, logicalSourceRowType, logicalTargetRowType);
        }
    }

    private void registerGqlSubQuery(SqlValidatorScope parentScope, String alias,
                                     SqlValidatorNamespace fromNs, SqlNode node) {
        if (node.getKind() == SqlKind.GQL_PATH_PATTERN_SUB_QUERY) {
            SqlPathPatternSubQuery subQuery = (SqlPathPatternSubQuery) node;
            GQLSubQueryNamespace ns = new GQLSubQueryNamespace(this, subQuery);
            registerNamespace(null, null, ns, true);
            GQLScope subQueryScope = new GQLSubQueryScope(parentScope, subQuery);
            subQueryScope.addChild(fromNs, alias, true);
            scopes.put(subQuery, subQueryScope);

            SqlValidatorNamespace pathPatternNs =
                registerPathPattern(subQuery.getPathPattern(), subQueryScope, fromNs, alias, true);
            if (subQuery.getReturnValue() != null) {
                SqlNode returnValue = subQuery.getReturnValue();
                GQLScope returnValueScope = new GQLScope(parentScope, returnValue);
                returnValueScope.addChild(pathPatternNs, deriveAlias(subQuery.getPathPattern()), true);
                scopes.put(returnValue, returnValueScope);
            }
        } else if (node instanceof SqlCall) {
            SqlCall call = (SqlCall) node;
            for (SqlNode operand : call.getOperandList()) {
                if (operand != null) {
                    registerGqlSubQuery(parentScope, alias, fromNs, operand);
                }
            }
        } else if (node instanceof SqlNodeList) {
            SqlNodeList nodes = (SqlNodeList) node;
            for (SqlNode item : nodes.getList()) {
                registerGqlSubQuery(parentScope, alias, fromNs, item);
            }
        }
    }

    public SqlMatchNode getStartCycleMatchNode(SqlMatchNode node) {
        return renamedMatchNodes.get(node);
    }

    public SqlValidatorScope getScopes(SqlNode node) {
        return scopes.get(getOriginal(node));
    }

    public RelDataType getMatchNodeType(SqlMatchNode matchNode) {
        RelDataType nodeType = matchNodeTypes.get(matchNode);
        assert nodeType != null;
        return nodeType;
    }

    public void registerMatchNodeType(SqlMatchNode matchNode, RelDataType nodeType) {
        matchNodeTypes.put(matchNode, nodeType);
    }

    public GQLContext getGQLContext() {
        return gContext;
    }

    public String deriveAlias(SqlNode node) {
        if (node instanceof SqlIdentifier && ((SqlIdentifier) node).isSimple()) {
            return ((SqlIdentifier) node).getSimple();
        }
        if (node.getKind() == SqlKind.AS) {
            return ((SqlCall) node).operand(1).toString();
        }
        return ANONYMOUS_COLUMN_PREFIX + nextGeneratedId++;
    }

    public String anonymousMatchNodeName(boolean isVertex) {
        if (isVertex) {
            return "v_" + ANONYMOUS_COLUMN_PREFIX + nextGeneratedId++;
        }
        return "e_" + ANONYMOUS_COLUMN_PREFIX + nextGeneratedId++;
    }

    public SqlSelect asSqlSelect(SqlReturnStatement returnStmt) {
        return new SqlSelect(SqlParserPos.ZERO, null,
            returnStmt.getReturnList(), null, null,
            returnStmt.getGroupBy(), null, null, returnStmt.getOrderList(), null,
            null);
    }

    public SqlSelect asSqlSelect(SqlNodeList selectItems) {
        return new SqlSelect(SqlParserPos.ZERO, null, selectItems,
            null, null, null, null, null, null,
            null, null);
    }

    public SqlNode getAggregate(SqlNodeList sqlNodeList) {
        return super.getAggregate(asSqlSelect(sqlNodeList));
    }

    @Override
    protected SqlNode performUnconditionalRewrites(SqlNode node, boolean underFrom) {
        if (node instanceof SqlMatchPattern) {
            SqlMatchPattern matchPattern = (SqlMatchPattern) node;
            if (matchPattern.getFrom() == null) {
                if (gContext.getCurrentGraph() == null) {
                    throw new GeaFlowDSLException(matchPattern.getParserPosition(),
                        "Missing 'from graph' for match");
                }
                // Set current graph to from if not exists.
                SqlIdentifier usingGraphId = new SqlIdentifier(gContext.getCurrentGraph(),
                    matchPattern.getParserPosition());
                matchPattern.setFrom(usingGraphId);
            }
            List<SqlNode> nodes = matchPattern.getOperandList();
            for (int i = 0; i < nodes.size(); i++) {
                SqlNode operand = nodes.get(i);
                SqlNode newOperand = performUnconditionalRewrites(operand, underFrom);
                if (newOperand != operand) {
                    matchPattern.setOperand(i, newOperand);
                }
            }
            return matchPattern;
        } else if (node instanceof SqlGraphAlgorithmCall) {
            SqlGraphAlgorithmCall graphAlgorithmCall = (SqlGraphAlgorithmCall) node;
            if (graphAlgorithmCall.getFrom() == null) {
                if (gContext.getCurrentGraph() == null) {
                    throw new GeaFlowDSLException(graphAlgorithmCall.getParserPosition().toString(),
                        "Missing 'from graph' for graph algorithm call");
                }
                // Set current graph to from if not exists.
                SqlIdentifier usingGraphId = new SqlIdentifier(gContext.getCurrentGraph(),
                    graphAlgorithmCall.getParserPosition());
                graphAlgorithmCall.setFrom(usingGraphId);
            }
            return graphAlgorithmCall;
        } else if (node instanceof SqlUnionPathPattern) {
            SqlUnionPathPattern unionPathPattern = (SqlUnionPathPattern) node;
            return new SqlUnionPathPattern(unionPathPattern.getParserPosition(),
                performUnconditionalRewrites(unionPathPattern.getLeft(), underFrom),
                performUnconditionalRewrites(unionPathPattern.getRight(), underFrom),
                unionPathPattern.isDistinct());
        } else if (node instanceof SqlPathPattern) {
            SqlPathPattern pathPattern = (SqlPathPattern) node;
            if (pathPattern.getPathAliasName() == null) {
                SqlIdentifier pathAlias = new SqlIdentifier("p_" + nextGeneratedId++,
                    pathPattern.getParserPosition());
                pathPattern.setPathAlias(pathAlias);
            }
            for (int i = 0; i < pathPattern.getPathNodes().size(); i++) {
                SqlMatchNode sqlMatchNode = (SqlMatchNode) pathPattern.getPathNodes().get(i);
                if (sqlMatchNode.getName() == null) {
                    String nodeName = anonymousMatchNodeName(
                        sqlMatchNode.getKind() == SqlKind.GQL_MATCH_NODE);
                    SqlParserPos pos = sqlMatchNode.getParserPosition();
                    sqlMatchNode.setName(new SqlIdentifier(nodeName, pos));
                }
            }
            renameCycleMatchNode(pathPattern);
            return super.performUnconditionalRewrites(pathPattern, underFrom);
        } else if (isExistsPathPattern(node)) {
            // Rewrite "where exists (a) - (b)" to
            // "where count((a) - (b) => a) > 0"
            SqlPathPatternSubQuery subQuery = (SqlPathPatternSubQuery) performUnconditionalRewrites(
                ((SqlBasicCall) node).getOperands()[0], underFrom);
            subQuery.setReturnValue(subQuery.getPathPattern().getFirst().getNameId());

            SqlNode count = SqlStdOperatorTable.COUNT.createCall(node.getParserPosition(), subQuery);
            SqlNode zero = SqlLiteral.createExactNumeric("0", node.getParserPosition());
            return SqlStdOperatorTable.GREATER_THAN.createCall(node.getParserPosition(), count, zero);
        } else if (node != null && node.getKind() == SqlKind.MOD) {
            SqlBasicCall mod = (SqlBasicCall) node;
            mod.setOperator(GeaFlowOverwriteSqlOperators.MOD);
            return mod;
        } else if (node instanceof SqlInsert) {
            // complete the insert target table name.
            // e.g. "insert into g.v" will replace to "insert into instance.g.v"
            SqlInsert insert = (SqlInsert) node;
            SqlIdentifier completeId = gContext.completeCatalogObjName((SqlIdentifier) insert.getTargetTable());
            insert.setTargetTable(completeId);
            return super.performUnconditionalRewrites(insert, underFrom);
        }
        return super.performUnconditionalRewrites(node, underFrom);
    }

    private void renameCycleMatchNode(SqlPathPattern pathPattern) {
        Map<String, List<SqlMatchNode>> name2MatchNodes = new HashMap<>();
        for (int i = 0; i < pathPattern.getPathNodes().size(); i++) {
            SqlMatchNode sqlMatchNode = (SqlMatchNode) pathPattern.getPathNodes().get(i);
            if (sqlMatchNode.getKind() != SqlKind.GQL_MATCH_NODE) {
                continue;
            }
            String oldName = sqlMatchNode.getName();
            if (name2MatchNodes.containsKey(oldName)) {
                sqlMatchNode.setName(new SqlIdentifier(
                    oldName + RECURRING_COLUMN_SUFFIX + name2MatchNodes.get(oldName).size(),
                    sqlMatchNode.getParserPosition()));
                name2MatchNodes.get(oldName).add(sqlMatchNode);
                if (name2MatchNodes.get(oldName).size() > 1) {
                    renamedMatchNodes.put(sqlMatchNode, name2MatchNodes.get(oldName).get(0));
                }
            } else {
                name2MatchNodes.put(sqlMatchNode.getName(), new ArrayList<>());
                name2MatchNodes.get(oldName).add(sqlMatchNode);
            }
        }
    }

    @Override
    protected RelDataType getLogicalTargetRowType(
        RelDataType targetRowType,
        SqlInsert insert) {
        RelDataType targetType = super.getLogicalTargetRowType(targetRowType, insert);
        if (targetType instanceof VertexRecordType) {
            List<RelDataTypeField> fields = new ArrayList<>(targetType.getFieldList());
            fields.remove(VertexType.LABEL_FIELD_POSITION);
            targetType = new RelRecordType(fields);
        } else if (targetType instanceof EdgeRecordType) {
            List<RelDataTypeField> fields = new ArrayList<>(targetType.getFieldList());
            fields.remove(EdgeType.LABEL_FIELD_POSITION);
            targetType = new RelRecordType(fields);
        }
        return targetType;
    }

    @Override
    protected RelDataType createTargetRowType(
        SqlValidatorTable table,
        SqlNodeList targetColumnList,
        boolean append) {
        GeaFlowGraph graph = table.unwrap(GeaFlowGraph.class);
        if (graph != null) { // for insert g
            GraphRecordType graphType = (GraphRecordType) graph.getRowType(getTypeFactory());
            if (targetColumnList == null || targetColumnList.size() == 0) {
                throw new GeaFlowDSLException("Missing target columns for insert graph statement");
            }
            for (SqlNode targetColumn : targetColumnList) {
                List<String> names = ((SqlIdentifier) targetColumn).names;
                RelDataTypeField field = graphType.getField(names, isCaseSensitive());
                if (field == null) {
                    throw new GeaFlowDSLException(targetColumn.getParserPosition().toString(),
                        "Insert field: {} is not found in graph: {}", targetColumn, graph.getName());
                }
            }
            return graphType;
        }
        return super.createTargetRowType(table, targetColumnList, append);
    }

    @Override
    protected void checkTypeAssignment(
        RelDataType sourceRowType,
        RelDataType targetRowType,
        final SqlNode query) {
        if (targetRowType instanceof GraphRecordType) { // for insert g
            GraphRecordType graphType = (GraphRecordType) targetRowType;
            SqlInsert insert = (SqlInsert) query;
            for (int i = 0; i < insert.getTargetColumnList().size(); i++) {
                SqlIdentifier targetColumn = (SqlIdentifier) insert.getTargetColumnList().get(i);
                List<String> names = targetColumn.names;
                RelDataTypeField targetField = graphType.getField(names, isCaseSensitive());
                RelDataTypeField sourceField = sourceRowType.getFieldList().get(i);

                if (!SqlTypeUtil.canAssignFrom(targetField.getType(), sourceField.getType())) {
                    throw newValidationError(targetColumn,
                        RESOURCE.typeNotAssignable(
                            targetField.getName(), targetField.getType().getFullTypeString(),
                            sourceField.getName(), sourceField.getType().getFullTypeString()));
                }
            }
        } else {
            super.checkTypeAssignment(sourceRowType, targetRowType, query);
        }
    }

    private boolean isExistsPathPattern(SqlNode node) {
        return node != null
            && node.getKind() == SqlKind.EXISTS
            && ((SqlBasicCall) node).getOperands().length == 1
            && ((SqlBasicCall) node).getOperands()[0] instanceof SqlPathPatternSubQuery
            ;
    }

    public SqlNode expandReturnGroupOrderExpr(SqlReturnStatement returnStmt,
                                              SqlValidatorScope scope, SqlNode orderExpr) {
        SqlNode newSqlNode =
            (new ReturnGroupOrderExpressionExpander(returnStmt, scope, orderExpr)).go();
        if (newSqlNode != orderExpr) {
            this.inferUnknownTypes(this.unknownType, scope, newSqlNode);
            RelDataType type = this.deriveType(scope, newSqlNode);
            this.setValidatedNodeType(newSqlNode, type);
        }
        return newSqlNode;
    }

    class ReturnGroupOrderExpressionExpander extends SqlScopedShuttle {

        private final List<String> aliasList;
        private final SqlReturnStatement returnStmt;
        private final SqlNode root;

        ReturnGroupOrderExpressionExpander(SqlReturnStatement returnStmt,
                                           SqlValidatorScope scope, SqlNode root) {
            super(scope);
            this.returnStmt = returnStmt;
            this.root = root;
            this.aliasList = getNamespace(returnStmt).getRowType().getFieldNames();
        }

        public SqlNode go() {
            return this.root.accept(this);
        }

        public SqlNode visit(SqlLiteral literal) {
            if (literal == this.root && getConformance().isSortByOrdinal()) {
                switch (literal.getTypeName()) {
                    case DECIMAL:
                    case DOUBLE:
                        int intValue = literal.intValue(false);
                        if (intValue >= 0) {
                            if (intValue >= 1 && intValue <= this.aliasList.size()) {
                                int ordinal = intValue - 1;
                                return this.nthSelectItem(ordinal, literal.getParserPosition());
                            }

                            throw newValidationError(literal,
                                RESOURCE.orderByOrdinalOutOfRange());
                        }
                        break;
                    default:
                }
            }

            return super.visit(literal);
        }

        private SqlNode nthSelectItem(int ordinal, SqlParserPos pos) {
            SqlNodeList expandedReturnList = returnStmt.getReturnList();
            SqlNode expr = expandedReturnList.get(ordinal);
            SqlNode exprx = SqlUtil.stripAs(expr);
            if (exprx instanceof SqlIdentifier) {
                exprx = this.getScope().fullyQualify((SqlIdentifier) exprx).identifier;
            }

            return exprx.clone(pos);
        }

        public SqlNode visit(SqlIdentifier id) {
            if (id.isSimple() && getConformance().isSortByAlias()) {
                String alias = id.getSimple();
                SqlValidatorNamespace selectNs = getNamespace(returnStmt);
                RelDataType rowType = selectNs.getRowTypeSansSystemColumns();
                SqlNameMatcher nameMatcher = getCatalogReader().nameMatcher();
                RelDataTypeField field = nameMatcher.field(rowType, alias);
                if (field != null) {
                    return this.nthSelectItem(field.getIndex(), id.getParserPosition());
                }
            }
            //Replace if alias exists
            int size = id.names.size();
            final SqlIdentifier prefix = id.getComponent(0, 1);
            String alias = prefix.getSimple();
            SqlValidatorNamespace selectNs = getNamespace(returnStmt);
            RelDataType rowType = selectNs.getRowTypeSansSystemColumns();
            SqlNameMatcher nameMatcher = getCatalogReader().nameMatcher();
            RelDataTypeField field = nameMatcher.field(rowType, alias);
            if (field != null) {
                SqlNode identifierNewPrefix = this.nthSelectItem(field.getIndex(),
                    id.getParserPosition());
                assert identifierNewPrefix instanceof SqlIdentifier : "At " + id.getParserPosition()
                    + " : Prefix in OrderBy should be identifier.";
                List<String> newIdList = new ArrayList<>();
                newIdList.addAll(((SqlIdentifier) identifierNewPrefix).names);
                newIdList.addAll(id.getComponent(1, size).names);
                return new SqlIdentifier(newIdList, id.getParserPosition());
            } else {
                return this.getScope().fullyQualify(id).identifier;
            }
        }

        protected SqlNode visitScoped(SqlCall call) {
            return call instanceof SqlSelect ? call : super.visitScoped(call);
        }
    }

    public boolean isCaseSensitive() {
        return getCatalogReader().nameMatcher().isCaseSensitive();
    }

    public SqlNameMatcher nameMatcher() {
        return getCatalogReader().nameMatcher();
    }

    public QueryNodeContext getCurrentQueryNodeContext() {
        return currentQueryNodeContext;
    }

    public void addModifyGraphType(SqlLetStatement letStatement, GraphRecordType modifyGraphType) {
        let2ModifyGraphType.put(letStatement, modifyGraphType);
    }

    public GraphRecordType getModifyGraphType(SqlLetStatement letStatement) {
        return let2ModifyGraphType.get(letStatement);
    }
}
