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

package org.apache.geaflow.dsl.rel;

import static org.apache.calcite.rel.RelFieldCollation.NullDirection.UNSPECIFIED;
import static org.apache.calcite.util.Static.RESOURCE;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.*;
import org.apache.calcite.sql2rel.AuxiliaryConverter;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.geaflow.dsl.calcite.GraphRecordType;
import org.apache.geaflow.dsl.calcite.MetaFieldType;
import org.apache.geaflow.dsl.calcite.MetaFieldType.MetaField;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.calcite.VertexRecordType;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.operator.SqlPathPatternOperator;
import org.apache.geaflow.dsl.rel.PathModify.PathModifyExpression;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphAlgorithm;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphMatch;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphModify;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphScan;
import org.apache.geaflow.dsl.rel.logical.LogicalParameterizedRelNode;
import org.apache.geaflow.dsl.rel.match.EdgeMatch;
import org.apache.geaflow.dsl.rel.match.IMatchNode;
import org.apache.geaflow.dsl.rel.match.LoopUntilMatch;
import org.apache.geaflow.dsl.rel.match.MatchDistinct;
import org.apache.geaflow.dsl.rel.match.MatchFilter;
import org.apache.geaflow.dsl.rel.match.MatchJoin;
import org.apache.geaflow.dsl.rel.match.MatchPathModify;
import org.apache.geaflow.dsl.rel.match.MatchPathSort;
import org.apache.geaflow.dsl.rel.match.MatchUnion;
import org.apache.geaflow.dsl.rel.match.SingleMatchNode;
import org.apache.geaflow.dsl.rel.match.SubQueryStart;
import org.apache.geaflow.dsl.rel.match.VertexMatch;
import org.apache.geaflow.dsl.rex.PathInputRef;
import org.apache.geaflow.dsl.rex.RexLambdaCall;
import org.apache.geaflow.dsl.rex.RexObjectConstruct;
import org.apache.geaflow.dsl.rex.RexObjectConstruct.VariableInfo;
import org.apache.geaflow.dsl.rex.RexParameterRef;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.GraphElementTable;
import org.apache.geaflow.dsl.schema.function.GeaFlowUserDefinedGraphAlgorithm;
import org.apache.geaflow.dsl.sqlnode.AbstractSqlGraphElementConstruct;
import org.apache.geaflow.dsl.sqlnode.SqlFilterStatement;
import org.apache.geaflow.dsl.sqlnode.SqlGraphAlgorithmCall;
import org.apache.geaflow.dsl.sqlnode.SqlLetStatement;
import org.apache.geaflow.dsl.sqlnode.SqlMatchEdge;
import org.apache.geaflow.dsl.sqlnode.SqlMatchNode;
import org.apache.geaflow.dsl.sqlnode.SqlMatchPattern;
import org.apache.geaflow.dsl.sqlnode.SqlPathPattern;
import org.apache.geaflow.dsl.sqlnode.SqlPathPatternSubQuery;
import org.apache.geaflow.dsl.sqlnode.SqlReturnStatement;
import org.apache.geaflow.dsl.sqlnode.SqlUnionPathPattern;
import org.apache.geaflow.dsl.util.GQLNodeUtil;
import org.apache.geaflow.dsl.util.GQLRelUtil;
import org.apache.geaflow.dsl.util.GQLRexUtil;
import org.apache.geaflow.dsl.validator.GQLValidatorImpl;
import org.apache.geaflow.dsl.validator.scope.GQLReturnScope;
import org.apache.geaflow.dsl.validator.scope.GQLScope;
import org.apache.geaflow.dsl.validator.scope.GQLWithBodyScope;

public class GQLToRelConverter extends SqlToRelConverter {

    private long queryIdCounter = 0;

    public GQLToRelConverter(ViewExpander viewExpander, SqlValidator validator,
                             CatalogReader catalogReader, RelOptCluster cluster,
                             SqlRexConvertletTable convertLetTable,
                             Config config) {
        super(viewExpander, validator, catalogReader, cluster, convertLetTable, config);
    }

    @Override
    protected RelRoot convertQueryRecursive(SqlNode query, boolean top,
                                            RelDataType targetRowType) {
        return RelRoot.of(convertQueryRecursive(query, top, targetRowType, null), query.getKind());
    }

    private RelNode convertQueryRecursive(SqlNode query, boolean top,
                                          RelDataType targetRowType, Blackboard withBb) {
        SqlKind kind = query.getKind();
        switch (kind) {
            case GQL_FILTER:
                return convertGQLFilter((SqlFilterStatement) query, top, withBb);
            case GQL_RETURN:
                return convertGQLReturn((SqlReturnStatement) query, top, withBb);
            case GQL_MATCH_PATTERN:
                return convertGQLMatchPattern((SqlMatchPattern) query, top, withBb);
            case GQL_LET:
                return convertGQLLet((SqlLetStatement) query, top, withBb);
            case GQL_ALGORITHM:
                return convertGQLAlgorithm((SqlGraphAlgorithmCall) query, top, withBb);
            default:
                return super.convertQueryRecursive(query, top, targetRowType).rel;
        }
    }

    @Override
    public RelRoot convertWith(SqlWith with, boolean top) {
        boolean containMatch = GQLNodeUtil.containMatch(with.body);
        if (containMatch) {
            if (with.withList.size() > 1) {
                throw new GeaFlowDSLException("Multi-with list is not support for match at "
                    + with.getParserPosition());
            }
            SqlWithItem withItem = (SqlWithItem) with.withList.get(0);
            RelNode parameterNode = convertQueryRecursive(withItem.query, false, null).rel;
            Blackboard withBb = createBlackboard(getValidator().getScopes(withItem), null, false);
            withBb.setRoot(parameterNode, true);

            RelNode queryNode = convertQueryRecursive(with.body, false, null, withBb);
            RelNode parameterizedNode = LogicalParameterizedRelNode.create(getCluster(), getCluster().traitSet(),
                parameterNode, queryNode);
            return RelRoot.of(parameterizedNode, with.getKind());
        }
        return super.convertWith(with, top);
    }

    private RelNode convertGQLFilter(SqlFilterStatement filterStatement, boolean top, Blackboard withBb) {
        SqlValidatorScope scope = getValidator().getScopes(filterStatement);
        Blackboard bb = createBlackboard(scope, null, top).setWithBb(withBb);
        convertGQLFrom(bb, filterStatement.getFrom(), withBb);
        RexNode condition = bb.convertExpression(filterStatement.getCondition());
        return LogicalFilter.create(bb.root, condition);
    }

    private RelNode convertGQLReturn(SqlReturnStatement returnStatement, boolean top, Blackboard withBb) {
        assert returnStatement != null : "return statement is null";
        SqlValidatorScope scope = getValidator().getScopes(returnStatement);
        Blackboard bb = createBlackboard(scope, null, top).setWithBb(withBb);

        convertGQLFrom(bb, returnStatement.getFrom(), withBb);

        List<RelFieldCollation> collationList = new ArrayList<>();
        List<SqlNode> orderExprList = new ArrayList<>();
        if (returnStatement.getOrderBy() != null) {
            for (SqlNode orderItem : returnStatement.getOrderList()) {
                collationList.add(convertOrderItem(returnStatement, orderItem, orderExprList, Direction.ASCENDING,
                    UNSPECIFIED));
            }
        }
        RelCollation collation = cluster.traitSet().canonize(RelCollations.of(collationList));

        SqlNodeList groupBy = returnStatement.getGroupBy();
        if (((GQLValidatorImpl) validator).getAggregate(returnStatement.getReturnList()) != null
            || groupBy != null) {
            convertAgg(bb, returnStatement, orderExprList);
        } else {
            convertReturnList(bb, returnStatement, orderExprList);
        }
        SqlValidatorScope orderByScope =
            getValidator().getScopes(returnStatement.getOrderBy());
        Blackboard orderByBb = createBlackboard(orderByScope, null, false).setWithBb(withBb);
        orderByBb.setRoot(bb.root, false);
        convertOrder(getValidator().asSqlSelect(returnStatement), orderByBb,
            collation, orderExprList, returnStatement.getOffset(), returnStatement.getFetch());
        bb.setRoot(orderByBb.root, false);
        return bb.root;
    }

    private void convertReturnList(Blackboard bb,
                                   SqlReturnStatement returnStmt, List<SqlNode> orderList) {
        SqlNodeList returnList = returnStmt.getReturnList();
        //Return Star & SubQueries are not support.
        List<String> fieldNames = new ArrayList<>();
        List<RexNode> exprs = new ArrayList<>();
        Collection<String> aliases = new TreeSet<>();
        int i = -1;
        for (SqlNode node : returnList) {
            exprs.add(bb.convertExpression(node));
            fieldNames.add(deriveAlias(node, aliases, ++i));
        }

        SqlValidatorScope orderScope =
            getValidator().getScopes(returnStmt.getOrderBy());
        for (SqlNode node2 : orderList) {
            SqlNode expandExpr =
                ((GQLValidatorImpl) validator).expandReturnGroupOrderExpr(returnStmt,
                    orderScope, node2);
            exprs.add(bb.convertExpression(expandExpr));
            fieldNames.add(deriveAlias(node2, aliases, ++i));
        }
        fieldNames = SqlValidatorUtil.uniquify(fieldNames, this.catalogReader.nameMatcher().isCaseSensitive());
        RelDataType rowType = RexUtil.createStructType(this.cluster.getTypeFactory(), exprs, fieldNames,
            SqlValidatorUtil.F_SUGGESTER);
        bb.setRoot(LogicalProject.create(bb.root, exprs, rowType), true);

        if (returnStmt.isDistinct()) {
            convertDistinct(bb, true);
        }
    }

    private void convertAgg(Blackboard bb,
                            SqlReturnStatement returnStmt, List<SqlNode> orderList) {
        assert bb.root != null : "precondition: child != null when converting AGG";

        SqlNodeList returnList = returnStmt.getReturnList();
        final ReturnAggregateFinder aggregateFinder = new ReturnAggregateFinder();
        returnList.accept(aggregateFinder);
        GQLReturnScope scope = (GQLReturnScope) getValidator().getScopes(returnStmt);
        final GQLReturnScope.Resolved r = scope.resolved.get();
        final GQLAggConverter aggConverter = new GQLAggConverter(bb, returnStmt);
        for (SqlNode groupExpr : r.groupExprList) {
            aggConverter.addGroupExpr(groupExpr);
        }

        final List<Pair<RexNode, String>> projects = new ArrayList<>();
        try {
            Preconditions.checkArgument(bb.getAgg() == null, "already in agg mode");
            bb.setAgg(aggConverter);

            returnList.accept(aggConverter);
            // Assert we don't have dangling items left in the stack
            assert !aggConverter.inOver;
            for (SqlNode expr : orderList) {
                expr.accept(aggConverter);
                assert !aggConverter.inOver;
            }
            // compute inputs to the aggregator
            List<Pair<RexNode, String>> preExprs = aggConverter.getPreExprs();

            final RelNode inputRel = bb.root;

            // Project the expressions required by agg and having.
            bb.setRoot(
                getRelBuilder().push(inputRel)
                    .projectNamed(Pair.left(preExprs), Pair.right(preExprs), true)
                    .build(),
                false);
            // Add the aggregator
            bb.setRoot(
                createAggregate(bb, r.groupSet, r.groupSets,
                    aggConverter.getAggCalls()), false);

            // Now sub-queries in the entire select list have been converted.
            // Convert the select expressions to get the final list to be
            // projected.
            int k = 0;

            // For select expressions, use the field names previously assigned
            // by the validator. If we derive afresh, we might generate names
            // like "EXPR$2" that don't match the names generated by the
            // validator. This is especially the case when there are system
            // fields; system fields appear in the relnode's rowtype but do not
            // (yet) appear in the validator type.
            GQLReturnScope returnScope = null;
            SqlValidatorScope curScope = bb.scope;
            while (curScope instanceof DelegatingScope) {
                if (curScope instanceof GQLReturnScope) {
                    returnScope = (GQLReturnScope) curScope;
                    break;
                }
                curScope = ((DelegatingScope) curScope).getParent();
            }
            assert returnScope != null;

            final SqlValidatorNamespace returnNamespace =
                validator.getNamespace(returnScope.getNode());
            final List<String> names =
                returnNamespace.getRowType().getFieldNames();
            int sysFieldCount = returnList.size() - names.size();
            for (SqlNode expr : returnList) {
                projects.add(
                    Pair.of(bb.convertExpression(expr),
                        k < sysFieldCount
                            ? validator.deriveAlias(expr, k++)
                            : names.get(k++ - sysFieldCount)));
            }
            for (SqlNode expr : orderList) {
                projects.add(
                    Pair.of(bb.convertExpression(expr),
                        validator.deriveAlias(expr, k++)));
            }
        } finally {
            bb.setAgg(null);
        }

        getRelBuilder().push(bb.root);
        // implement the SELECT list
        getRelBuilder().project(Pair.left(projects), Pair.right(projects))
            .rename(Pair.right(projects));
        bb.setRoot(getRelBuilder().build(), true);
    }

    private RelNode convertGQLMatchPattern(SqlMatchPattern matchPattern, boolean top, Blackboard withBb) {
        SqlValidatorScope scope = getValidator().getScopes(matchPattern);
        Blackboard bb = createBlackboard(scope, null, top).setWithBb(withBb);
        convertGQLFrom(bb, matchPattern.getFrom(), withBb);

        SqlNodeList pathPatterns = matchPattern.getPathPatterns();

        List<IMatchNode> relPathPatterns = new ArrayList<>();
        List<PathRecordType> pathPatternTypes = new ArrayList<>();
        // concat path pattern
        for (SqlNode pathPattern : pathPatterns) {
            IMatchNode relPathPattern = convertPathPattern(pathPattern, withBb);
            PathRecordType pathType =
                pathPattern instanceof SqlUnionPathPattern
                    ? (PathRecordType) getValidator().getNamespace(pathPattern).getRowType() :
                    (PathRecordType) getValidator().getValidatedNodeType(pathPattern);
            // concat path pattern and type if then can connect in a line.
            concatPathPatterns(relPathPatterns, pathPatternTypes, relPathPattern, pathType);
        }

        IMatchNode joinPattern = null;
        for (IMatchNode relPathPattern : relPathPatterns) {
            if (joinPattern == null) {
                joinPattern = relPathPattern;
            } else {
                IMatchNode left = joinPattern;
                IMatchNode right = relPathPattern;
                RexNode condition = GQLRelUtil.createPathJoinCondition(left, right, isCaseSensitive(), rexBuilder);
                joinPattern = MatchJoin.create(left.getCluster(), left.getTraitSet(),
                    left, right, condition, JoinRelType.INNER);
            }
        }

        if (matchPattern.isDistinct()) {
            joinPattern = MatchDistinct.create(joinPattern);
        }

        RelDataType graphType = getValidator().getValidatedNodeType(matchPattern);
        GraphMatch graphMatch;
        if (bb.root instanceof GraphMatch) { // merge with pre graph match.
            GraphMatch input = (GraphMatch) bb.root;
            graphMatch = input.merge(joinPattern);
        } else {
            graphMatch = LogicalGraphMatch.create(getCluster(), bb.root, joinPattern, graphType);
        }
        if (matchPattern.getWhere() != null) {
            SqlNode where = matchPattern.getWhere();
            SqlValidatorScope whereScope = getValidator().getScopes(where);
            GQLBlackboard whereBb = createBlackboard(whereScope, null, false)
                .setWithBb(withBb);
            whereBb.setRoot(graphMatch, true);
            replaceSubQueries(whereBb, where, RelOptUtil.Logic.UNKNOWN_AS_FALSE);
            RexNode condition = whereBb.convertExpression(where);

            IMatchNode newPathPattern = MatchFilter.create(graphMatch.getPathPattern(),
                condition, graphMatch.getPathPattern().getPathSchema());
            graphMatch = graphMatch.copy(newPathPattern);
        }

        List<RexNode> orderByExpList = new ArrayList<>();
        if (matchPattern.getOrderBy() != null) {
            SqlValidatorScope orderScope = getValidator().getScopes(matchPattern.getOrderBy());
            Blackboard orderBb = createBlackboard(orderScope, null, top)
                .setWithBb(withBb);
            orderBb.setRoot(graphMatch, true);
            for (SqlNode orderItem : matchPattern.getOrderBy()) {
                orderByExpList.add(orderBb.convertExpression(orderItem));
            }
        }
        // convert match order
        SqlNode limit = matchPattern.getLimit();
        if (limit != null || orderByExpList.size() > 0) {
            MatchPathSort newPathPattern = MatchPathSort.create(graphMatch.getPathPattern(),
                orderByExpList, limit == null ? null : convertExpression(limit),
                graphMatch.getPathPattern().getPathSchema());

            graphMatch = graphMatch.copy(newPathPattern);
        }
        return graphMatch;
    }

    private void concatPathPatterns(List<IMatchNode> concatPatterns, List<PathRecordType> concatPathTypes,
                                    IMatchNode pathPattern, PathRecordType pathType) {
        if (concatPatterns.isEmpty() || !(pathPattern instanceof SingleMatchNode)) {
            concatPatterns.add(pathPattern);
            concatPathTypes.add(pathType);
            return;
        }
        SingleMatchNode singlePathPattern = (SingleMatchNode) pathPattern;
        String firstLabel = GQLRelUtil.getFirstMatchNode(singlePathPattern).getLabel();
        String latestLabel = GQLRelUtil.getLatestMatchNode(singlePathPattern).getLabel();
        int i;
        for (i = 0; i < concatPatterns.size(); i++) {
            IMatchNode pathPattern2 = concatPatterns.get(i);
            if (pathPattern2 instanceof SingleMatchNode) {
                String latestLabel2 = GQLRelUtil.getLatestMatchNode((SingleMatchNode) pathPattern2).getLabel();
                // pathPattern2 is "(a) - (b) - (c)", where singlePathPattern is "(c) - (d)"
                // Then concat pathPattern2 with singlePathPattern and return "(a) - (b) - (c) - (d)"
                if (getValidator().nameMatcher().matches(firstLabel, latestLabel2)) {
                    IMatchNode concatPattern = GQLRelUtil.concatPathPattern((SingleMatchNode) pathPattern2,
                        singlePathPattern, isCaseSensitive());
                    concatPatterns.set(i, concatPattern);

                    PathRecordType pathType2 = concatPathTypes.get(i);
                    PathRecordType concatPathType = pathType2.concat(pathType, isCaseSensitive());
                    concatPathTypes.set(i, concatPathType);
                    break;
                } else {
                    String firstLabel2 = GQLRelUtil.getFirstMatchNode((SingleMatchNode) pathPattern2).getLabel();
                    // singlePathPattern is "(a) - (b) - (c)", where pathPattern2 is "(c) - (d)"
                    // Then concat singlePathPattern with pathPattern2 and return "(a) - (b) - (c) - (d)"
                    if (getValidator().nameMatcher().matches(firstLabel2, latestLabel)) {
                        IMatchNode concatPattern = GQLRelUtil.concatPathPattern(singlePathPattern,
                            (SingleMatchNode) pathPattern2, isCaseSensitive());
                        concatPatterns.set(i, concatPattern);

                        PathRecordType pathType2 = concatPathTypes.get(i);
                        PathRecordType concatPathType = pathType.concat(pathType2, isCaseSensitive());
                        concatPathTypes.set(i, concatPathType);
                        break;
                    }
                }
            }
        }
        // If merge not happen, just add it.
        if (i == concatPatterns.size()) {
            concatPatterns.add(pathPattern);
            concatPathTypes.add(pathType);
        }
    }

    private SingleMatchNode convertMatchNodeWhere(SqlMatchNode matchNode, SqlNode matchWhere,
                                                  IMatchNode input, Blackboard withBb) {
        assert input != null;
        SqlValidatorScope whereScope = getValidator().getScopes(matchWhere);
        Blackboard nodeBb = createBlackboard(whereScope, null, false).setWithBb(withBb);
        nodeBb.setRoot(new WhereMatchNode(input), true);
        if (withBb != null) {
            nodeBb.addInput(withBb.root);
        }
        replaceSubQueries(nodeBb, matchWhere, RelOptUtil.Logic.UNKNOWN_AS_FALSE);
        RexNode condition = nodeBb.convertExpression(matchWhere);

        PathRecordType pathRecordType = input.getPathSchema();
        RelDataTypeField pathField = pathRecordType.getField(matchNode.getName(), isCaseSensitive(), false);
        condition = GQLRexUtil.toPathInputRefForWhere(pathField, condition);
        return MatchFilter.create(input, condition, input.getPathSchema());
    }

    private static class WhereMatchNode extends AbstractRelNode {

        public WhereMatchNode(IMatchNode matchNode) {
            super(matchNode.getCluster(), matchNode.getTraitSet());
            this.rowType = matchNode.getNodeType();
        }
    }

    private IMatchNode convertPathPattern(SqlNode sqlNode, Blackboard withBb) {
        if (sqlNode instanceof SqlUnionPathPattern) {
            SqlUnionPathPattern unionPathPattern = (SqlUnionPathPattern) sqlNode;
            IMatchNode left = convertPathPattern(unionPathPattern.getLeft(), withBb);
            IMatchNode right = convertPathPattern(unionPathPattern.getRight(), withBb);
            return MatchUnion.create(getCluster(), getCluster().traitSet(),
                Lists.newArrayList(left, right), unionPathPattern.isUnionAll());
        }
        SqlPathPattern pathPattern = (SqlPathPattern) sqlNode;
        SingleMatchNode relPathPattern = null;
        SqlMatchNode preMatchNode = null;
        for (SqlNode pathNode : pathPattern.getPathNodes()) {
            PathRecordType pathType = (PathRecordType) getValidator().getValidatedNodeType(pathNode);
            switch (pathNode.getKind()) {
                case GQL_MATCH_NODE:
                    SqlMatchNode matchNode = (SqlMatchNode) pathNode;
                    RelDataType nodeType = getValidator().getMatchNodeType(matchNode);

                    SingleMatchNode vertexMatch = VertexMatch.create(getCluster(), relPathPattern,
                        matchNode.getName(), matchNode.getLabelNames(), nodeType, pathType);
                    if (matchNode.getWhere() != null) {
                        vertexMatch = convertMatchNodeWhere(matchNode, matchNode.getWhere(),
                            vertexMatch, withBb);
                    }
                    // generate for regex match
                    if (preMatchNode instanceof SqlMatchEdge && ((SqlMatchEdge) preMatchNode).isRegexMatch()) {
                        SqlMatchEdge inputEdgeNode = (SqlMatchEdge) preMatchNode;
                        EdgeMatch inputEdgeMatch = (EdgeMatch) vertexMatch.find(node -> node instanceof EdgeMatch);
                        relPathPattern = convertRegexMatch(inputEdgeNode, inputEdgeMatch, vertexMatch);
                    } else {
                        relPathPattern = vertexMatch;
                    }
                    if (getValidator().getStartCycleMatchNode((SqlMatchNode) pathNode) != null) {
                        relPathPattern = translateCycleMatchNode(relPathPattern, pathNode);
                    }
                    break;
                case GQL_MATCH_EDGE:
                    SqlMatchEdge matchEdge = (SqlMatchEdge) pathNode;
                    RelDataType edgeType = getValidator().getMatchNodeType(matchEdge);

                    EdgeMatch edgeMatch = EdgeMatch.create(
                        getCluster(), relPathPattern,
                        matchEdge.getName(), matchEdge.getLabelNames(),
                        matchEdge.getDirection(), edgeType, pathType);

                    if (matchEdge.getWhere() != null) {
                        relPathPattern = convertMatchNodeWhere(matchEdge, matchEdge.getWhere(),
                            edgeMatch, withBb);
                    } else {
                        relPathPattern = edgeMatch;
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Illegal path node kind: " + pathNode.getKind());
            }
            preMatchNode = (SqlMatchNode) pathNode;
        }
        return relPathPattern;
    }

    private SingleMatchNode translateCycleMatchNode(SingleMatchNode relPathPattern, SqlNode pathNode) {
        PathRecordType pathRecordType = relPathPattern.getPathSchema();
        int rightIndex = pathRecordType.getField(((SqlMatchNode) pathNode).getName(),
            isCaseSensitive(), false).getIndex();
        int leftIndex = pathRecordType.getField(getValidator()
                .getStartCycleMatchNode((SqlMatchNode) pathNode).getName(),
            isCaseSensitive(), false).getIndex();
        assert leftIndex >= 0 && rightIndex >= 0;
        RexNode condition = getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS,
            getRexBuilder().makeInputRef(pathRecordType, leftIndex),
            getRexBuilder().makeInputRef(pathRecordType, rightIndex)
        );
        return MatchFilter.create(relPathPattern, condition,
            relPathPattern.getPathSchema());
    }

    private SingleMatchNode convertRegexMatch(SqlMatchEdge regexEdge, EdgeMatch regexEdgeMatch,
                                              SingleMatchNode vertexMatch) {
        IMatchNode loopStart = (IMatchNode) regexEdgeMatch.getInput();
        SubQueryStart queryStart = SubQueryStart.create(getCluster(),
            loopStart.getTraitSet(), generateSubQueryName(), loopStart.getPathSchema(),
            (VertexRecordType) loopStart.getNodeType());
        // replace the input of regexEdgeMatch to queryStart and clone vertexMatch
        SingleMatchNode loopBody = GQLRelUtil.replaceInput(vertexMatch, regexEdgeMatch, queryStart);

        RexNode utilCondition = getRexBuilder().makeLiteral(true);
        return LoopUntilMatch.create(getCluster(), vertexMatch.getTraitSet(), loopStart,
            loopBody, utilCondition, regexEdge.getMinHop(), regexEdge.getMaxHop(),
            vertexMatch.getPathSchema());
    }

    private RelNode convertGQLAlgorithm(SqlGraphAlgorithmCall algorithmCall, boolean top,
                                        Blackboard withBb) {

        SqlValidatorScope scope = getValidator().getScopes(algorithmCall);
        Blackboard bb = createBlackboard(scope, null, top).setWithBb(withBb);
        convertGQLFrom(bb, algorithmCall.getFrom(), withBb);

        Object[] params =
            algorithmCall.getParameters() == null ? new Object[0] : algorithmCall.getParameters().getList()
                .stream().map(sqlNode -> GQLRexUtil.getLiteralValue(bb.convertExpression(sqlNode)))
                .toArray();
        return LogicalGraphAlgorithm.create(cluster, getCluster().traitSet(),
            bb.root,
            ((GeaFlowUserDefinedGraphAlgorithm) algorithmCall.getOperator()).getImplementClass(),
            params);
    }

    private RelNode convertGQLLet(SqlLetStatement letStatement, boolean top, Blackboard withBb) {
        SqlValidatorScope scope = getValidator().getScopes(letStatement);
        Blackboard bb = createBlackboard(scope, null, top).setWithBb(withBb);
        convertGQLFrom(bb, letStatement.getFrom(), withBb);

        RexNode rightExpression = bb.convertExpression(letStatement.getExpression());

        PathRecordType letType = (PathRecordType) getValidator().getValidatedNodeType(letStatement);
        PathInputRef leftRex = convertLeftLabel(letStatement.getLeftLabel(), letType);

        String leftVarField = letStatement.getLeftField();
        List<RexNode> operands = new ArrayList<>();
        SqlIdentifier[] fieldNameNodes = new SqlIdentifier[leftRex.getType().getFieldCount()];
        Map<RexNode, VariableInfo> rex2VariableInfo = new HashMap<>();
        int c = 0;
        for (RelDataTypeField field : leftRex.getType().getFieldList()) {
            VariableInfo variableInfo;
            RexNode operand;
            if (getValidator().nameMatcher().matches(leftVarField, field.getName())) {
                // The field is the let a.xx
                boolean isGlobal = letStatement.isGlobal();
                // cast right expression to field type.
                operand = getRexBuilder().makeCast(field.getType(), rightExpression);
                variableInfo = new VariableInfo(isGlobal, field.getName());
            } else {
                RexInputRef labelRef = getRexBuilder().makeInputRef(leftRex.getType(), leftRex.getIndex());
                operand = getRexBuilder().makeFieldAccess(labelRef, field.getIndex());
                variableInfo = new VariableInfo(false, field.getName());
            }
            operands.add(operand);
            rex2VariableInfo.put(operand, variableInfo);
            fieldNameNodes[c++] = new SqlIdentifier(field.getName(), SqlParserPos.ZERO);
        }
        // Construct RexObjectConstruct for dynamic field append expression.
        RexObjectConstruct rightRex = new RexObjectConstruct(leftRex.getType(), operands, rex2VariableInfo);
        PathModifyExpression modifyExpression = new PathModifyExpression(leftRex, rightRex);
        GraphRecordType modifyGraphType = getValidator().getModifyGraphType(letStatement);

        RelNode input = bb.root;
        assert input instanceof GraphMatch;
        GraphMatch graphMatch = (GraphMatch) input;
        MatchPathModify newPathPattern = MatchPathModify.create(graphMatch.getPathPattern(),
            Collections.singletonList(modifyExpression), letType, modifyGraphType);
        return graphMatch.copy(newPathPattern);
    }

    private PathInputRef convertLeftLabel(String leftLabel, PathRecordType letType) {
        RelDataTypeField labelField = letType.getField(leftLabel, isCaseSensitive(), false);
        return new PathInputRef(leftLabel, labelField.getIndex(), labelField.getType());
    }

    private void convertGQLFrom(Blackboard bb, SqlNode from, Blackboard withBb) {
        RelNode node;
        switch (from.getKind()) {
            case GQL_MATCH_PATTERN:
                node = convertGQLMatchPattern((SqlMatchPattern) from, false, withBb);
                bb.setRoot(node, true);
                break;
            case GQL_RETURN:
                node = convertGQLReturn((SqlReturnStatement) from, false, withBb);
                bb.setRoot(node, true);
                break;
            case GQL_FILTER:
                node = convertGQLFilter((SqlFilterStatement) from, false, withBb);
                bb.setRoot(node, true);
                break;
            case IDENTIFIER:
                SqlIdentifier identifier = (SqlIdentifier) from;
                SqlIdentifier completeIdentifier = getValidator().getGQLContext()
                    .completeCatalogObjName(identifier);
                RelOptTable table = catalogReader.getTable(completeIdentifier.names);
                node = LogicalGraphScan.create(getCluster(), table);
                bb.setRoot(node, true);
                break;
            case GQL_LET:
                node = convertGQLLet((SqlLetStatement) from, false, withBb);
                bb.setRoot(node, true);
                break;
            case GQL_ALGORITHM:
                node = convertGQLAlgorithm((SqlGraphAlgorithmCall) from, false, withBb);
                bb.setRoot(node, true);
                break;
            default:
                throw new IllegalArgumentException("Illegal match from sql node: " + from.getKind());
        }
        if (withBb != null) {
            bb.addInput(withBb.root);
        }
    }

    @Override
    protected RelNode convertInsert(SqlInsert call) {
        RelOptTable targetTable = getTargetTable(call);
        Table table = targetTable.unwrap(Table.class);
        final RelDataType targetRowType =
            validator.getValidatedNodeType(call);
        assert targetRowType != null;

        RelNode sourceRel = convertQueryRecursive(call.getSource(), false, targetRowType).project();

        if (table instanceof GraphElementTable) {
            List<String> targetColumns;
            if (call.getTargetColumnList() != null) {
                targetColumns = call.getTargetColumnList().getList()
                    .stream().map(id -> ((SqlIdentifier) id).getSimple())
                    .collect(Collectors.toList());
            } else {
                targetColumns = new ArrayList<>();
                for (RelDataTypeField field : targetRowType.getFieldList()) {
                    targetColumns.add(field.getName());
                }
            }
            GraphElementTable graphElementTable = (GraphElementTable) table;
            GeaFlowGraph graph = graphElementTable.getGraph();
            RelDataType tableRowType = table.getRowType(validator.getTypeFactory());
            int[] targetColumnIndices = new int[tableRowType.getFieldCount()];
            for (int c = 0; c < tableRowType.getFieldList().size(); c++) {
                RelDataTypeField field = tableRowType.getFieldList().get(c);
                int i;
                for (i = 0; i < targetColumns.size(); i++) {
                    if (getValidator().nameMatcher().matches(targetColumns.get(i), field.getName())) {
                        break;
                    }
                }
                if (i < targetColumns.size()) {
                    targetColumnIndices[c] = i;
                } else { // -1 means the meta field
                    targetColumnIndices[c] = -1;
                }
            }
            RexObjectConstruct objConstruct = createObjectConstruct(tableRowType, sourceRel,
                graphElementTable, targetColumnIndices);
            return createGraphModify(graph, new String[]{graphElementTable.getTypeName()},
                new RexNode[]{objConstruct}, sourceRel);
        } else if (table instanceof GeaFlowGraph) {
            GeaFlowGraph graph = (GeaFlowGraph) table;
            Map<String, List<String>> vertexEdgeType2RefFields = new HashMap<>();
            Map<String, int[]> vertexEdgeType2ExpIndices = new HashMap<>();

            SqlNodeList targetColumns = call.getTargetColumnList();
            assert targetColumns != null && targetColumns.size() > 0;

            for (SqlNode targetColumn : targetColumns) {
                List<String> names = ((SqlIdentifier) targetColumn).names;
                assert names.size() == 2;
                String vertexEdgeTypeName = names.get(0);
                String fieldName = names.get(1);
                vertexEdgeType2RefFields.computeIfAbsent(vertexEdgeTypeName, k -> new ArrayList<>()).add(fieldName);
            }
            for (int c = 0; c < targetColumns.size(); c++) {
                SqlIdentifier targetColumn = (SqlIdentifier) targetColumns.get(c);
                List<String> names = targetColumn.names;
                String vertexEdgeTypeName = names.get(0);
                GraphElementTable vertexEdgeTable = graph.getTable(vertexEdgeTypeName);
                RelDataType tableRowType = vertexEdgeTable.getRowType(validator.getTypeFactory());
                int[] targetColumnExpIndices = vertexEdgeType2ExpIndices.computeIfAbsent(vertexEdgeTypeName,
                    k -> {
                        int[] indices = new int[tableRowType.getFieldCount()];
                        Arrays.fill(indices, -1);
                        return indices;
                    });
                String fieldName = names.get(1);
                int fieldIndex = tableRowType.getFieldNames().indexOf(fieldName);
                targetColumnExpIndices[fieldIndex] = c;
            }

            String[] typeNames = new String[vertexEdgeType2RefFields.size()];
            RexNode[] constructs = new RexNode[typeNames.length];
            int c = 0;
            RelDataType graphType = graph.getRowType(validator.getTypeFactory());

            for (int i = 0; i < graphType.getFieldCount(); i++) {
                String typeName = graphType.getFieldNames().get(i);
                GraphElementTable vertexEdgeTable = graph.getTable(typeName);
                RelDataType tableRowType = vertexEdgeTable.getRowType(validator.getTypeFactory());

                int[] targetColumnExpIndices = vertexEdgeType2ExpIndices.get(typeName);
                if (targetColumnExpIndices != null) {
                    RexObjectConstruct objConstruct = createObjectConstruct(tableRowType, sourceRel,
                        vertexEdgeTable, targetColumnExpIndices);
                    typeNames[c] = typeName;
                    constructs[c] = objConstruct;
                    c++;
                }
            }
            return createGraphModify(graph, typeNames, constructs, sourceRel);
        }
        return super.convertInsert(call);
    }

    private RexObjectConstruct createObjectConstruct(RelDataType tableRowType, RelNode sourceRel,
                                                     GraphElementTable table, int[] targetColumnExpIndices) {
        List<RexNode> columnExpressions = new ArrayList<>();
        List<RelDataTypeField> fields = tableRowType.getFieldList();

        for (int i = 0; i < fields.size(); i++) {
            RelDataTypeField field = fields.get(i);
            int fieldExpIndex = targetColumnExpIndices[i];
            if (fieldExpIndex != -1) {
                RelDataType sourceFieldType = sourceRel.getRowType().getFieldList().get(fieldExpIndex).getType();
                RelDataType targetFieldType = field.getType();
                RexNode inputRef = getRexBuilder().makeCast(targetFieldType,
                    getRexBuilder().makeInputRef(sourceFieldType, fieldExpIndex));
                columnExpressions.add(inputRef);
            } else if (field.getType() instanceof MetaFieldType
                && ((MetaFieldType) field.getType()).getMetaField() == MetaField.VERTEX_TYPE) {
                RexLiteral vertexLabel = getRexBuilder().makeLiteral(table.getTypeName());
                columnExpressions.add(getRexBuilder().makeCast(field.getType(), vertexLabel));
            } else if (field.getType() instanceof MetaFieldType
                && ((MetaFieldType) field.getType()).getMetaField() == MetaField.EDGE_TYPE) {
                RexLiteral edgeLabel = getRexBuilder().makeLiteral(table.getTypeName());
                columnExpressions.add(getRexBuilder().makeCast(field.getType(), edgeLabel));
            } else {
                RexLiteral nullLiteral = getRexBuilder().makeNullLiteral(field.getType());
                columnExpressions.add(nullLiteral);
            }
        }
        Map<RexNode, VariableInfo> rex2VarInfo = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            VariableInfo variableInfo = new VariableInfo(false, fields.get(i).getName());
            rex2VarInfo.put(columnExpressions.get(i), variableInfo);
        }
        return new RexObjectConstruct(tableRowType, columnExpressions, rex2VarInfo);
    }

    private GraphModify createGraphModify(GeaFlowGraph graph, String[] typeNames,
                                          RexNode[] constructs, RelNode sourceRel) {
        GraphRecordType graphType = (GraphRecordType) graph.getRowType(validator.getTypeFactory());
        List<RexNode> projects = new ArrayList<>();
        for (RelDataTypeField field : graphType.getFieldList()) {
            int i;
            for (i = 0; i < typeNames.length; i++) {
                if (getValidator().nameMatcher().matches(field.getName(), typeNames[i])) {
                    break;
                }
            }
            if (i == typeNames.length) {
                projects.add(getRexBuilder().makeNullLiteral(field.getType()));
            } else {
                projects.add(constructs[i]);
            }
        }
        LogicalProject project = LogicalProject.create(sourceRel, projects, graphType);
        return LogicalGraphModify.create(project.getCluster(), graph, project);
    }

    @Override
    protected void convertFrom(
        Blackboard bb,
        SqlNode from) {
        RelNode relNode;
        if (from == null) {
            super.convertFrom(bb, null);
            return;
        }
        switch (from.getKind()) {
            case GQL_RETURN:
            case GQL_FILTER:
                relNode = convertQueryRecursive(from, false, null).rel;
                bb.setRoot(relNode, false);
                break;
            case GQL_ALGORITHM:
            case GQL_MATCH_PATTERN:
            case GQL_LET:
            case WITH:
                relNode = convertQueryRecursive(from, false, null).rel;
                bb.setRoot(relNode, true);
                break;
            default:
                super.convertFrom(bb, from);
        }
    }

    private String deriveAlias(SqlNode node, Collection<String> aliases, int ordinal) {
        String alias = this.validator.deriveAlias(node, ordinal);
        if (alias == null || aliases.contains(alias)) {
            String aliasBase = alias == null ? "EXPR$" : alias;
            int j = 0;

            while (true) {
                alias = aliasBase + j;
                if (!aliases.contains(alias)) {
                    break;
                }

                ++j;
            }
        }

        aliases.add(alias);
        return alias;
    }

    private static boolean desc(Direction direction) {
        switch (direction) {
            case DESCENDING:
            case STRICTLY_DESCENDING:
                return true;
            default:
                return false;
        }
    }

    protected RelFieldCollation convertOrderItem(SqlReturnStatement returnStmt, SqlNode orderItem,
                                                 List<SqlNode> extraExprs, Direction direction,
                                                 NullDirection nullDirection) {
        assert returnStmt != null;

        switch (orderItem.getKind()) {
            case DESCENDING:
                return this.convertOrderItem(returnStmt, ((SqlCall) orderItem).operand(0), extraExprs,
                    Direction.DESCENDING, nullDirection);
            case NULLS_FIRST:
                return this.convertOrderItem(returnStmt, ((SqlCall) orderItem).operand(0), extraExprs, direction,
                    NullDirection.FIRST);
            case NULLS_LAST:
                return this.convertOrderItem(returnStmt, ((SqlCall) orderItem).operand(0), extraExprs, direction,
                    NullDirection.LAST);
            default:
                SqlValidatorScope orderScope = getValidator().getScopes(returnStmt.getOrderBy());
                SqlNode converted = ((GQLValidatorImpl) validator).expandReturnGroupOrderExpr(returnStmt, orderScope,
                    orderItem);
                if (nullDirection == UNSPECIFIED) {
                    nullDirection = this.validator.getDefaultNullCollation().last(desc(direction))
                        ? NullDirection.LAST : NullDirection.FIRST;
                }
                GQLReturnScope returnScope =
                    (GQLReturnScope) getValidator().getScopes(returnStmt);
                int ordinal = -1;
                Iterator<SqlNode> returnListItr = returnScope.getExpandedReturnList().iterator();

                SqlNode extraExpr;
                do {
                    if (!returnListItr.hasNext()) {
                        returnListItr = extraExprs.iterator();

                        do {
                            if (!returnListItr.hasNext()) {
                                extraExprs.add(converted);
                                return new RelFieldCollation(ordinal + 1, direction, nullDirection);
                            }

                            extraExpr = returnListItr.next();
                            ++ordinal;
                        } while (!converted.equalsDeep(extraExpr, Litmus.IGNORE));

                        return new RelFieldCollation(ordinal, direction, nullDirection);
                    }

                    extraExpr = returnListItr.next();
                    ++ordinal;
                } while (!converted.equalsDeep(SqlUtil.stripAs(extraExpr), Litmus.IGNORE));

                return new RelFieldCollation(ordinal, direction, nullDirection);
        }
    }

    private static class ReturnAggregateFinder extends SqlBasicVisitor<Void> {

        final SqlNodeList list = new SqlNodeList(SqlParserPos.ZERO);

        @Override
        public Void visit(SqlCall call) {
            if (call.getOperator().isAggregator()) {
                list.add(call);
                return null;
            }
            return call.getOperator().acceptCall(this, call);
        }
    }

    private void convertDistinct(Blackboard bb, boolean checkForDupExprs) {
        // Look for duplicate expressions in the project.
        // Say we have 'select x, y, x, z'.
        // Then dups will be {[2, 0]}
        // and oldToNew will be {[0, 0], [1, 1], [2, 0], [3, 2]}
        RelNode rel = bb.root;
        if (checkForDupExprs && (rel instanceof LogicalProject)) {
            LogicalProject project = (LogicalProject) rel;
            final List<RexNode> projectExprs = project.getProjects();
            final List<Integer> origins = new ArrayList<>();
            int dupCount = 0;
            for (int i = 0; i < projectExprs.size(); i++) {
                int x = projectExprs.indexOf(projectExprs.get(i));
                if (x >= 0 && x < i) {
                    origins.add(x);
                    ++dupCount;
                } else {
                    origins.add(i);
                }
            }
            if (dupCount == 0) {
                convertDistinct(bb, false);
                return;
            }

            final Map<Integer, Integer> squished = new HashMap<>();
            final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
            final List<Pair<RexNode, String>> newProjects = new ArrayList<>();
            for (int i = 0; i < fields.size(); i++) {
                if (origins.get(i) == i) {
                    squished.put(i, newProjects.size());
                    newProjects.add(RexInputRef.of2(i, fields));
                }
            }
            rel =
                LogicalProject.create(rel, Pair.left(newProjects),
                    Pair.right(newProjects));
            bb.root = rel;
            convertDistinct(bb, false);
            rel = bb.root;

            // Create the expressions to reverse the mapping.
            // Project($0, $1, $0, $2).
            final List<Pair<RexNode, String>> undoProjects = new ArrayList<>();
            for (int i = 0; i < fields.size(); i++) {
                final int origin = origins.get(i);
                RelDataTypeField field = fields.get(i);
                undoProjects.add(Pair.of(
                    new RexInputRef(squished.get(origin), field.getType()), field.getName()));
            }

            rel =
                LogicalProject.create(rel, Pair.left(undoProjects),
                    Pair.right(undoProjects));
            bb.setRoot(
                rel,
                false);
            return;
        }

        // Usual case: all of the expressions in the SELECT clause are
        // different.
        final ImmutableBitSet groupSet =
            ImmutableBitSet.range(rel.getRowType().getFieldCount());
        rel = createAggregate(bb, groupSet, ImmutableList.of(groupSet),
            ImmutableList.of());

        bb.setRoot(
            rel,
            false);
    }

    public class GQLAggConverter extends AggConverter implements SqlVisitor<Void> {

        private final Blackboard bb;
        public final GQLReturnScope gqlReturnScope;

        private final Map<String, String> nameMap = new HashMap<>();

        /**
         * The group-by expressions, in {@link SqlNode} format.
         */
        private final SqlNodeList groupExprs =
            new SqlNodeList(SqlParserPos.ZERO);

        /**
         * The auxiliary group-by expressions.
         */
        private final Map<SqlNode, Ord<AuxiliaryConverter>> auxiliaryGroupExprs =
            new HashMap<>();

        /**
         * Input expressions for the group columns and aggregates, in
         * {@link RexNode} format. The first elements of the list correspond to the
         * elements in {@link #groupExprs}; the remaining elements are for
         * aggregates. The right field of each pair is the name of the expression,
         * where the expressions are simple mappings to input fields.
         */
        private final List<Pair<RexNode, String>> convertedInputExprs =
            new ArrayList<>();

        /**
         * Expressions to be evaluated as rows are being placed into the
         * aggregate's hash table. This is when group functions such as TUMBLE
         * cause rows to be expanded.
         */

        private final List<AggregateCall> aggCalls = new ArrayList<>();
        private final Map<SqlNode, RexNode> aggMapping = new HashMap<>();
        private final Map<AggregateCall, RexNode> aggCallMapping =
            new HashMap<>();

        private boolean inOver = false;

        public GQLAggConverter(Blackboard bb, SqlReturnStatement returnStatement) {
            super(bb);
            this.bb = bb;
            this.gqlReturnScope = (GQLReturnScope) getValidator().getScopes(returnStatement);

            // Collect all expressions used in the select list so that aggregate
            // calls can be named correctly.
            final SqlNodeList returnList = returnStatement.getReturnList();
            for (int i = 0; i < returnList.size(); i++) {
                SqlNode returnItem = returnList.get(i);
                String name = null;
                if (SqlUtil.isCallTo(
                    returnItem,
                    SqlStdOperatorTable.AS)) {
                    final SqlCall call = (SqlCall) returnItem;
                    returnItem = call.operand(0);
                    name = call.operand(1).toString();
                }
                if (name == null) {
                    name = validator.deriveAlias(returnItem, i);
                }
                nameMap.put(returnItem.toString(), name);
            }
        }

        public int addGroupExpr(SqlNode expr) {
            int ref = lookupGroupExpr(expr);
            if (ref >= 0) {
                return ref;
            }
            final int index = groupExprs.size();
            groupExprs.add(expr);
            String name = nameMap.get(expr.toString());
            RexNode convExpr = bb.convertExpression(expr);
            addExpr(convExpr, name);

            if (expr instanceof SqlCall) {
                SqlCall call = (SqlCall) expr;
                for (Pair<SqlNode, AuxiliaryConverter> p
                    : SqlStdOperatorTable.convertGroupToAuxiliaryCalls(call)) {
                    addAuxiliaryGroupExpr(p.left, index, p.right);
                }
            }

            return index;
        }

        void addAuxiliaryGroupExpr(SqlNode node, int index,
                                   AuxiliaryConverter converter) {
            for (SqlNode node2 : auxiliaryGroupExprs.keySet()) {
                if (node2.equalsDeep(node, Litmus.IGNORE)) {
                    return;
                }
            }
            auxiliaryGroupExprs.put(node, Ord.of(index, converter));
        }

        /**
         * Adds an expression, deducing an appropriate name if possible.
         *
         * @param expr Expression
         * @param name Suggested name
         */
        private void addExpr(RexNode expr, String name) {
            if ((name == null) && (expr instanceof RexInputRef)) {
                final int i = ((RexInputRef) expr).getIndex();
                name = bb.root.getRowType().getFieldList().get(i).getName();
            }
            if (Pair.right(convertedInputExprs).contains(name)) {
                // In case like 'SELECT ... GROUP BY x, y, x', don't add
                // name 'x' twice.
                name = null;
            }
            convertedInputExprs.add(Pair.of(expr, name));
        }

        public Void visit(SqlIdentifier id) {
            return null;
        }

        public Void visit(SqlNodeList nodeList) {
            for (int i = 0; i < nodeList.size(); i++) {
                nodeList.get(i).accept(this);
            }
            return null;
        }

        public Void visit(SqlLiteral lit) {
            return null;
        }

        public Void visit(SqlDataTypeSpec type) {
            return null;
        }

        public Void visit(SqlDynamicParam param) {
            return null;
        }

        public Void visit(SqlIntervalQualifier intervalQualifier) {
            return null;
        }

        public Void visit(SqlCall call) {
            switch (call.getKind()) {
                case FILTER:
                case WITHIN_GROUP:
                    translateAgg(call);
                    return null;
                case SELECT:
                    // rchen 2006-10-17:
                    // for now do not detect aggregates in sub-queries.
                    return null;
                default:
            }
            final boolean prevInOver = inOver;
            // Ignore window aggregates and ranking functions (associated with OVER
            // operator). However, do not ignore nested window aggregates.
            if (call.getOperator().getKind() == SqlKind.OVER) {
                // Track aggregate nesting levels only within an OVER operator.
                List<SqlNode> operandList = call.getOperandList();
                assert operandList.size() == 2;

                // Ignore the top level window aggregates and ranking functions
                // positioned as the first operand of a OVER operator
                inOver = true;
                operandList.get(0).accept(this);

                // Normal translation for the second operand of a OVER operator
                inOver = false;
                operandList.get(1).accept(this);
                return null;
            }

            // Do not translate the top level window aggregate. Only do so for
            // nested aggregates, if present
            if (call.getOperator().isAggregator()) {
                if (inOver) {
                    // Add the parent aggregate level before visiting its children
                    inOver = false;
                } else {
                    // We're beyond the one ignored level
                    translateAgg(call);
                    return null;
                }
            }
            for (SqlNode operand : call.getOperandList()) {
                // Operands are occasionally null, e.g. switched CASE arg 0.
                if (operand != null) {
                    operand.accept(this);
                }
            }
            // Remove the parent aggregate level after visiting its children
            inOver = prevInOver;
            return null;
        }

        private void translateAgg(SqlCall call) {
            translateAgg(call, null, null, call);
        }

        private void translateAgg(SqlCall call, SqlNode filter,
                                  SqlNodeList orderList, SqlCall outerCall) {
            assert bb.getAgg() == this;
            assert outerCall != null;
            switch (call.getKind()) {
                case FILTER:
                    assert filter == null;
                    translateAgg(call.operand(0), call.operand(1), orderList, outerCall);
                    return;
                case WITHIN_GROUP:
                    assert orderList == null;
                    translateAgg(call.operand(0), filter, call.operand(1), outerCall);
                    return;
                default:
            }
            final List<Integer> args = new ArrayList<>();
            int filterArg = -1;
            final List<RelDataType> argTypes =
                call.getOperator() instanceof SqlCountAggFunction
                    ? new ArrayList<>(call.getOperandList().size())
                    : null;
            try {
                // switch out of agg mode
                bb.setAgg(null);
                for (SqlNode operand : call.getOperandList()) {

                    // special case for COUNT(*):  delete the *
                    if (operand instanceof SqlIdentifier) {
                        SqlIdentifier id = (SqlIdentifier) operand;
                        if (id.isStar()) {
                            assert call.operandCount() == 1;
                            assert args.isEmpty();
                            break;
                        }
                    }
                    RexNode convertedExpr = bb.convertExpression(operand);
                    assert convertedExpr != null;
                    if (argTypes != null) {
                        argTypes.add(convertedExpr.getType());
                    }
                    args.add(lookupOrCreateGroupExpr(convertedExpr));
                }

                if (filter != null) {
                    RexNode convertedExpr = bb.convertExpression(filter);
                    assert convertedExpr != null;
                    if (convertedExpr.getType().isNullable()) {
                        convertedExpr =
                            rexBuilder.makeCall(SqlStdOperatorTable.IS_TRUE, convertedExpr);
                    }
                    filterArg = lookupOrCreateGroupExpr(convertedExpr);
                }
            } finally {
                // switch back into agg mode
                bb.setAgg(this);
            }

            SqlAggFunction aggFunction =
                (SqlAggFunction) call.getOperator();
            final RelDataType type = validator.deriveType(bb.scope, call);
            boolean distinct = false;
            SqlLiteral quantifier = call.getFunctionQuantifier();
            if ((null != quantifier)
                && (quantifier.getValue() == SqlSelectKeyword.DISTINCT)) {
                distinct = true;
            }
            boolean approximate = false;
            if (aggFunction == SqlStdOperatorTable.APPROX_COUNT_DISTINCT) {
                aggFunction = SqlStdOperatorTable.COUNT;
                distinct = true;
                approximate = true;
            }
            final RelCollation collation;
            if (orderList == null || orderList.size() == 0) {
                collation = RelCollations.EMPTY;
            } else {
                collation = RelCollations.of(
                    orderList.getList()
                        .stream()
                        .map(order ->
                            bb.convertSortExpression(order,
                                RelFieldCollation.Direction.ASCENDING,
                                RelFieldCollation.NullDirection.UNSPECIFIED))
                        .map(fieldCollation ->
                            new RelFieldCollation(
                                lookupOrCreateGroupExpr(fieldCollation.left),
                                fieldCollation.getDirection(),
                                fieldCollation.getNullDirection()))
                        .collect(Collectors.toList()));
            }
            final AggregateCall aggCall =
                AggregateCall.create(
                    aggFunction,
                    distinct,
                    approximate,
                    args,
                    filterArg,
                    collation,
                    type,
                    nameMap.get(outerCall.toString()));

            gqlReturnScope.resolved.get();
            RexNode rex =
                rexBuilder.addAggCall(
                    aggCall,
                    groupExprs.size(),
                    false,
                    aggCalls,
                    aggCallMapping,
                    argTypes);
            aggMapping.put(outerCall, rex);
        }

        private int lookupOrCreateGroupExpr(RexNode expr) {
            int index = 0;
            for (RexNode convertedInputExpr : Pair.left(convertedInputExprs)) {
                if (expr.equals(convertedInputExpr)) {
                    return index;
                }
                ++index;
            }

            // not found -- add it
            addExpr(expr, null);
            return index;
        }

        /**
         * If an expression is structurally identical to one of the group-by
         * expressions, returns a reference to the expression, otherwise returns
         * null.
         */
        public int lookupGroupExpr(SqlNode expr) {
            for (int i = 0; i < groupExprs.size(); i++) {
                SqlNode groupExpr = groupExprs.get(i);
                if (expr.equalsDeep(groupExpr, Litmus.IGNORE)) {
                    return i;
                }
            }
            return -1;
        }

        public RexNode lookupAggregates(SqlCall call) {
            // assert call.getOperator().isAggregator();
            assert bb.getAgg() == this;

            for (Map.Entry<SqlNode, Ord<AuxiliaryConverter>> e
                : auxiliaryGroupExprs.entrySet()) {
                if (call.equalsDeep(e.getKey(), Litmus.IGNORE)) {
                    AuxiliaryConverter converter = e.getValue().e;
                    final int groupOrdinal = e.getValue().i;
                    return converter.convert(rexBuilder,
                        convertedInputExprs.get(groupOrdinal).left,
                        rexBuilder.makeInputRef(bb.root, groupOrdinal));
                }
            }

            return aggMapping.get(call);
        }

        public List<Pair<RexNode, String>> getPreExprs() {
            return convertedInputExprs;
        }

        public List<AggregateCall> getAggCalls() {
            return aggCalls;
        }

        public RelDataTypeFactory getTypeFactory() {
            return typeFactory;
        }

    }

    public static class GQLAggChecker extends SqlBasicVisitor<Void> {
        //~ Instance fields --------------------------------------------------------

        private final Deque<SqlValidatorScope> scopes = new ArrayDeque<>();
        private final List<SqlNode> extraExprs;
        private final List<SqlNode> groupExprs;
        private final boolean distinct;
        private final SqlValidatorImpl validator;

        //~ Constructors -----------------------------------------------------------

        /**
         * Creates an AggChecker.
         *
         * @param validator  Validator
         * @param scope      Scope
         * @param groupExprs Expressions in GROUP BY (or SELECT DISTINCT) clause, that are therefore available
         * @param distinct   Whether aggregation checking is because of a SELECT DISTINCT clause
         */
        public GQLAggChecker(
            SqlValidatorImpl validator,
            SqlValidatorScope scope,
            List<SqlNode> extraExprs,
            List<SqlNode> groupExprs,
            boolean distinct) {
            this.validator = validator;
            this.extraExprs = extraExprs;
            this.groupExprs = groupExprs;
            this.distinct = distinct;
            this.scopes.push(scope);
        }

        //~ Methods ----------------------------------------------------------------

        public boolean isGroupExpr(SqlNode expr) {
            for (SqlNode groupExpr : groupExprs) {
                if (groupExpr.equalsDeep(expr, Litmus.IGNORE)) {
                    return true;
                }
            }

            for (SqlNode extraExpr : extraExprs) {
                if (extraExpr.equalsDeep(expr, Litmus.IGNORE)) {
                    return true;
                }
            }
            return false;
        }

        public Void visit(SqlIdentifier id) {
            if (isGroupExpr(id) || id.isStar()) {
                // Star may validly occur in "SELECT COUNT(*) OVER w"
                return null;
            }

            // Is it a call to a parentheses-free function?
            SqlCall call =
                SqlUtil.makeCall(
                    validator.getOperatorTable(),
                    id);
            if (call != null) {
                return call.accept(this);
            }

            // Didn't find the identifier in the group-by list as is, now find
            // it fully-qualified.
            // TODO: It would be better if we always compared fully-qualified
            // to fully-qualified.
            assert scopes.peek() != null : "GQLToRelConverter has no scopes";
            final SqlQualified fqId = scopes.peek().fullyQualify(id);
            if (isGroupExpr(fqId.identifier)) {
                return null;
            }
            SqlNode originalExpr = validator.getOriginal(id);
            final String exprString = originalExpr.toString();
            throw validator.newValidationError(originalExpr,
                distinct
                    ? RESOURCE.notSelectDistinctExpr(exprString)
                    : RESOURCE.notGroupExpr(exprString));
        }

        public Void visit(SqlCall call) {
            assert scopes.peek() != null : "GQLToRelConverter has no scopes";
            final SqlValidatorScope scope = scopes.peek();
            if (call.getOperator().isAggregator()) {
                return null;
            }
            if (isGroupExpr(call)) {
                // This call matches an expression in the GROUP BY clause.
                return null;
            }

            final SqlCall groupCall =
                SqlStdOperatorTable.convertAuxiliaryToGroupCall(call);
            if (groupCall != null) {
                if (isGroupExpr(groupCall)) {
                    // This call is an auxiliary function that matches a group call in the
                    // GROUP BY clause.
                    //
                    // For example TUMBLE_START is an auxiliary of the TUMBLE
                    // group function, and
                    //   TUMBLE_START(rowtime, INTERVAL '1' HOUR)
                    // matches
                    //   TUMBLE(rowtime, INTERVAL '1' HOUR')
                    return null;
                }
                throw validator.newValidationError(groupCall,
                    RESOURCE.auxiliaryWithoutMatchingGroupCall(
                        call.getOperator().getName(), groupCall.getOperator().getName()));
            }

            if (call.isA(SqlKind.QUERY)) {
                // Allow queries for now, even though they may contain
                // references to forbidden columns.
                return null;
            }

            // Switch to new scope.
            SqlValidatorScope newScope = scope.getOperandScope(call);
            scopes.push(newScope);

            // Visit the operands (only expressions).
            call.getOperator()
                .acceptCall(this, call, true, ArgHandlerImpl.instance());

            // Restore scope.
            scopes.pop();
            return null;
        }
    }

    private GQLValidatorImpl getValidator() {
        return (GQLValidatorImpl) validator;
    }

    public class GQLBlackboard extends Blackboard {

        private Blackboard withBb;

        protected GQLBlackboard(SqlValidatorScope scope,
                                Map<String, RexNode> nameToNodeMap, boolean top) {
            super(scope, nameToNodeMap, top);
        }

        public GQLBlackboard setWithBb(Blackboard withBb) {
            this.withBb = withBb;
            return this;
        }

        @Override
        public RexNode convertExpression(SqlNode expr) {
            RexNode rexNode;
            switch (expr.getKind()) {
                case VERTEX_VALUE_CONSTRUCTOR:
                case EDGE_VALUE_CONSTRUCTOR:
                    AbstractSqlGraphElementConstruct construct = (AbstractSqlGraphElementConstruct) expr;
                    SqlNode[] valueNodes = construct.getValueNodes();
                    SqlIdentifier[] keyNodes = construct.getKeyNodes();
                    List<RexNode> operands = new ArrayList<>();
                    Map<RexNode, VariableInfo> rex2VariableInfo = new HashMap<>();

                    for (int i = 0; i < valueNodes.length; i++) {
                        SqlNode valueNode = valueNodes[i];
                        RexNode operand = convertExpression(valueNode);
                        VariableInfo variableInfo = new VariableInfo(false, keyNodes[i].getSimple());
                        operands.add(operand);
                        rex2VariableInfo.put(operand, variableInfo);
                    }

                    RelDataType type = getValidator().getValidatedNodeType(expr);
                    rexNode = new RexObjectConstruct(type, operands, rex2VariableInfo);
                    break;
                case GQL_PATH_PATTERN_SUB_QUERY:
                    SqlPathPatternSubQuery subQuery = (SqlPathPatternSubQuery) expr;

                    SqlValidatorNamespace ns = getValidator().getNamespace(subQuery.getPathPattern());
                    assert ns.getType() instanceof PathRecordType;
                    PathRecordType pathRecordType = (PathRecordType) ns.getType();
                    assert pathRecordType.getFieldCount() > 0;

                    IMatchNode matchNode = convertPathPattern(subQuery.getPathPattern(), withBb);
                    assert matchNode instanceof SingleMatchNode : "Sub-query should be single path match";
                    IMatchNode firstNode = GQLRelUtil.getFirstMatchNode((SingleMatchNode) matchNode);

                    assert pathRecordType.firstField().isPresent() : "Path type is empty";
                    VertexRecordType firstFieldType = (VertexRecordType) pathRecordType.firstField().get().getType();
                    // SubQueryStart's path schema is the same with the first match node in the suq-query.
                    SubQueryStart subQueryStart = SubQueryStart.create(matchNode.getCluster(), matchNode.getTraitSet(),
                        generateSubQueryName(), firstNode.getPathSchema(), firstFieldType);
                    // add sub query start node to the head of the match node.
                    matchNode = GQLRelUtil.addSubQueryStartNode(matchNode, subQueryStart);

                    SqlNode returnValue = subQuery.getReturnValue();
                    SqlNode originReturnValue = GQLToRelConverter.this.getValidator().getOriginal(returnValue);
                    GQLScope returnValueScope = (GQLScope) GQLToRelConverter.this.getValidator()
                        .getScopes(originReturnValue);

                    GQLBlackboard bb = new GQLBlackboard(returnValueScope, null, true);
                    bb.setRoot(matchNode, true);
                    RexNode returnValueNode = bb.convertExpression(returnValue);

                    RexSubQuery pathPatternSubQuery = RexSubQuery.create(matchNode.getPathSchema(),
                        SqlPathPatternOperator.INSTANCE, ImmutableList.of(), matchNode);
                    rexNode = new RexLambdaCall(pathPatternSubQuery, returnValueNode);
                    break;
                default:
                    rexNode = super.convertExpression(expr);
                    break;
            }
            return convertRexParameterRef(scope, rexNode);
        }
    }

    private RexNode convertRexParameterRef(SqlValidatorScope scope, RexNode rexNode) {
        if (scope instanceof ListScope
            && ((ListScope) scope).getParent() instanceof GQLWithBodyScope) {
            GQLWithBodyScope withScope = (GQLWithBodyScope) ((ListScope) scope).getParent();

            return rexNode.accept(new RexShuttle() {
                @Override
                public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                    // replace CorrelVariable to ParameterRef for referring the with-body fields.
                    if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
                        SqlValidatorNamespace withItemNs = withScope.children.get(0).getNamespace();
                        return new RexParameterRef(fieldAccess.getField().getIndex(),
                            fieldAccess.getType(), withItemNs.getType());
                    }
                    return fieldAccess;
                }
            });
        }
        return rexNode;
    }

    @Override
    protected GQLBlackboard createBlackboard(SqlValidatorScope scope,
                                             Map<String, RexNode> nameToNodeMap, boolean top) {
        return new GQLBlackboard(scope, nameToNodeMap, top);
    }

    public boolean isCaseSensitive() {
        return getValidator().isCaseSensitive();
    }

    private String generateSubQueryName() {
        return "SubQuery-" + queryIdCounter++;
    }
}
