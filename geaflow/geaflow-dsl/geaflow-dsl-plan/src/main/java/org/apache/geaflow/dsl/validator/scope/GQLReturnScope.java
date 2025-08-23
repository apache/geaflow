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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.geaflow.dsl.rel.GQLToRelConverter.GQLAggChecker;
import org.apache.geaflow.dsl.sqlnode.SqlReturnStatement;

public class GQLReturnScope extends GQLScope {

    private final SqlReturnStatement returnStatement;

    private boolean containAgg = false;

    private List<SqlNode> expandedReturnList = null;

    private List<SqlNode> temporaryGroupExprList;

    public GQLReturnScope(SqlValidatorScope parent, SqlReturnStatement returnStatement) {
        super(parent, returnStatement);
        this.returnStatement = returnStatement;
    }

    public final Supplier<Resolved> resolved =
        Suppliers.memoize(() -> {
            assert temporaryGroupExprList == null;
            temporaryGroupExprList = new ArrayList<>();
            try {
                return resolve();
            } finally {
                temporaryGroupExprList = null;
            }
        })::get;

    public void setAggMode() {
        containAgg = true;
    }

    public List<SqlNode> getExpandedReturnList() {
        return expandedReturnList;
    }

    public void setExpandedReturnList(List<SqlNode> returnList) {
        expandedReturnList = returnList;
    }

    private Resolved resolve() {
        final ImmutableList.Builder<ImmutableList<ImmutableBitSet>> builder =
            ImmutableList.builder();
        List<SqlNode> extraExprs = ImmutableList.of();
        Map<Integer, Integer> groupExprProjection = ImmutableMap.of();
        if (returnStatement.getGroupBy() != null) {
            final SqlNodeList groupList = returnStatement.getGroupBy();
            final SqlValidatorUtil.GroupAnalyzer groupAnalyzer =
                new SqlValidatorUtil.GroupAnalyzer(temporaryGroupExprList);
            for (SqlNode groupExpr : groupList) {
                SqlValidatorUtil.analyzeGroupItem(this, groupAnalyzer, builder,
                    groupExpr);
            }
            extraExprs = groupAnalyzer.getExtraExprs();
            groupExprProjection = groupAnalyzer.getGroupExprProjection();
        }

        final Set<ImmutableBitSet> flatGroupSets =
            Sets.newTreeSet(ImmutableBitSet.COMPARATOR);
        for (List<ImmutableBitSet> groupSet : Linq4j.product(builder.build())) {
            flatGroupSets.add(ImmutableBitSet.union(groupSet));
        }

        // For GROUP BY (), we need a singleton grouping set.
        if (flatGroupSets.isEmpty()) {
            flatGroupSets.add(ImmutableBitSet.of());
        }

        return new Resolved(extraExprs, temporaryGroupExprList, flatGroupSets, groupExprProjection);
    }

    /**
     * Returns the expressions that are in the GROUP BY clause (or the SELECT
     * DISTINCT clause, if distinct) and that can therefore be referenced
     * without being wrapped in aggregate functions.
     *
     * <p>The expressions are fully-qualified, and any "*" in select clauses are
     * expanded.
     *
     * @return list of grouping expressions
     */
    private Pair<ImmutableList<SqlNode>, ImmutableList<SqlNode>> getGroupExprs() {
        if (returnStatement.getGroupBy() != null) {
            if (temporaryGroupExprList != null) {
                // we are in the middle of resolving
                return Pair.of(ImmutableList.of(),
                    ImmutableList.copyOf(temporaryGroupExprList));
            } else {
                final Resolved resolved = this.resolved.get();
                return Pair.of(resolved.extraExprList, resolved.groupExprList);
            }
        } else {
            return Pair.of(ImmutableList.of(), ImmutableList.of());
        }
    }

    private static boolean allContain(List<ImmutableBitSet> bitSets, int bit) {
        for (ImmutableBitSet bitSet : bitSets) {
            if (!bitSet.get(bit)) {
                return false;
            }
        }
        return true;
    }

    public boolean checkAggregateExpr(SqlNode expr, boolean deep) {
        // Fully-qualify any identifiers in expr.
        if (deep) {
            expr = validator.expand(expr, this);
        }

        // Make sure expression is valid, throws if not.
        Pair<ImmutableList<SqlNode>, ImmutableList<SqlNode>> pair = getGroupExprs();
        final GQLAggChecker aggChecker =
            new GQLAggChecker(validator, this, pair.left, pair.right, false);
        if (deep) {
            expr.accept(aggChecker);
        }

        // Return whether expression exactly matches one of the group
        // expressions.
        return aggChecker.isGroupExpr(expr);
    }

    public void validateExpr(SqlNode expr) {
        if (containAgg) {
            checkAggregateExpr(expr, true);
        } else {
            super.validateExpr(expr);
        }
    }


    public static class Resolved {
        public final ImmutableList<SqlNode> extraExprList;
        public final ImmutableList<SqlNode> groupExprList;
        public final ImmutableBitSet groupSet;
        public final ImmutableList<ImmutableBitSet> groupSets;
        public final Map<Integer, Integer> groupExprProjection;

        Resolved(List<SqlNode> extraExprList, List<SqlNode> groupExprList,
                 Iterable<ImmutableBitSet> groupSets,
                 Map<Integer, Integer> groupExprProjection) {
            this.extraExprList = ImmutableList.copyOf(extraExprList);
            this.groupExprList = ImmutableList.copyOf(groupExprList);
            this.groupSet = ImmutableBitSet.range(groupExprList.size());
            this.groupSets = ImmutableList.copyOf(groupSets);
            this.groupExprProjection = ImmutableMap.copyOf(groupExprProjection);
        }

        public boolean isNullable(int i) {
            return i < groupExprList.size() && !allContain(groupSets, i);
        }
    }
}
