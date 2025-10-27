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

package org.apache.geaflow.dsl.runtime.command;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.geaflow.dsl.common.binary.decoder.DefaultRowDecoder;
import org.apache.geaflow.dsl.common.binary.decoder.RowDecoder;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.optimize.rule.ProjectFieldPruneRule;
import org.apache.geaflow.dsl.planner.GQLContext;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.QueryResult;
import org.apache.geaflow.dsl.runtime.RDataView;
import org.apache.geaflow.dsl.runtime.plan.PhysicConvention;
import org.apache.geaflow.dsl.runtime.plan.PhysicRelNode;
import org.apache.geaflow.dsl.runtime.plan.converters.ConvertRules;
import org.apache.geaflow.dsl.util.SqlTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryCommand implements IQueryCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryCommand.class);

    private final SqlNode query;

    public QueryCommand(SqlNode query) {
        this.query = query;
    }

    @SuppressWarnings("unchecked")
    @Override
    public QueryResult execute(QueryContext context) {
        long startTs = System.currentTimeMillis();
        LOGGER.info("Execute query:\n{}", query);

        GQLContext gqlContext = context.getGqlContext();
        SqlNode validateQuery = gqlContext.validate(query);

        RelNode logicalPlan = gqlContext.toRelNode(validateQuery);
        LOGGER.info("Convert sql to logical plan:\n{}", RelOptUtil.toString(logicalPlan));

        RelNode optimizedNode = gqlContext.optimize(context.getLogicalRules(), logicalPlan);
        LOGGER.info("After optimize logical plan:\n{}", RelOptUtil.toString(optimizedNode));

        PhysicRelNode<?> physicNode = (PhysicRelNode<?>) gqlContext.transform(ConvertRules.TRANSFORM_RULES,
            optimizedNode, optimizedNode.getTraitSet().plus(PhysicConvention.INSTANCE).simplify());
        LOGGER.info("Convert to physic plan:\n{}", RelOptUtil.toString(physicNode));

        physicNode = (PhysicRelNode<?>) context.getPathAnalyzer().analyze(physicNode);
        LOGGER.info("After path analyzer:\n{}", RelOptUtil.toString(physicNode));
        RDataView dataView = physicNode.translate(context);
        context.setCurrentResultType(physicNode.getRowType());

        if (context.isCompile()) {
            long compileSpend = System.currentTimeMillis() - startTs;
            LOGGER.info("Finish compile query, spend:{}ms", compileSpend);
            return new QueryResult(dataView);
        }

        if (query.getKind() != SqlKind.INSERT) {
            List<Row> rows = (List<Row>) dataView.take(SqlTypeUtil.convertType(physicNode.getRowType()));
            RowDecoder rowDecoder =
                new DefaultRowDecoder((StructType) SqlTypeUtil.convertType(physicNode.getRowType()));
            List<Row> decodeRows = new ArrayList<>(rows.size());
            for (Row row : rows) {
                decodeRows.add(rowDecoder.decode(row));
            }
            long spend = System.currentTimeMillis() - startTs;
            LOGGER.info("Finish execute query, take records: {}, spend: {}ms", rows.size(), spend);
            return new QueryResult(decodeRows);
        }
        long spend = System.currentTimeMillis() - startTs;
        LOGGER.info("Finish execute query, spend: {}ms", spend);
        return new QueryResult(true);
    }

    @Override
    public SqlNode getSqlNode() {
        return query;
    }
}
