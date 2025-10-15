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

package org.apache.geaflow.dsl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.optimize.GQLOptimizer;
import org.apache.geaflow.dsl.optimize.RuleGroup;
import org.apache.geaflow.dsl.parser.GeaFlowDSLParser;
import org.apache.geaflow.dsl.planner.GQLContext;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.schema.GeaFlowTable;
import org.apache.geaflow.dsl.sqlnode.SqlCreateGraph;
import org.apache.geaflow.dsl.sqlnode.SqlCreateTable;
import org.testng.Assert;

public class PlanTester {

    public static final String defaultGraphDDL =
        "create graph g0("
            + "vertex user("
            + " id bigint ID,"
            + "name varchar,"
            + "age integer"
            + "),"
            + "edge knows("
            + " src_id bigint SOURCE ID,"
            + " target_id bigint DESTINATION ID,"
            + " weight double"
            + ")"
            + ")";

    public static final String defaultTableDDL =
        "create table t0 (id bigint, name varchar, age int)";

    private String gql;

    private SqlNode validateNode;

    private Exception validateException;

    private RelNode relNode;

    private final GeaFlowDSLParser parser = new GeaFlowDSLParser();

    private final GQLContext gqlContext = GQLContext.create(new Configuration(), false);

    private PlanTester() {
        try {
            GeaFlowDSLParser parser = new GeaFlowDSLParser();
            SqlCreateGraph createGraph = (SqlCreateGraph) parser.parseStatement(defaultGraphDDL);
            GeaFlowGraph graph = gqlContext.convertToGraph(createGraph);
            gqlContext.registerGraph(graph);
            gqlContext.setCurrentGraph(graph.getName());

            SqlCreateTable createTable = (SqlCreateTable) parser.parseStatement(defaultTableDDL);
            GeaFlowTable table = gqlContext.convertToTable(createTable);
            gqlContext.registerTable(table);
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    public PlanTester registerGraph(String ddl) {
        try {
            GeaFlowDSLParser parser = new GeaFlowDSLParser();
            SqlCreateGraph createGraph = (SqlCreateGraph) parser.parseStatement(ddl);
            GeaFlowGraph graph = gqlContext.convertToGraph(createGraph);
            gqlContext.registerGraph(graph);
            gqlContext.setCurrentGraph(graph.getName());
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            this.validateException = e;
        }
        return this;
    }

    public PlanTester setCurrentGraph(String graphName) {
        gqlContext.setCurrentGraph(graphName);
        return this;
    }

    public static PlanTester build() {
        return new PlanTester();
    }

    public PlanTester gql(String gql) {
        if (this.gql != null) {
            throw new IllegalArgumentException("Duplicate setting for gql.");
        }
        this.gql = gql;
        return this;
    }

    public PlanTester validate() {
        if (validateException != null || validateNode != null) {
            return this;
        }
        try {
            SqlNode sqlNode = parser.parseStatement(gql);
            validateNode = gqlContext.validate(sqlNode);
        } catch (Exception e) {
            this.validateException = e;
        }
        return this;
    }

    public PlanTester toRel() {
        if (relNode != null) {
            return this;
        }
        validate();
        if (validateException == null) {
            this.relNode = gqlContext.toRelNode(validateNode);
        }
        return this;
    }

    public PlanTester opt(RelOptRule... rules) {
        if (relNode == null) {
            return this;
        }
        GQLOptimizer optimizer = new GQLOptimizer();
        List<RelOptRule> useRuleList = new ArrayList<>(Arrays.asList(rules));
        RuleGroup useRules = new RuleGroup(useRuleList);
        optimizer.addRuleGroup(useRules);
        this.relNode = optimizer.optimize(relNode);
        return this;
    }

    public void expectValidateType(String expectType) {
        if (validateException != null) {
            throw new GeaFlowDSLException(validateException);
        }
        RelDataType dataType = gqlContext.getValidator().getValidatedNodeType(validateNode);
        Assert.assertEquals(dataType.toString(), expectType);
    }

    public void expectException(String expectErrorMsg) {
        Assert.assertNotNull(validateException);
        Assert.assertEquals(validateException.getMessage(), expectErrorMsg);
    }

    public PlanTester checkRelNode(String expectPlan) {
        if (validateException != null) {
            throw new GeaFlowDSLException(validateException);
        }
        String actualPlan = RelOptUtil.toString(relNode);
        Assert.assertEquals(actualPlan, expectPlan);
        return this;
    }

    public String getDefaultGraphDDL() {
        return defaultGraphDDL;
    }
}
