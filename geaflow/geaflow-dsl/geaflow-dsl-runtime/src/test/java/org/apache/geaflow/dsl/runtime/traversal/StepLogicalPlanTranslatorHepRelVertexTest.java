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

package org.apache.geaflow.dsl.runtime.traversal;

import java.util.Collections;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.optimize.GQLOptimizer;
import org.apache.geaflow.dsl.optimize.RuleGroup;
import org.apache.geaflow.dsl.optimize.rule.GraphMatchFieldPruneRule;
import org.apache.geaflow.dsl.parser.GeaFlowDSLParser;
import org.apache.geaflow.dsl.planner.GQLContext;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphMatch;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.sqlnode.SqlCreateGraph;
import org.apache.geaflow.dsl.util.GQLRelUtil;
import org.apache.geaflow.dsl.util.SqlTypeUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StepLogicalPlanTranslatorHepRelVertexTest {

    private static final String GRAPH_G1 = "create graph g1("
        + "vertex user("
        + " id bigint ID,"
        + "name varchar"
        + "),"
        + "vertex person("
        + " id bigint ID,"
        + "name varchar,"
        + "gender int,"
        + "age integer"
        + "),"
        + "edge knows("
        + " src_id bigint SOURCE ID,"
        + " target_id bigint DESTINATION ID,"
        + " time bigint TIMESTAMP,"
        + " weight double"
        + ")"
        + ")";

    @Test
    public void testTranslateWithHepRelVertexInputs() throws Exception {
        GeaFlowDSLParser parser = new GeaFlowDSLParser();
        GQLContext gqlContext = GQLContext.create(new Configuration(), false);

        SqlCreateGraph createGraph = (SqlCreateGraph) parser.parseStatement(GRAPH_G1);
        GeaFlowGraph graph = gqlContext.convertToGraph(createGraph);
        gqlContext.registerGraph(graph);
        gqlContext.setCurrentGraph(graph.getName());

        String gql =
            "MATCH (a:person WHERE a.age > 18)"
                + "-[e:knows WHERE e.weight > 0.5]"
                + "->(b:user WHERE b.id != 0 AND name like 'MARKO')\n";
        SqlNode sqlNode = parser.parseStatement(gql);
        SqlNode validateNode = gqlContext.validate(sqlNode);
        RelNode relNode = gqlContext.toRelNode(validateNode);

        // Ensure the optimizer introduces HepRelVertex wrappers under HepPlanner,
        // and StepLogicalPlanTranslator can unwrap them.
        GQLOptimizer optimizer = new GQLOptimizer();
        optimizer.addRuleGroup(new RuleGroup(Collections.<RelOptRule>singletonList(GraphMatchFieldPruneRule.INSTANCE)));
        RelNode optimized = optimizer.optimize(relNode, 1);

        LogicalGraphMatch graphMatch = findGraphMatch(optimized);
        Assert.assertNotNull(graphMatch, "LogicalGraphMatch should exist");

        RelNode graphInput = GQLRelUtil.toRel(graphMatch.getInput());
        GraphSchema graphSchema = (GraphSchema) SqlTypeUtil.convertType(graphInput.getRowType());
        StepLogicalPlanSet planSet = new StepLogicalPlanSet(graphSchema);

        StepLogicalPlanTranslator translator = new StepLogicalPlanTranslator();
        Assert.assertNotNull(translator.translate(graphMatch, planSet));
    }

    private static LogicalGraphMatch findGraphMatch(RelNode root) {
        if (root == null) {
            return null;
        }
        RelNode node = GQLRelUtil.toRel(root);
        if (node instanceof LogicalGraphMatch) {
            return (LogicalGraphMatch) node;
        }
        for (RelNode input : node.getInputs()) {
            LogicalGraphMatch found = findGraphMatch(input);
            if (found != null) {
                return found;
            }
        }
        return null;
    }
}
