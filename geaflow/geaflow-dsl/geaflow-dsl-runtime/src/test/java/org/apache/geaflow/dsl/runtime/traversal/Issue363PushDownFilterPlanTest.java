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
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.calcite.VertexRecordType;
import org.apache.geaflow.dsl.optimize.GQLOptimizer;
import org.apache.geaflow.dsl.optimize.OptimizeRules;
import org.apache.geaflow.dsl.optimize.RuleGroup;
import org.apache.geaflow.dsl.parser.GeaFlowDSLParser;
import org.apache.geaflow.dsl.planner.GQLContext;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphMatch;
import org.apache.geaflow.dsl.rel.match.VertexMatch;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.sqlnode.SqlCreateGraph;
import org.apache.geaflow.dsl.util.GQLRelUtil;
import org.apache.geaflow.dsl.util.GQLRexUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests for ID filter push-down behavior in the optimizer.
 *
 * <p>This test verifies that MatchIdFilterSimplifyRule correctly extracts ID equality
 * filters to VertexMatch.idSet for efficient O(1) vertex lookup. The rule order in
 * OptimizeRules ensures MatchIdFilterSimplifyRule runs before IdFilterPushdownRule.
 */
public class Issue363PushDownFilterPlanTest {

    private static final String GRAPH_DDL = "create graph g_issue363_simple("
        + "vertex Person("
        + " id bigint ID,"
        + " name varchar"
        + "),"
        + "edge knows("
        + " src_id bigint SOURCE ID,"
        + " target_id bigint DESTINATION ID"
        + ")"
        + ")";

    @Test
    public void testExtractIdsFromVertexMatchPushDownFilter() throws Exception {
        GeaFlowDSLParser parser = new GeaFlowDSLParser();
        GQLContext gqlContext = GQLContext.create(new Configuration(), false);

        SqlCreateGraph createGraph = (SqlCreateGraph) parser.parseStatement(GRAPH_DDL);
        GeaFlowGraph graph = gqlContext.convertToGraph(createGraph);
        gqlContext.registerGraph(graph);
        gqlContext.setCurrentGraph(graph.getName());

        String gql = "MATCH (a:Person where a.id = 1)-[knows]->(b:Person)\n"
            + "RETURN a.id as a_id, a.name as a_name, b.id as b_id, b.name as b_name";
        SqlNode sqlNode = parser.parseStatement(gql);
        SqlNode validateNode = gqlContext.validate(sqlNode);
        RelNode relNode = gqlContext.toRelNode(validateNode);

        GQLOptimizer optimizer = new GQLOptimizer();
        for (RuleGroup ruleGroup : OptimizeRules.RULE_GROUPS) {
            optimizer.addRuleGroup(ruleGroup);
        }
        RelNode optimized = optimizer.optimize(relNode);

        LogicalGraphMatch graphMatch = findGraphMatch(optimized);
        Assert.assertNotNull(graphMatch, "LogicalGraphMatch should exist");

        VertexMatch aMatch = findVertexMatchByLabel(graphMatch.getPathPattern(), "a");
        Assert.assertNotNull(aMatch, "VertexMatch(a) should exist");

        RexNode pushDownFilter = aMatch.getPushDownFilter();
        Set<RexNode> idsFromFilter = pushDownFilter == null ? Collections.emptySet()
            : GQLRexUtil.findVertexIds(pushDownFilter, (VertexRecordType) aMatch.getNodeType());

        boolean hasIdSet = aMatch.getIdSet() != null && !aMatch.getIdSet().isEmpty();
        boolean hasIdsFromFilter = !idsFromFilter.isEmpty();

        // Assert that ID filter was successfully extracted to either idSet or pushDownFilter
        Assert.assertTrue(hasIdSet || hasIdsFromFilter,
            "ID filter should be extracted to idSet or pushDownFilter. "
            + "idSet=" + aMatch.getIdSet() + ", pushDownFilter=" + pushDownFilter);

        // Verify idSet contains the expected ID value
        if (hasIdSet) {
            Assert.assertTrue(aMatch.getIdSet().contains(1L),
                "idSet should contain ID value 1, but got: " + aMatch.getIdSet());
        }
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

    private static VertexMatch findVertexMatchByLabel(RelNode root, String label) {
        if (root == null) {
            return null;
        }
        RelNode node = GQLRelUtil.toRel(root);
        if (node instanceof VertexMatch) {
            VertexMatch vertexMatch = (VertexMatch) node;
            if (vertexMatch.getLabel().equals(label)) {
                return vertexMatch;
            }
        }
        for (RelNode input : node.getInputs()) {
            VertexMatch found = findVertexMatchByLabel(input, label);
            if (found != null) {
                return found;
            }
        }
        return null;
    }
}
