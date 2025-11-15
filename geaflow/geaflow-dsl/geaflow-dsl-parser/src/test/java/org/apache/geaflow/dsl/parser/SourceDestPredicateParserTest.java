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

package org.apache.geaflow.dsl.parser;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.geaflow.dsl.sqlnode.SqlMatchEdge;
import org.apache.geaflow.dsl.sqlnode.SqlMatchPattern;
import org.apache.geaflow.dsl.sqlnode.SqlPathPattern;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SourceDestPredicateParserTest {

    @Test
    public void testParseSourcePredicate() throws SqlParseException {
        String sql = "MATCH (a:person) -[e:knows where e.weight > 0.5, SOURCE a.age > 25]->(b:person)";
        GeaFlowDSLParser parser = new GeaFlowDSLParser();
        SqlNode node = parser.parseStatement(sql);
        
        Assert.assertTrue(node instanceof SqlMatchPattern);
        SqlMatchPattern matchPattern = (SqlMatchPattern) node;
        
        // Get the path pattern
        SqlPathPattern pathPattern = (SqlPathPattern) matchPattern.getPathPatterns().get(0);
        
        // Get the edge (second element in path)
        SqlMatchEdge edge = (SqlMatchEdge) pathPattern.getPathNodes().get(1);
        
        // Verify source condition exists
        Assert.assertNotNull(edge.getSourceCondition());
    }

    @Test
    public void testParseDestinationPredicate() throws SqlParseException {
        String sql = "MATCH (a:person) -[e:knows where e.weight > 0.5, DESTINATION b.age < 35]->(b:person)";
        GeaFlowDSLParser parser = new GeaFlowDSLParser();
        SqlNode node = parser.parseStatement(sql);
        
        Assert.assertTrue(node instanceof SqlMatchPattern);
        SqlMatchPattern matchPattern = (SqlMatchPattern) node;
        
        // Get the path pattern
        SqlPathPattern pathPattern = (SqlPathPattern) matchPattern.getPathPatterns().get(0);
        
        // Get the edge (second element in path)
        SqlMatchEdge edge = (SqlMatchEdge) pathPattern.getPathNodes().get(1);
        
        // Verify destination condition exists
        Assert.assertNotNull(edge.getDestCondition());
    }

    @Test
    public void testParseBothSourceAndDestPredicate() throws SqlParseException {
        String sql = "MATCH (a:person) -[e:knows where e.weight > 0.5, SOURCE a.age > 25, DESTINATION b.age < 35]->(b:person)";
        GeaFlowDSLParser parser = new GeaFlowDSLParser();
        SqlNode node = parser.parseStatement(sql);
        
        Assert.assertTrue(node instanceof SqlMatchPattern);
        SqlMatchPattern matchPattern = (SqlMatchPattern) node;
        
        // Get the path pattern
        SqlPathPattern pathPattern = (SqlPathPattern) matchPattern.getPathPatterns().get(0);
        
        // Get the edge (second element in path)
        SqlMatchEdge edge = (SqlMatchEdge) pathPattern.getPathNodes().get(1);
        
        // Verify both conditions exist
        Assert.assertNotNull(edge.getSourceCondition());
        Assert.assertNotNull(edge.getDestCondition());
    }
}
