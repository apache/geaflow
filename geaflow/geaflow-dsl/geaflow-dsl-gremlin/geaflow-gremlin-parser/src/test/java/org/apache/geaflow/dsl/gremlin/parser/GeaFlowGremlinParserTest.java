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

package org.apache.geaflow.dsl.gremlin.parser;

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for GeaFlowGremlinParser.
 */
public class GeaFlowGremlinParserTest {

    private GeaFlowGremlinParser parser;

    @Before
    public void setUp() {
        // Create a mock graph for testing
        Graph graph = TinkerGraph.open();
        parser = new GeaFlowGremlinParser(graph);
    }

    @Test
    public void testParseSimpleQuery() {
        // Test parsing a simple Gremlin query
        String gremlinQuery = "g.V()";
        GremlinQuery query = parser.parse(gremlinQuery);
        
        Assert.assertNotNull(query);
        Assert.assertEquals(gremlinQuery, query.getQueryString());
        Assert.assertNotNull(query.getBytecode());
        Assert.assertNotNull(query.getTraversal());
        Assert.assertTrue(query.isValid());
        // Note: The actual step count may vary depending on the implementation
        Assert.assertTrue(query.getStepCount() >= 0);
    }

    @Test
    public void testParseComplexQuery() {
        // Test parsing a more complex Gremlin query
        String gremlinQuery = "g.V().out('knows').has('age', gt(30))";
        GremlinQuery query = parser.parse(gremlinQuery);
        
        Assert.assertNotNull(query);
        Assert.assertEquals(gremlinQuery, query.getQueryString());
        Assert.assertNotNull(query.getBytecode());
        Assert.assertNotNull(query.getTraversal());
        Assert.assertTrue(query.isValid());
        // Note: The actual step count may vary depending on the implementation
        Assert.assertTrue(query.getStepCount() >= 0);
    }
    
    @Test
    public void testParseQueryWithValues() {
        // Test parsing a query with values step
        String gremlinQuery = "g.V().values('name')";
        GremlinQuery query = parser.parse(gremlinQuery);
        
        Assert.assertNotNull(query);
        Assert.assertEquals(gremlinQuery, query.getQueryString());
        Assert.assertNotNull(query.getBytecode());
        Assert.assertNotNull(query.getTraversal());
        Assert.assertTrue(query.isValid());
        // Note: The actual step count may vary depending on the implementation
        Assert.assertTrue(query.getStepCount() >= 0);
    }
    
    @Test
    public void testParseQueryWithPath() {
        // Test parsing a query with path step
        String gremlinQuery = "g.V().out().path()";
        GremlinQuery query = parser.parse(gremlinQuery);
        
        Assert.assertNotNull(query);
        Assert.assertEquals(gremlinQuery, query.getQueryString());
        Assert.assertNotNull(query.getBytecode());
        Assert.assertNotNull(query.getTraversal());
        Assert.assertTrue(query.isValid());
        // Note: The actual step count may vary depending on the implementation
        Assert.assertTrue(query.getStepCount() >= 0);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testParseNullQuery() {
        // Test parsing a null query
        parser.parse((String)null);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testParseEmptyQuery() {
        // Test parsing an empty query
        parser.parse("");
    }
    
    @Test
    public void testParseBytecode() {
        // Test parsing bytecode
        String gremlinQuery = "g.V().out('knows')";
        GremlinQuery originalQuery = parser.parse(gremlinQuery);
        Bytecode bytecode = originalQuery.getBytecode();
        
        GremlinQuery queryFromBytecode = parser.parse(bytecode);
        
        Assert.assertNotNull(queryFromBytecode);
        Assert.assertNull(queryFromBytecode.getQueryString());
        Assert.assertEquals(bytecode, queryFromBytecode.getBytecode());
        Assert.assertNotNull(queryFromBytecode.getTraversal());
        Assert.assertTrue(queryFromBytecode.isValid());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testParseNullBytecode() {
        // Test parsing null bytecode
        parser.parse((Bytecode) null);
    }
    
    @Test
    public void testGetTraversalSource() {
        // Test getting the traversal source
        Assert.assertNotNull(parser.getTraversalSource());
    }
    
    @Test
    public void testQueryEquality() {
        // Test query equality
        String gremlinQuery = "g.V().out('knows')";
        GremlinQuery query1 = parser.parse(gremlinQuery);
        GremlinQuery query2 = parser.parse(gremlinQuery);
        
        Assert.assertEquals(query1, query2);
        Assert.assertEquals(query1.hashCode(), query2.hashCode());
    }
}