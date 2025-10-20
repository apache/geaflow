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

package org.apache.geaflow.dsl.gremlin.integration;

import org.apache.geaflow.dsl.gremlin.parser.GeaFlowGremlinParser;
import org.apache.geaflow.dsl.gremlin.parser.GremlinQuery;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for Gremlin support in GeaFlow.
 * Tests cover Phase 2 (Core Steps) and Phase 3 (Advanced Features).
 */
public class GremlinIntegrationTest {

    private Graph graph;
    private GeaFlowGremlinParser parser;

    @Before
    public void setUp() {
        // Create a test graph
        graph = TinkerGraph.open();
        
        // Add test data
        Vertex v1 = graph.addVertex("id", 1, "name", "Alice", "age", 30);
        Vertex v2 = graph.addVertex("id", 2, "name", "Bob", "age", 35);
        Vertex v3 = graph.addVertex("id", 3, "name", "Charlie", "age", 25);
        Vertex v4 = graph.addVertex("id", 4, "name", "David", "age", 40);
        
        v1.addEdge("knows", v2, "weight", 0.5);
        v1.addEdge("knows", v3, "weight", 0.8);
        v2.addEdge("knows", v4, "weight", 0.6);
        v3.addEdge("knows", v4, "weight", 0.9);
        
        parser = new GeaFlowGremlinParser(graph);
    }

    // ==================== Phase 2: Core Steps ====================
    
    @Test
    public void testVertexQuery() {
        // Test g.V() - get all vertices
        String query = "g.V()";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    @Test
    public void testVertexQueryWithId() {
        // Test g.V(id) - get vertex by ID
        String query = "g.V(1)";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    @Test
    public void testOutStep() {
        // Test .out() - traverse outgoing edges
        String query = "g.V(1).out('knows')";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    @Test
    public void testInStep() {
        // Test .in() - traverse incoming edges
        String query = "g.V(4).in('knows')";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    @Test
    public void testBothStep() {
        // Test .both() - traverse both directions
        String query = "g.V(2).both('knows')";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    @Test
    public void testOutEStep() {
        // Test .outE() - get outgoing edges
        String query = "g.V(1).outE('knows')";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    @Test
    public void testHasStep() {
        // Test .has() - filter by property
        String query = "g.V().has('age', gt(30))";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    @Test
    public void testHasLabelStep() {
        // Test .hasLabel() - filter by label
        String query = "g.V().hasLabel('person')";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    @Test
    public void testValuesStep() {
        // Test .values() - get property values
        String query = "g.V().values('name')";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    @Test
    public void testValueMapStep() {
        // Test .valueMap() - get all properties
        String query = "g.V().valueMap()";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    @Test
    public void testMapStep() {
        // Test .map() - transform elements
        String query = "g.V().map(values('name'))";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    // ==================== Phase 3: Advanced Features ====================
    
    @Test
    public void testPathStep() {
        // Test .path() - get traversal path
        String query = "g.V(1).out('knows').path()";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    @Test
    public void testRepeatStep() {
        // Test .repeat() - loop traversal
        String query = "g.V(1).repeat(out('knows')).times(2)";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    @Test
    public void testCountStep() {
        // Test .count() - count elements
        String query = "g.V().count()";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    @Test
    public void testSumStep() {
        // Test .sum() - sum values
        String query = "g.V().values('age').sum()";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    @Test
    public void testGroupCountStep() {
        // Test .groupCount() - group and count
        String query = "g.V().groupCount().by('age')";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    @Test
    public void testOrderStep() {
        // Test .order() - sort results
        String query = "g.V().order().by('age', desc)";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    @Test
    public void testLimitStep() {
        // Test .limit() - limit results
        String query = "g.V().limit(10)";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    @Test
    public void testRangeStep() {
        // Test .range() - get range of results
        String query = "g.V().range(5, 15)";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    // ==================== Complex Queries ====================
    
    @Test
    public void testComplexQuery1() {
        // Test complex query with multiple steps
        String query = "g.V().has('age', gt(30)).out('knows').values('name')";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    @Test
    public void testComplexQuery2() {
        // Test complex query with repeat and path
        String query = "g.V(1).repeat(out('knows')).times(2).path()";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
    
    @Test
    public void testComplexQuery3() {
        // Test complex query with aggregation
        String query = "g.V().groupCount().by('age').order(local).by(values, desc)";
        GremlinQuery gremlinQuery = parser.parse(query);
        Assert.assertNotNull(gremlinQuery);
        Assert.assertTrue(gremlinQuery.isValid());
    }
}

