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

package org.apache.geaflow.dsl.connector.neo4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.window.WindowType;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.window.SizeFetchWindow;
import org.apache.geaflow.dsl.connector.api.window.AllFetchWindow;
import org.mockito.Mockito;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class Neo4jTableSourceTest {

    private static Neo4jContainer<?> neo4jContainer;
    private static String neo4jUri;
    private static final String USERNAME = "neo4j";
    private static final String PASSWORD = "testpassword";
    private static final String DATABASE = "neo4j";

    @BeforeClass
    public static void setup() {
        neo4jContainer = new Neo4jContainer<>(DockerImageName.parse("neo4j:5.14.0"))
            .withAdminPassword(PASSWORD)
            .withEnv("NEO4J_AUTH", USERNAME + "/" + PASSWORD);
        neo4jContainer.start();
        
        neo4jUri = neo4jContainer.getBoltUrl();
        
        // Initialize test data
        try (Driver driver = org.neo4j.driver.GraphDatabase.driver(
                neo4jUri, 
                org.neo4j.driver.AuthTokens.basic(USERNAME, PASSWORD))) {
            try (Session session = driver.session()) {
                // Create test nodes
                for (int i = 1; i <= 50; i++) {
                    session.run(String.format(
                        "CREATE (p:TestNode {id: %d, name: 'Node%d', value: %d})", 
                        i, i, i * 10));
                }
                
                // Create nodes with different types
                session.run("CREATE (n:TypeNode {" +
                    "id: 100, " +
                    "str_field: 'test', " +
                    "int_field: 42, " +
                    "long_field: 9999999999, " +
                    "double_field: 3.14, " +
                    "bool_field: true})");
                
                // Create relationships
                session.run("MATCH (a:TestNode {id: 1}), (b:TestNode {id: 2}) " +
                           "CREATE (a)-[:LINKS {weight: 0.5}]->(b)");
                session.run("MATCH (a:TestNode {id: 2}), (b:TestNode {id: 3}) " +
                           "CREATE (a)-[:LINKS {weight: 0.8}]->(b)");
            }
        }
    }

    @AfterClass
    public static void cleanup() {
        if (neo4jContainer != null) {
            neo4jContainer.stop();
        }
    }

    @Test
    public void testReadSimpleQuery() throws Exception {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_QUERY, 
                "MATCH (n:TestNode) WHERE n.id <= 5 RETURN n.id as id, n.name as name ORDER BY n.id");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.LONG, false));
        fields.add(new TableField("name", Types.STRING, false));
        TableSchema tableSchema = new TableSchema(fields);

        Neo4jTableSource source = new Neo4jTableSource();
        source.init(conf, tableSchema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        source.open(context);

        List<Partition> partitions = source.listPartitions();
        Assert.assertEquals(partitions.size(), 1);

        SizeFetchWindow window = new SizeFetchWindow(0L, 100L);
        FetchData<Row> fetchData = source.fetch(partitions.get(0), Optional.empty(), window);
        
        List<Row> rows = (List<Row>) fetchData.getDataIterator();
        Assert.assertEquals(rows.size(), 5);
        
        // Verify data
        for (int i = 0; i < 5; i++) {
            Row row = rows.get(i);
            Assert.assertEquals(row.getField(0, Types.LONG), (long)(i + 1));
            Assert.assertEquals(row.getField(1, Types.STRING), "Node" + (i + 1));
        }

        source.close();
    }

    @Test
    public void testReadWithDifferentDataTypes() throws Exception {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_QUERY, 
                "MATCH (n:TypeNode {id: 100}) RETURN n.str_field as str_field, " +
                "n.int_field as int_field, n.long_field as long_field, " +
                "n.double_field as double_field, n.bool_field as bool_field");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("str_field", Types.STRING, false));
        fields.add(new TableField("int_field", Types.LONG, false));
        fields.add(new TableField("long_field", Types.LONG, false));
        fields.add(new TableField("double_field", Types.DOUBLE, false));
        fields.add(new TableField("bool_field", Types.BOOLEAN, false));
        TableSchema tableSchema = new TableSchema(fields);

        Neo4jTableSource source = new Neo4jTableSource();
        source.init(conf, tableSchema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        source.open(context);

        List<Partition> partitions = source.listPartitions();
        AllFetchWindow window = new AllFetchWindow(0L);
        FetchData<Row> fetchData = source.fetch(partitions.get(0), Optional.empty(), window);
        
        List<Row> rows = (List<Row>) fetchData.getDataIterator();
        Assert.assertEquals(rows.size(), 1);
        
        Row row = rows.get(0);
        Assert.assertEquals(row.getField(0, Types.STRING), "test");
        Assert.assertEquals(row.getField(1, Types.LONG), 42L);
        Assert.assertEquals(row.getField(2, Types.LONG), 9999999999L);
        Assert.assertEquals((Double) row.getField(3, Types.DOUBLE), 3.14, 0.01);
        Assert.assertTrue((Boolean) row.getField(4, Types.BOOLEAN));

        source.close();
    }

    @Test
    public void testPaginationFirstPage() throws Exception {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_QUERY, 
                "MATCH (n:TestNode) RETURN n.id as id ORDER BY n.id");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.LONG, false));
        TableSchema tableSchema = new TableSchema(fields);

        Neo4jTableSource source = new Neo4jTableSource();
        source.init(conf, tableSchema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        source.open(context);

        List<Partition> partitions = source.listPartitions();
        
        // Fetch first page with size 10
        SizeFetchWindow window = new SizeFetchWindow(0L, 10L);
        FetchData<Row> fetchData = source.fetch(partitions.get(0), Optional.empty(), window);
        
        List<Row> rows = (List<Row>) fetchData.getDataIterator();
        Assert.assertEquals(rows.size(), 10);
        Assert.assertFalse(fetchData.isFinish());
        Assert.assertEquals(fetchData.getNextOffset().getOffset(), 10);

        source.close();
    }

    @Test
    public void testPaginationMultiplePages() throws Exception {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_QUERY, 
                "MATCH (n:TestNode) RETURN n.id as id, n.value as value ORDER BY n.id");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.LONG, false));
        fields.add(new TableField("value", Types.LONG, false));
        TableSchema tableSchema = new TableSchema(fields);

        Neo4jTableSource source = new Neo4jTableSource();
        source.init(conf, tableSchema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        source.open(context);

        List<Partition> partitions = source.listPartitions();
        SizeFetchWindow window = new SizeFetchWindow(0L, 15L);
        
        // First page
        FetchData<Row> fetchData1 = source.fetch(partitions.get(0), Optional.empty(), window);
        List<Row> rows1 = new ArrayList<>();
        fetchData1.getDataIterator().forEachRemaining(rows1::add);
        Assert.assertEquals(rows1.size(), 15);
        Assert.assertFalse(fetchData1.isFinish());
        
        // Second page
        FetchData<Row> fetchData2 = source.fetch(partitions.get(0), Optional.of(fetchData1.getNextOffset()), window);
        List<Row> rows2 = new ArrayList<>();
        fetchData2.getDataIterator().forEachRemaining(rows2::add);
        Assert.assertEquals(rows2.size(), 15);
        
        // Third page
        FetchData<Row> fetchData3 = source.fetch(partitions.get(0), Optional.of(fetchData2.getNextOffset()), window);
        List<Row> rows3 = new ArrayList<>();
        fetchData3.getDataIterator().forEachRemaining(rows3::add);
        Assert.assertEquals(rows3.size(), 15);
        
        // Fourth page (should be smaller or empty)
        FetchData<Row> fetchData4 = source.fetch(partitions.get(0), Optional.of(fetchData3.getNextOffset()), window);
        List<Row> rows4 = new ArrayList<>();
        fetchData4.getDataIterator().forEachRemaining(rows4::add);
        Assert.assertTrue(rows4.size() <= 15);

        source.close();
    }

    @Test
    public void testReadRelationships() throws Exception {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_QUERY, 
                "MATCH (a)-[r:LINKS]->(b) " +
                "RETURN a.id as source, b.id as target, r.weight as weight ORDER BY a.id");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("source", Types.LONG, false));
        fields.add(new TableField("target", Types.LONG, false));
        fields.add(new TableField("weight", Types.DOUBLE, false));
        TableSchema tableSchema = new TableSchema(fields);

        Neo4jTableSource source = new Neo4jTableSource();
        source.init(conf, tableSchema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        source.open(context);

        List<Partition> partitions = source.listPartitions();
        AllFetchWindow window = new AllFetchWindow(0L);
        FetchData<Row> fetchData = source.fetch(partitions.get(0), Optional.empty(), window);
        
        List<Row> rows = new ArrayList<>();
        fetchData.getDataIterator().forEachRemaining(rows::add);
        Assert.assertEquals(rows.size(), 2);
        
        // Verify first relationship
        Row row1 = rows.get(0);
        Assert.assertEquals(row1.getField(0, Types.LONG), 1L);
        Assert.assertEquals(row1.getField(1, Types.LONG), 2L);
        Assert.assertEquals((Double) row1.getField(2, Types.DOUBLE), 0.5, 0.01);

        source.close();
    }

    @Test
    public void testEmptyResult() throws Exception {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_QUERY, 
                "MATCH (n:NonExistent) RETURN n.id as id");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.LONG, false));
        TableSchema tableSchema = new TableSchema(fields);

        Neo4jTableSource source = new Neo4jTableSource();
        source.init(conf, tableSchema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        source.open(context);

        List<Partition> partitions = source.listPartitions();
        AllFetchWindow window = new AllFetchWindow(0L);
        FetchData<Row> fetchData = source.fetch(partitions.get(0), Optional.empty(), window);
        
        List<Row> rows = (List<Row>) fetchData.getDataIterator();
        Assert.assertEquals(rows.size(), 0);
        Assert.assertTrue(fetchData.isFinish());

        source.close();
    }

    @Test
    public void testReadWithNullValues() throws Exception {
        // Create a node with null values
        try (Driver driver = org.neo4j.driver.GraphDatabase.driver(
                neo4jUri, 
                org.neo4j.driver.AuthTokens.basic(USERNAME, PASSWORD))) {
            try (Session session = driver.session()) {
                session.run("CREATE (n:NullTestNode {id: 999, name: 'test'})");
            }
        }

        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_QUERY, 
                "MATCH (n:NullTestNode {id: 999}) RETURN n.id as id, n.name as name, n.missing as missing");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.LONG, false));
        fields.add(new TableField("name", Types.STRING, false));
        fields.add(new TableField("missing", Types.STRING, true));
        TableSchema tableSchema = new TableSchema(fields);

        Neo4jTableSource source = new Neo4jTableSource();
        source.init(conf, tableSchema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        source.open(context);

        List<Partition> partitions = source.listPartitions();
        AllFetchWindow window = new AllFetchWindow(0L);
        FetchData<Row> fetchData = source.fetch(partitions.get(0), Optional.empty(), window);
        
        List<Row> rows = (List<Row>) fetchData.getDataIterator();
        Assert.assertEquals(rows.size(), 1);
        
        Row row = rows.get(0);
        Assert.assertEquals(row.getField(0, Types.LONG), 999L);
        Assert.assertEquals(row.getField(1, Types.STRING), "test");
        Assert.assertNull(row.getField(2, Types.STRING));

        source.close();
    }

    @Test
    public void testAllWindowType() throws Exception {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_QUERY, 
                "MATCH (n:TestNode) WHERE n.id <= 10 RETURN n.id as id ORDER BY n.id");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.LONG, false));
        TableSchema tableSchema = new TableSchema(fields);

        Neo4jTableSource source = new Neo4jTableSource();
        source.init(conf, tableSchema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        source.open(context);

        List<Partition> partitions = source.listPartitions();
        AllFetchWindow window = new AllFetchWindow(0L);
        FetchData<Row> fetchData = source.fetch(partitions.get(0), Optional.empty(), window);
        
        List<Row> rows = (List<Row>) fetchData.getDataIterator();
        Assert.assertEquals(rows.size(), 10);
        Assert.assertTrue(fetchData.isFinish()); // ALL_WINDOW should mark as finished

        source.close();
    }

    @Test(expectedExceptions = org.apache.geaflow.dsl.common.exception.GeaFlowDSLException.class)
    public void testMissingQuery() {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        // No query specified

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.LONG, false));
        TableSchema tableSchema = new TableSchema(fields);

        Neo4jTableSource source = new Neo4jTableSource();
        source.init(conf, tableSchema); // Should throw exception
    }
}
