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
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.window.SizeFetchWindow;
import org.mockito.Mockito;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class Neo4jTableConnectorTest {

    private static Neo4jContainer<?> neo4jContainer;
    private static String neo4jUri;
    private static String username = "neo4j";
    private static String password = "testpassword";
    private static String database = "neo4j";

    @BeforeClass
    public static void setup() {
        neo4jContainer = new Neo4jContainer<>(DockerImageName.parse("neo4j:5.14.0"))
            .withAdminPassword(password)
            .withEnv("NEO4J_AUTH", username + "/" + password);
        neo4jContainer.start();
        
        neo4jUri = neo4jContainer.getBoltUrl();
        
        // Initialize test data
        try (Driver driver = org.neo4j.driver.GraphDatabase.driver(
                neo4jUri, 
                org.neo4j.driver.AuthTokens.basic(username, password))) {
            try (Session session = driver.session()) {
                // Create test nodes
                session.run("CREATE (p1:Person {id: 1, name: 'Alice', age: 30})");
                session.run("CREATE (p2:Person {id: 2, name: 'Bob', age: 25})");
                session.run("CREATE (p3:Person {id: 3, name: 'Charlie', age: 35})");
                
                // Create test relationships
                session.run("MATCH (a:Person {id: 1}), (b:Person {id: 2}) " +
                           "CREATE (a)-[:KNOWS {since: 2020}]->(b)");
                session.run("MATCH (a:Person {id: 2}), (b:Person {id: 3}) " +
                           "CREATE (a)-[:KNOWS {since: 2021}]->(b)");
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
    public void testConnectorType() {
        Neo4jTableConnector connector = new Neo4jTableConnector();
        Assert.assertEquals(connector.getType(), "Neo4j");
    }

    @Test
    public void testCreateSource() {
        Neo4jTableConnector connector = new Neo4jTableConnector();
        Configuration conf = new Configuration();
        Neo4jTableSource source = (Neo4jTableSource) connector.createSource(conf);
        Assert.assertNotNull(source);
    }

    @Test
    public void testCreateSink() {
        Neo4jTableConnector connector = new Neo4jTableConnector();
        Configuration conf = new Configuration();
        Neo4jTableSink sink = (Neo4jTableSink) connector.createSink(conf);
        Assert.assertNotNull(sink);
    }

    @Test
    public void testReadNodes() throws Exception {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, username);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, password);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, database);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_QUERY, 
                "MATCH (p:Person) RETURN p.id as id, p.name as name, p.age as age ORDER BY p.id");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.LONG, false));
        fields.add(new TableField("name", Types.STRING, false));
        fields.add(new TableField("age", Types.LONG, false));
        TableSchema tableSchema = new TableSchema(fields);

        Neo4jTableSource source = new Neo4jTableSource();
        source.init(conf, tableSchema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        source.open(context);

        List<Partition> partitions = source.listPartitions();
        Assert.assertEquals(partitions.size(), 1);

        SizeFetchWindow window = new SizeFetchWindow(0L, 10L);
        FetchData<Row> fetchData = source.fetch(partitions.get(0), Optional.empty(), window);
        
        // Collect rows from iterator
        List<Row> rows = new ArrayList<>();
        while (fetchData.getDataIterator().hasNext()) {
            rows.add(fetchData.getDataIterator().next());
        }
        Assert.assertEquals(rows.size(), 3);
        
        // Verify first row
        Row row1 = rows.get(0);
        Assert.assertEquals((Long) row1.getField(0, Types.LONG), Long.valueOf(1L));
        Assert.assertEquals(row1.getField(1, Types.STRING), "Alice");
        Assert.assertEquals((Long) row1.getField(2, Types.LONG), Long.valueOf(30L));

        source.close();
    }

    @Test
    public void testWriteNodes() throws Exception {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, username);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, password);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, database);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE, "node");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_LABEL, "TestPerson");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_ID_FIELD, "id");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_BATCH_SIZE, "2");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.LONG, false));
        fields.add(new TableField("name", Types.STRING, false));
        fields.add(new TableField("email", Types.STRING, true));
        StructType schema = new StructType(fields);

        Neo4jTableSink sink = new Neo4jTableSink();
        sink.init(conf, schema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        sink.open(context);

        // Write test data
        Row row1 = ObjectRow.create(new Object[]{100L, "David", "david@example.com"});
        Row row2 = ObjectRow.create(new Object[]{101L, "Eva", "eva@example.com"});
        Row row3 = ObjectRow.create(new Object[]{102L, "Frank", null});

        sink.write(row1);
        sink.write(row2);
        sink.write(row3);
        
        sink.finish();
        sink.close();

        // Verify data was written
        try (Driver driver = org.neo4j.driver.GraphDatabase.driver(
                neo4jUri, 
                org.neo4j.driver.AuthTokens.basic(username, password))) {
            try (Session session = driver.session()) {
                var result = session.run("MATCH (p:TestPerson) WHERE p.id >= 100 RETURN count(p) as count");
                long count = result.single().get("count").asLong();
                Assert.assertEquals(count, 3);
                
                // Verify specific node
                var result2 = session.run("MATCH (p:TestPerson {id: 100}) RETURN p.name as name, p.email as email");
                var record = result2.single();
                Assert.assertEquals(record.get("name").asString(), "David");
                Assert.assertEquals(record.get("email").asString(), "david@example.com");
            }
        }
    }

    @Test
    public void testWriteRelationships() throws Exception {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, username);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, password);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, database);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE, "relationship");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_RELATIONSHIP_TYPE, "FOLLOWS");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_RELATIONSHIP_SOURCE_FIELD, "from_id");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_RELATIONSHIP_TARGET_FIELD, "to_id");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_BATCH_SIZE, "2");

        // First create some nodes to connect
        try (Driver driver = org.neo4j.driver.GraphDatabase.driver(
                neo4jUri, 
                org.neo4j.driver.AuthTokens.basic(username, password))) {
            try (Session session = driver.session()) {
                session.run("CREATE (p:TestUser {id: 200, name: 'User1'})");
                session.run("CREATE (p:TestUser {id: 201, name: 'User2'})");
                session.run("CREATE (p:TestUser {id: 202, name: 'User3'})");
            }
        }

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("from_id", Types.LONG, false));
        fields.add(new TableField("to_id", Types.LONG, false));
        fields.add(new TableField("weight", Types.DOUBLE, true));
        StructType schema = new StructType(fields);

        Neo4jTableSink sink = new Neo4jTableSink();
        sink.init(conf, schema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        sink.open(context);

        // Write relationships
        Row rel1 = ObjectRow.create(new Object[]{200L, 201L, 0.8});
        Row rel2 = ObjectRow.create(new Object[]{201L, 202L, 0.9});

        sink.write(rel1);
        sink.write(rel2);
        
        sink.finish();
        sink.close();

        // Verify relationships were created
        try (Driver driver = org.neo4j.driver.GraphDatabase.driver(
                neo4jUri, 
                org.neo4j.driver.AuthTokens.basic(username, password))) {
            try (Session session = driver.session()) {
                var result = session.run(
                    "MATCH ()-[r:FOLLOWS]->() WHERE r.weight IS NOT NULL RETURN count(r) as count");
                long count = result.single().get("count").asLong();
                Assert.assertEquals(count, 2);
            }
        }
    }

    @Test
    public void testReadWithPagination() throws Exception {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, username);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, password);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, database);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_QUERY, 
                "MATCH (p:Person) RETURN p.id as id, p.name as name ORDER BY p.id");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.LONG, false));
        fields.add(new TableField("name", Types.STRING, false));
        TableSchema tableSchema = new TableSchema(fields);

        Neo4jTableSource source = new Neo4jTableSource();
        source.init(conf, tableSchema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        source.open(context);

        List<Partition> partitions = source.listPartitions();
        
        // Fetch first page
        SizeFetchWindow window = new SizeFetchWindow(0L, 2L);
        FetchData<Row> fetchData1 = source.fetch(partitions.get(0), Optional.empty(), window);
        List<Row> rows1 = (List<Row>) fetchData1.getDataIterator();
        Assert.assertEquals(rows1.size(), 2);
        
        // Fetch second page
        FetchData<Row> fetchData2 = source.fetch(partitions.get(0), 
            Optional.of(fetchData1.getNextOffset()), window);
        List<Row> rows2 = (List<Row>) fetchData2.getDataIterator();
        Assert.assertEquals(rows2.size(), 1);

        source.close();
    }

    @Test
    public void testBatchWriting() throws Exception {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, username);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, password);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, database);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE, "node");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_LABEL, "BatchTest");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_ID_FIELD, "id");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_BATCH_SIZE, "3");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.LONG, false));
        fields.add(new TableField("value", Types.STRING, false));
        StructType schema = new StructType(fields);

        Neo4jTableSink sink = new Neo4jTableSink();
        sink.init(conf, schema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        sink.open(context);

        // Write 10 rows (should trigger multiple batches)
        for (long i = 300; i < 310; i++) {
            Row row = ObjectRow.create(new Object[]{i, "Value" + i});
            sink.write(row);
        }
        
        sink.finish();
        sink.close();

        // Verify all data was written
        try (Driver driver = org.neo4j.driver.GraphDatabase.driver(
                neo4jUri, 
                org.neo4j.driver.AuthTokens.basic(username, password))) {
            try (Session session = driver.session()) {
                var result = session.run("MATCH (n:BatchTest) WHERE n.id >= 300 AND n.id < 310 RETURN count(n) as count");
                long count = result.single().get("count").asLong();
                Assert.assertEquals(count, 10);
            }
        }
    }

    @Test(expectedExceptions = org.apache.geaflow.dsl.common.exception.GeaFlowDSLException.class)
    public void testInvalidWriteMode() {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, username);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, password);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, database);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE, "invalid_mode");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_ID_FIELD, "id");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.LONG, false));
        StructType schema = new StructType(fields);

        Neo4jTableSink sink = new Neo4jTableSink();
        sink.init(conf, schema); // Should throw exception
    }

    @Test(expectedExceptions = org.apache.geaflow.dsl.common.exception.GeaFlowDSLException.class)
    public void testMissingNodeIdField() {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, username);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, password);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, database);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE, "node");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.LONG, false));
        StructType schema = new StructType(fields);

        Neo4jTableSink sink = new Neo4jTableSink();
        sink.init(conf, schema); // Should throw exception
    }
}
