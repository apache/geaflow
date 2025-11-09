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
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.mockito.Mockito;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class Neo4jTableSinkTest {

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
    }

    @AfterClass
    public static void cleanup() {
        if (neo4jContainer != null) {
            neo4jContainer.stop();
        }
    }

    @Test
    public void testWriteNodesWithNullValues() throws Exception {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE, "node");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_LABEL, "NullTest");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_ID_FIELD, "id");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_BATCH_SIZE, "10");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.LONG, false));
        fields.add(new TableField("name", Types.STRING, true));
        fields.add(new TableField("age", Types.INTEGER, true));
        StructType schema = new StructType(fields);

        Neo4jTableSink sink = new Neo4jTableSink();
        sink.init(conf, schema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        sink.open(context);

        // Write rows with null values
        Row row1 = ObjectRow.create(new Object[]{1000L, null, 25});
        Row row2 = ObjectRow.create(new Object[]{1001L, "John", null});
        Row row3 = ObjectRow.create(new Object[]{1002L, null, null});

        sink.write(row1);
        sink.write(row2);
        sink.write(row3);
        
        sink.finish();
        sink.close();

        // Verify data
        try (Driver driver = org.neo4j.driver.GraphDatabase.driver(
                neo4jUri, 
                org.neo4j.driver.AuthTokens.basic(USERNAME, PASSWORD))) {
            try (Session session = driver.session()) {
                // Check first row
                var result1 = session.run("MATCH (n:NullTest {id: 1000}) RETURN n.age as age");
                Assert.assertEquals(result1.single().get("age").asInt(), 25);
                
                // Check second row
                var result2 = session.run("MATCH (n:NullTest {id: 1001}) RETURN n.name as name");
                Assert.assertEquals(result2.single().get("name").asString(), "John");
            }
        }
    }

    @Test
    public void testWriteDifferentDataTypes() throws Exception {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE, "node");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_LABEL, "TypeTest");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_ID_FIELD, "id");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_BATCH_SIZE, "10");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.LONG, false));
        fields.add(new TableField("int_val", Types.INTEGER, true));
        fields.add(new TableField("long_val", Types.LONG, true));
        fields.add(new TableField("double_val", Types.DOUBLE, true));
        fields.add(new TableField("bool_val", Types.BOOLEAN, true));
        fields.add(new TableField("str_val", Types.STRING, true));
        StructType schema = new StructType(fields);

        Neo4jTableSink sink = new Neo4jTableSink();
        sink.init(conf, schema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        sink.open(context);

        Row row = ObjectRow.create(new Object[]{
            2000L, 
            42, 
            9999999999L, 
            3.14159, 
            true, 
            "test string"
        });

        sink.write(row);
        sink.finish();
        sink.close();

        // Verify all data types
        try (Driver driver = org.neo4j.driver.GraphDatabase.driver(
                neo4jUri, 
                org.neo4j.driver.AuthTokens.basic(USERNAME, PASSWORD))) {
            try (Session session = driver.session()) {
                var result = session.run(
                    "MATCH (n:TypeTest {id: 2000}) " +
                    "RETURN n.int_val as int_val, n.long_val as long_val, " +
                    "n.double_val as double_val, n.bool_val as bool_val, n.str_val as str_val");
                var record = result.single();
                
                Assert.assertEquals(record.get("int_val").asInt(), 42);
                Assert.assertEquals(record.get("long_val").asLong(), 9999999999L);
                Assert.assertEquals(record.get("double_val").asDouble(), 3.14159, 0.00001);
                Assert.assertTrue(record.get("bool_val").asBoolean());
                Assert.assertEquals(record.get("str_val").asString(), "test string");
            }
        }
    }

    @Test
    public void testNodeMergeStrategy() throws Exception {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE, "node");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_LABEL, "MergeTest");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_ID_FIELD, "id");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_BATCH_SIZE, "10");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.LONG, false));
        fields.add(new TableField("value", Types.STRING, false));
        StructType schema = new StructType(fields);

        Neo4jTableSink sink = new Neo4jTableSink();
        sink.init(conf, schema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        sink.open(context);

        // Write same ID twice with different values
        Row row1 = ObjectRow.create(new Object[]{3000L, "first"});
        sink.write(row1);
        sink.finish();
        sink.close();

        // Write again with same ID
        sink = new Neo4jTableSink();
        sink.init(conf, schema);
        sink.open(context);
        Row row2 = ObjectRow.create(new Object[]{3000L, "second"});
        sink.write(row2);
        sink.finish();
        sink.close();

        // Verify only one node exists with updated value
        try (Driver driver = org.neo4j.driver.GraphDatabase.driver(
                neo4jUri, 
                org.neo4j.driver.AuthTokens.basic(USERNAME, PASSWORD))) {
            try (Session session = driver.session()) {
                var result = session.run("MATCH (n:MergeTest {id: 3000}) RETURN count(n) as count, n.value as value");
                var record = result.single();
                Assert.assertEquals(record.get("count").asLong(), 1L);
                Assert.assertEquals(record.get("value").asString(), "second");
            }
        }
    }

    @Test
    public void testRelationshipWithMultipleProperties() throws Exception {
        // Create test nodes first
        try (Driver driver = org.neo4j.driver.GraphDatabase.driver(
                neo4jUri, 
                org.neo4j.driver.AuthTokens.basic(USERNAME, PASSWORD))) {
            try (Session session = driver.session()) {
                session.run("CREATE (a {id: 4000}), (b {id: 4001})");
            }
        }

        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE, "relationship");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_RELATIONSHIP_TYPE, "CONNECTS");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_RELATIONSHIP_SOURCE_FIELD, "source");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_RELATIONSHIP_TARGET_FIELD, "target");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_BATCH_SIZE, "10");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("source", Types.LONG, false));
        fields.add(new TableField("target", Types.LONG, false));
        fields.add(new TableField("weight", Types.DOUBLE, true));
        fields.add(new TableField("type", Types.STRING, true));
        fields.add(new TableField("active", Types.BOOLEAN, true));
        StructType schema = new StructType(fields);

        Neo4jTableSink sink = new Neo4jTableSink();
        sink.init(conf, schema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        sink.open(context);

        Row rel = ObjectRow.create(new Object[]{4000L, 4001L, 0.95, "direct", true});
        sink.write(rel);
        sink.finish();
        sink.close();

        // Verify relationship properties
        try (Driver driver = org.neo4j.driver.GraphDatabase.driver(
                neo4jUri, 
                org.neo4j.driver.AuthTokens.basic(USERNAME, PASSWORD))) {
            try (Session session = driver.session()) {
                var result = session.run(
                    "MATCH ({id: 4000})-[r:CONNECTS]->({id: 4001}) " +
                    "RETURN r.weight as weight, r.type as type, r.active as active");
                var record = result.single();
                
                Assert.assertEquals(record.get("weight").asDouble(), 0.95, 0.0001);
                Assert.assertEquals(record.get("type").asString(), "direct");
                Assert.assertTrue(record.get("active").asBoolean());
            }
        }
    }

    @Test
    public void testTransactionRollback() throws Exception {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE, "node");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_LABEL, "RollbackTest");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_ID_FIELD, "id");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_BATCH_SIZE, "10");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.LONG, false));
        fields.add(new TableField("value", Types.STRING, false));
        StructType schema = new StructType(fields);

        Neo4jTableSink sink = new Neo4jTableSink();
        sink.init(conf, schema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        sink.open(context);

        Row row = ObjectRow.create(new Object[]{5000L, "test"});
        sink.write(row);
        
        // Close without finish - should not commit
        sink.close();

        // Verify data was not written
        try (Driver driver = org.neo4j.driver.GraphDatabase.driver(
                neo4jUri, 
                org.neo4j.driver.AuthTokens.basic(USERNAME, PASSWORD))) {
            try (Session session = driver.session()) {
                var result = session.run("MATCH (n:RollbackTest {id: 5000}) RETURN count(n) as count");
                long count = result.single().get("count").asLong();
                Assert.assertEquals(count, 0L);
            }
        }
    }

    @Test(expectedExceptions = org.apache.geaflow.dsl.common.exception.GeaFlowDSLException.class)
    public void testNullNodeId() throws Exception {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE, "node");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_LABEL, "ErrorTest");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_ID_FIELD, "id");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_BATCH_SIZE, "10");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.LONG, false));
        fields.add(new TableField("value", Types.STRING, false));
        StructType schema = new StructType(fields);

        Neo4jTableSink sink = new Neo4jTableSink();
        sink.init(conf, schema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        sink.open(context);

        // Try to write with null ID - should throw exception
        Row row = ObjectRow.create(new Object[]{null, "test"});
        sink.write(row);
        sink.finish(); // Should throw here
        sink.close();
    }

    @Test(expectedExceptions = org.apache.geaflow.dsl.common.exception.GeaFlowDSLException.class)
    public void testNullRelationshipEndpoints() throws Exception {
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE, "relationship");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_RELATIONSHIP_TYPE, "ERROR");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_RELATIONSHIP_SOURCE_FIELD, "source");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_RELATIONSHIP_TARGET_FIELD, "target");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_BATCH_SIZE, "10");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("source", Types.LONG, false));
        fields.add(new TableField("target", Types.LONG, false));
        StructType schema = new StructType(fields);

        Neo4jTableSink sink = new Neo4jTableSink();
        sink.init(conf, schema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        sink.open(context);

        // Try to write with null source - should throw exception
        Row row = ObjectRow.create(new Object[]{null, 1L});
        sink.write(row);
        sink.finish(); // Should throw here
        sink.close();
    }
}
