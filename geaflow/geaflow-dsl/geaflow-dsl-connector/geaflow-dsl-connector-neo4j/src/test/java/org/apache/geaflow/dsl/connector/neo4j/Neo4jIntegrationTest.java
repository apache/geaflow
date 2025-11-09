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
import org.apache.geaflow.dsl.connector.api.window.AllFetchWindow;
import org.mockito.Mockito;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class Neo4jIntegrationTest {

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
    public void testWriteAndReadNodes() throws Exception {
        // Write nodes
        Configuration writeConf = new Configuration();
        writeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        writeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        writeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        writeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        writeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE, "node");
        writeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_LABEL, "Product");
        writeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_ID_FIELD, "product_id");
        writeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_BATCH_SIZE, "5");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("product_id", Types.LONG, false));
        fields.add(new TableField("product_name", Types.STRING, false));
        fields.add(new TableField("price", Types.DOUBLE, false));
        fields.add(new TableField("in_stock", Types.BOOLEAN, false));
        StructType schema = new StructType(fields);

        Neo4jTableSink sink = new Neo4jTableSink();
        sink.init(writeConf, schema);
        
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        sink.open(context);

        // Write test data
        for (long i = 1; i <= 10; i++) {
            Row row = ObjectRow.create(new Object[]{
                i,
                "Product" + i,
                99.99 + i,
                i % 2 == 0
            });
            sink.write(row);
        }
        
        sink.finish();
        sink.close();

        // Read nodes back
        Configuration readConf = new Configuration();
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_QUERY, 
                "MATCH (p:Product) RETURN p.id as product_id, p.product_name as product_name, " +
                "p.price as price, p.in_stock as in_stock ORDER BY p.id");

        TableSchema tableSchema = new TableSchema(fields);

        Neo4jTableSource source = new Neo4jTableSource();
        source.init(readConf, tableSchema);
        source.open(context);

        List<Partition> partitions = source.listPartitions();
        AllFetchWindow window = new AllFetchWindow(0L);
        FetchData<Row> fetchData = source.fetch(partitions.get(0), Optional.empty(), window);
        
        List<Row> rows = (List<Row>) fetchData.getDataIterator();
        Assert.assertEquals(rows.size(), 10);

        // Verify first row
        Row row1 = rows.get(0);
        Assert.assertEquals(row1.getField(0, Types.LONG), 1L);
        Assert.assertEquals(row1.getField(1, Types.STRING), "Product1");
        Assert.assertEquals((Double) row1.getField(2, Types.DOUBLE), 100.99, 0.01);
        Assert.assertFalse((Boolean) row1.getField(3, Types.BOOLEAN));

        source.close();
    }

    @Test
    public void testWriteNodesAndRelationships() throws Exception {
        RuntimeContext context = Mockito.mock(RuntimeContext.class);

        // First write nodes
        Configuration nodeConf = new Configuration();
        nodeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        nodeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        nodeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        nodeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        nodeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE, "node");
        nodeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_LABEL, "City");
        nodeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_ID_FIELD, "city_id");
        nodeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_BATCH_SIZE, "10");

        List<TableField> nodeFields = new ArrayList<>();
        nodeFields.add(new TableField("city_id", Types.LONG, false));
        nodeFields.add(new TableField("city_name", Types.STRING, false));
        nodeFields.add(new TableField("population", Types.LONG, false));
        StructType nodeSchema = new StructType(nodeFields);

        Neo4jTableSink nodeSink = new Neo4jTableSink();
        nodeSink.init(nodeConf, nodeSchema);
        nodeSink.open(context);

        Row city1 = ObjectRow.create(new Object[]{1L, "Beijing", 21540000L});
        Row city2 = ObjectRow.create(new Object[]{2L, "Shanghai", 24280000L});
        Row city3 = ObjectRow.create(new Object[]{3L, "Guangzhou", 15300000L});

        nodeSink.write(city1);
        nodeSink.write(city2);
        nodeSink.write(city3);
        nodeSink.finish();
        nodeSink.close();

        // Then write relationships
        Configuration relConf = new Configuration();
        relConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        relConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        relConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        relConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        relConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE, "relationship");
        relConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_RELATIONSHIP_TYPE, "CONNECTED_TO");
        relConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_RELATIONSHIP_SOURCE_FIELD, "from_city");
        relConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_RELATIONSHIP_TARGET_FIELD, "to_city");
        relConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_BATCH_SIZE, "10");

        List<TableField> relFields = new ArrayList<>();
        relFields.add(new TableField("from_city", Types.LONG, false));
        relFields.add(new TableField("to_city", Types.LONG, false));
        relFields.add(new TableField("distance", Types.DOUBLE, false));
        relFields.add(new TableField("travel_time", Types.INTEGER, false));
        StructType relSchema = new StructType(relFields);

        Neo4jTableSink relSink = new Neo4jTableSink();
        relSink.init(relConf, relSchema);
        relSink.open(context);

        Row rel1 = ObjectRow.create(new Object[]{1L, 2L, 1214.0, 120});
        Row rel2 = ObjectRow.create(new Object[]{2L, 3L, 1376.0, 150});

        relSink.write(rel1);
        relSink.write(rel2);
        relSink.finish();
        relSink.close();

        // Read and verify the graph
        Configuration readConf = new Configuration();
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_QUERY, 
                "MATCH (a:City)-[r:CONNECTED_TO]->(b:City) " +
                "RETURN a.id as from_city, b.id as to_city, r.distance as distance, " +
                "r.travel_time as travel_time ORDER BY a.id");

        TableSchema tableSchema = new TableSchema(relFields);

        Neo4jTableSource source = new Neo4jTableSource();
        source.init(readConf, tableSchema);
        source.open(context);

        List<Partition> partitions = source.listPartitions();
        AllFetchWindow window = new AllFetchWindow(0L);
        FetchData<Row> fetchData = source.fetch(partitions.get(0), Optional.empty(), window);
        
        // Collect all rows from iterator
        List<Row> rows = new ArrayList<>();
        fetchData.getDataIterator().forEachRemaining(rows::add);
        Assert.assertEquals(rows.size(), 2);

        // Verify relationships
        Row relRow1 = rows.get(0);
        Assert.assertEquals((Long) relRow1.getField(0, Types.LONG), Long.valueOf(1L));
        Assert.assertEquals((Long) relRow1.getField(1, Types.LONG), Long.valueOf(2L));
        Assert.assertEquals((Double) relRow1.getField(2, Types.DOUBLE), 1214.0, 0.1);
        Assert.assertEquals((Integer) relRow1.getField(3, Types.INTEGER), Integer.valueOf(120));

        source.close();
    }

    @Test
    public void testUpdateExistingNodes() throws Exception {
        RuntimeContext context = Mockito.mock(RuntimeContext.class);

        // Write initial nodes
        Configuration conf = new Configuration();
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE, "node");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_LABEL, "Account");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_ID_FIELD, "account_id");
        conf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_BATCH_SIZE, "10");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("account_id", Types.LONG, false));
        fields.add(new TableField("balance", Types.DOUBLE, false));
        fields.add(new TableField("status", Types.STRING, false));
        StructType schema = new StructType(fields);

        // Initial write
        Neo4jTableSink sink1 = new Neo4jTableSink();
        sink1.init(conf, schema);
        sink1.open(context);
        Row row1 = ObjectRow.create(new Object[]{1000L, 1000.0, "active"});
        sink1.write(row1);
        sink1.finish();
        sink1.close();

        // Update the same node
        Neo4jTableSink sink2 = new Neo4jTableSink();
        sink2.init(conf, schema);
        sink2.open(context);
        Row row2 = ObjectRow.create(new Object[]{1000L, 1500.0, "premium"});
        sink2.write(row2);
        sink2.finish();
        sink2.close();

        // Read and verify updated value
        Configuration readConf = new Configuration();
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_QUERY, 
                "MATCH (a:Account {id: 1000}) RETURN a.id as account_id, " +
                "a.balance as balance, a.status as status");

        TableSchema tableSchema = new TableSchema(fields);

        Neo4jTableSource source = new Neo4jTableSource();
        source.init(readConf, tableSchema);
        source.open(context);

        List<Partition> partitions = source.listPartitions();
        AllFetchWindow window = new AllFetchWindow(0L);
        FetchData<Row> fetchData = source.fetch(partitions.get(0), Optional.empty(), window);
        
        // Collect all rows from iterator
        List<Row> rows = new ArrayList<>();
        fetchData.getDataIterator().forEachRemaining(rows::add);
        Assert.assertEquals(rows.size(), 1);

        Row row = rows.get(0);
        Assert.assertEquals((Long) row.getField(0, Types.LONG), Long.valueOf(1000L));
        Assert.assertEquals((Double) row.getField(1, Types.DOUBLE), 1500.0, 0.01);
        Assert.assertEquals(row.getField(2, Types.STRING), "premium");

        source.close();
    }

    @Test
    public void testLargeDatasetWriteAndRead() throws Exception {
        RuntimeContext context = Mockito.mock(RuntimeContext.class);

        Configuration writeConf = new Configuration();
        writeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        writeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        writeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        writeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        writeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE, "node");
        writeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_LABEL, "LargeDataset");
        writeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_ID_FIELD, "id");
        writeConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_BATCH_SIZE, "50");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.LONG, false));
        fields.add(new TableField("value", Types.LONG, false));
        StructType schema = new StructType(fields);

        Neo4jTableSink sink = new Neo4jTableSink();
        sink.init(writeConf, schema);
        sink.open(context);

        // Write 200 rows
        for (long i = 10000; i < 10200; i++) {
            Row row = ObjectRow.create(new Object[]{i, i * 2});
            sink.write(row);
        }
        sink.finish();
        sink.close();

        // Read back with pagination
        Configuration readConf = new Configuration();
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, neo4jUri);
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, USERNAME);
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, PASSWORD);
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE, DATABASE);
        readConf.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_QUERY, 
                "MATCH (n:LargeDataset) RETURN n.id as id, n.value as value ORDER BY n.id");

        TableSchema tableSchema = new TableSchema(fields);

        Neo4jTableSource source = new Neo4jTableSource();
        source.init(readConf, tableSchema);
        source.open(context);

        List<Partition> partitions = source.listPartitions();
        SizeFetchWindow window = new SizeFetchWindow(0L, 75L);
        
        int totalRows = 0;
        Optional<org.apache.geaflow.dsl.connector.api.Offset> offset = Optional.empty();
        
        while (true) {
            FetchData<Row> fetchData = source.fetch(partitions.get(0), offset, window);
            // Count rows from iterator
            List<Row> pageRows = new ArrayList<>();
            fetchData.getDataIterator().forEachRemaining(pageRows::add);
            totalRows += pageRows.size();
            
            if (fetchData.isFinish()) {
                break;
            }
            offset = Optional.of(fetchData.getNextOffset());
        }

        Assert.assertEquals(totalRows, 200);

        source.close();
    }
}
