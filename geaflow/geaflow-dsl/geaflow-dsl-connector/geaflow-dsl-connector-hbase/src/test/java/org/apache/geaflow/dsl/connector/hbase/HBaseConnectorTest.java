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

package org.apache.geaflow.dsl.connector.hbase;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.TableConnector;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.apache.geaflow.dsl.connector.api.TableWritableConnector;
import org.apache.geaflow.dsl.connector.api.util.ConnectorFactory;
import org.apache.geaflow.runtime.core.context.DefaultRuntimeContext;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class HBaseConnectorTest {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseConnectorTest.class);

    private static String namespace = "TuGraph";

    public static final String zookeeperQuorum = "127.0.0.1";

    public static final String tableType = "HBase";

    public static final String tmpDataDir = "/tmp/GeaFlow-HBase-Sink-Connector";

    private final StructType dataSchema = new StructType(
        new TableField("id", Types.INTEGER, false),
        new TableField("name", Types.BINARY_STRING),
        new TableField("price", Types.DOUBLE),
        new TableField("weight", Types.LONG)
    );

    Object[][] items = {
        {1, "a1", 10.11, 12L},
        {2, "a2", 12.22, 10000000L},
        {3, "a3", 13.33, 1237879479832L},
        {4, "a4", 14.44, 34978947328979L},
        {5, "a5", 25.67, 98302183091830190L}
    };

    private final TableSchema tableSchema = new TableSchema(dataSchema);

    @BeforeClass
    public static void setup() throws IOException {
        System.setProperty("test.build.data.basedirectory", tmpDataDir);
        HBaseLocalTestUtils.createNamespace(namespace);
    }

    @AfterClass
    public static void tearDown() {
        HBaseLocalTestUtils.closeConnection();
    }

    @Test
    public void testLoadConnector() {
        TableConnector tableConnector = ConnectorFactory.loadConnector(tableType);
        Assert.assertEquals(tableConnector.getType().toLowerCase(Locale.ROOT), "hbase");
    }

    private void prepare(String tableName, Map<String, String> tableConfMap,
                         String... columnFamilies) throws IOException {
        if (!tableConfMap.containsKey(HBaseConfigKeys.GEAFLOW_DSL_HBASE_NAME_SPACE.getKey())) {
            namespace = "default";
        }
        HBaseLocalTestUtils.createTable(namespace, tableName, columnFamilies);


        TableConnector tableConnector = ConnectorFactory.loadConnector(tableType);
        TableWritableConnector readableConnector = (TableWritableConnector) tableConnector;
        Configuration tableConf = new Configuration(tableConfMap);
        TableSink tableSink = readableConnector.createSink(tableConf);
        tableSink.init(tableConf, tableSchema);
        tableSink.open(new DefaultRuntimeContext(tableConf));

        for (Object[] item : items) {
            Row row = ObjectRow.create(item);
            tableSink.write(row);
        }
        tableSink.finish();

        List<Result> results = Lists.newArrayList();
        ResultScanner scanner = HBaseLocalTestUtils.getScanner(namespace, tableName);
        for (Result result : scanner) {
            results.add(result);
        }
        Assert.assertEquals(results.size(), 5);
    }

    @Test
    public void testWriteBase() throws IOException {
        String tableName = "GeaFlowBase";
        Map<String, String> tableConfMap = buildConfiguration(zookeeperQuorum, namespace, tableName,
            "id", null, "{\"name\": "
                + "\"A\", \"price\": \"A\", \"weight\": \"B\"}");
        prepare(tableName, tableConfMap, "A", "B");
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals(Bytes.toString(HBaseLocalTestUtils.getCell(namespace, tableName,
                String.valueOf(i + 1), "A", "name")), items[i][1]);
            Assert.assertEquals(ByteBuffer.wrap(HBaseLocalTestUtils.getCell(namespace, tableName,
                String.valueOf(i + 1), "A", "price")).getDouble(), items[i][2]);
            Assert.assertEquals(ByteBuffer.wrap(HBaseLocalTestUtils.getCell(namespace, tableName,
                String.valueOf(i + 1), "B", "weight")).getLong(), items[i][3]);
        }
    }

    @Test
    public void testWriteRowKey() throws IOException {
        String tableName = "GeaFlowRowKey";
        Map<String, String> tableConfMap = buildConfiguration(zookeeperQuorum, namespace, tableName,
            "id,name", "-", "{\"price\": \"A\", \"weight\": \"A\"}");
        prepare("GeaFlowRowKey", tableConfMap, "A");

        for (int i = 0; i < 5; i++) {
            Assert.assertEquals(ByteBuffer.wrap(HBaseLocalTestUtils.getCell(namespace, tableName,
                (i + 1) + "-" + items[i][1], "A", "price")).getDouble(), items[i][2]);
            Assert.assertEquals(ByteBuffer.wrap(HBaseLocalTestUtils.getCell(namespace, tableName,
                (i + 1) + "-" + items[i][1], "A", "weight")).getLong(), items[i][3]);
        }
    }

    @Test
    public void testWriteDefaultColumnFamily() throws IOException {
        String tableName = "GeaFlowDefaultColumnFamily";
        Map<String, String> tableConfMap = buildConfiguration(zookeeperQuorum, namespace, tableName,
            "id", null, null);
        // default column family is "GeaFlow"
        prepare(tableName, tableConfMap, "A", "GeaFlow");

        for (int i = 0; i < 5; i++) {
            Assert.assertEquals(Bytes.toString(HBaseLocalTestUtils.getCell(namespace, tableName,
                String.valueOf(i + 1), "GeaFlow", "name")), items[i][1]);
            Assert.assertEquals(ByteBuffer.wrap(HBaseLocalTestUtils.getCell(namespace, tableName,
                String.valueOf(i + 1), "GeaFlow", "price")).getDouble(), items[i][2]);
            Assert.assertEquals(ByteBuffer.wrap(HBaseLocalTestUtils.getCell(namespace, tableName,
                String.valueOf(i + 1), "GeaFlow", "weight")).getLong(), items[i][3]);
        }
    }

    @Test
    public void testWriteDefaultNamespace() throws IOException {
        String tableName = "GeaFlowDefaultNamespace";
        // default namespace is "default"
        Map<String, String> tableConfMap = buildConfiguration(zookeeperQuorum, null, tableName,
            "id", null, "{\"name\": "
                + "\"A\", \"price\": \"A\", \"weight\": \"B\"}");
        prepare(tableName, tableConfMap, "A", "B");
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals(Bytes.toString(HBaseLocalTestUtils.getCell("default", tableName,
                String.valueOf(i + 1), "A", "name")), items[i][1]);
            Assert.assertEquals(ByteBuffer.wrap(HBaseLocalTestUtils.getCell("default", tableName,
                String.valueOf(i + 1), "A", "price")).getDouble(), items[i][2]);
            Assert.assertEquals(ByteBuffer.wrap(HBaseLocalTestUtils.getCell("default", tableName,
                String.valueOf(i + 1), "B", "weight")).getLong(), items[i][3]);
        }
    }

    private Map<String, String> buildConfiguration(String zkQuorum, String namespace,
                                                   String tableName, String rowKeyColumn,
                                                   String rowKeySeparator,
                                                   String familyNameMapping) {
        Map<String, String> tableConfMap = new HashMap<>();
        tableConfMap.put(HBaseConfigKeys.GEAFLOW_DSL_HBASE_ZOOKEEPER_QUORUM.getKey(), zkQuorum);
        if (Objects.nonNull(namespace)) {
            tableConfMap.put(HBaseConfigKeys.GEAFLOW_DSL_HBASE_NAME_SPACE.getKey(), namespace);
        }
        tableConfMap.put(HBaseConfigKeys.GEAFLOW_DSL_HBASE_TABLE_NAME.getKey(), tableName);
        tableConfMap.put(HBaseConfigKeys.GEAFLOW_DSL_HBASE_ROWKEY_COLUMNS.getKey(), rowKeyColumn);
        if (Objects.nonNull(rowKeySeparator)) {
            tableConfMap.put(HBaseConfigKeys.GEAFLOW_DSL_HBASE_ROWKEY_SEPARATOR.getKey(),
                rowKeySeparator);
        }
        if (Objects.nonNull(familyNameMapping)) {
            tableConfMap.put(HBaseConfigKeys.GEAFLOW_DSL_HBASE_FAMILY_NAME.getKey(),
                familyNameMapping);
        }
        return tableConfMap;
    }
}
