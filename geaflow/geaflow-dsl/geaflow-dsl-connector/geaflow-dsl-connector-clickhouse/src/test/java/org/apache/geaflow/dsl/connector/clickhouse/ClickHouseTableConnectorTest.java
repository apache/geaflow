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

package org.apache.geaflow.dsl.connector.clickhouse;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableConnector;
import org.apache.geaflow.dsl.connector.api.util.ConnectorFactory;
import org.apache.geaflow.dsl.connector.api.window.AllFetchWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ClickHouseTableConnectorTest {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(ClickHouseTableConnectorTest.class);

    private static final DockerImageName IMAGE =
        DockerImageName.parse("clickhouse/clickhouse-server:24.3");

    private static final int ROWS = 500;

    private static GenericContainer<?> clickhouse;
    private static String url;
    private static String username;
    private static String password;

    @BeforeClass
    public static void setup() throws SQLException {
        username = "ch_user";
        password = "ch_pass";
        clickhouse = new GenericContainer<>(IMAGE);
        clickhouse.withExposedPorts(8123);
        clickhouse.withEnv("CLICKHOUSE_USER", username);
        clickhouse.withEnv("CLICKHOUSE_PASSWORD", password);
        clickhouse.withEnv("CLICKHOUSE_DB", "default");
        clickhouse.withEnv("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1");
        clickhouse.waitingFor(Wait.forHttp("/ping").forStatusCode(200));
        clickhouse.start();
        url = "jdbc:clickhouse://" + clickhouse.getHost() + ":"
            + clickhouse.getMappedPort(8123) + "/default";
        LOGGER.info("clickhouse started at {}", url);

        try (Connection connection = DriverManager.getConnection(url, username, password);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE events (id Int64, name String, score Float64) "
                + "ENGINE = MergeTree() ORDER BY id");
            // Seed the read tests with a known, ordered data set.
            StringBuilder insert = new StringBuilder("INSERT INTO events (id, name, score) VALUES ");
            for (int i = 0; i < ROWS; i++) {
                if (i > 0) {
                    insert.append(",");
                }
                insert.append(String.format(Locale.ROOT, "(%d, 'name-%d', %f)", i, i, (double) i));
            }
            statement.execute(insert.toString());
        }
    }

    @AfterClass
    public static void cleanup() {
        if (clickhouse != null) {
            clickhouse.stop();
        }
    }

    private static TableField[] fields() {
        return new TableField[]{
            new TableField("id", Types.LONG, false),
            new TableField("name", Types.BINARY_STRING, false),
            new TableField("score", Types.DOUBLE, false)
        };
    }

    private static Configuration baseConf(String tableName) {
        Configuration conf = new Configuration();
        conf.put(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_URL, url);
        conf.put(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_USERNAME, username);
        conf.put(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_PASSWORD, password);
        conf.put(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_TABLE_NAME, tableName);
        return conf;
    }

    private static long countRows(String tableName) throws SQLException {
        try (Connection connection = DriverManager.getConnection(url, username, password);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT count() FROM " + tableName)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static void createTable(String tableName, String engine) throws SQLException {
        try (Connection connection = DriverManager.getConnection(url, username, password);
             Statement statement = connection.createStatement()) {
            statement.execute(String.format("CREATE TABLE %s (id Int64, name String, score Float64) "
                + "ENGINE = %s", tableName, engine));
        }
    }

    private static void writeRows(String tableName, int batchSize, int count) throws IOException {
        Configuration conf = baseConf(tableName);
        conf.put(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_WRITE_BATCH_SIZE,
            String.valueOf(batchSize));
        ClickHouseTableSink sink = new ClickHouseTableSink();
        sink.init(conf, new StructType(fields()));
        sink.open(null);
        try {
            for (int i = 0; i < count; i++) {
                sink.write(ObjectRow.create(new Object[]{(long) i, "name-" + i, (double) i}));
            }
            sink.finish();
        } finally {
            sink.close();
        }
    }

    private static List<Row> readAll(Configuration conf) throws IOException {
        ClickHouseTableSource source = new ClickHouseTableSource();
        source.init(conf, new TableSchema(fields()));
        source.open(null);
        List<Row> rows = new ArrayList<>();
        try {
            for (Partition partition : source.listPartitions()) {
                FetchData<Row> data = source.fetch(partition, Optional.empty(),
                    new AllFetchWindow(0L));
                Iterator<Row> iterator = data.getDataIterator();
                while (iterator.hasNext()) {
                    rows.add(iterator.next());
                }
            }
        } finally {
            source.close();
        }
        return rows;
    }

    @Test
    public void testSinkBatchWrite() throws Exception {
        createTable("sink_test", "MergeTree() ORDER BY id");
        writeRows("sink_test", 100, ROWS);
        Assert.assertEquals(countRows("sink_test"), ROWS);
    }

    @Test
    public void testSourceReadAll() throws Exception {
        Configuration conf = baseConf("events");
        List<Row> rows = readAll(conf);
        Assert.assertEquals(rows.size(), ROWS);
        // Rows come back ordered by id; spot check that the full first row round-tripped.
        Row first = rows.get(0);
        Assert.assertEquals(((Number) first.getField(0, Types.LONG)).longValue(), 0L);
        Assert.assertEquals(String.valueOf(first.getField(1, Types.BINARY_STRING)), "name-0");
        Assert.assertEquals(((Number) first.getField(2, Types.DOUBLE)).doubleValue(), 0.0, 1e-9);
    }

    @Test
    public void testSourceParallelPartitionedRead() throws Exception {
        Configuration conf = baseConf("events");
        conf.put(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_PARTITION_NUM, "2");
        conf.put(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_PARTITION_COLUMN, "id");
        conf.put(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_PARTITION_LOWERBOUND, "0");
        conf.put(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_PARTITION_UPPERBOUND,
            String.valueOf(ROWS));

        ClickHouseTableSource source = new ClickHouseTableSource();
        source.init(conf, new TableSchema(fields()));
        source.open(null);
        List<Partition> partitions = source.listPartitions();
        Assert.assertEquals(partitions.size(), 2);

        int total = 0;
        for (Partition partition : partitions) {
            FetchData<Row> data = source.fetch(partition, Optional.empty(), new AllFetchWindow(0L));
            Iterator<Row> iterator = data.getDataIterator();
            while (iterator.hasNext()) {
                iterator.next();
                total++;
            }
        }
        source.close();
        // The two partitions cover disjoint id ranges; together they must return every row exactly once.
        Assert.assertEquals(total, ROWS);
    }

    @Test
    public void testListPartitionsSplitsRange() {
        Configuration conf = baseConf("events");
        conf.put(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_PARTITION_NUM, "4");
        conf.put(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_PARTITION_LOWERBOUND, "0");
        conf.put(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_PARTITION_UPPERBOUND, "400");
        ClickHouseTableSource source = new ClickHouseTableSource();
        source.init(conf, new TableSchema(fields()));
        List<Partition> partitions = source.listPartitions();
        Assert.assertEquals(partitions.size(), 4);
        for (Partition partition : partitions) {
            Assert.assertTrue(partition.getName().contains("WHERE"));
        }
    }

    @Test
    public void testConnectorDiscoveredViaSpi() {
        // The DSL resolves `type='clickhouse'` through ConnectorFactory + the ServiceLoader SPI;
        // this verifies the META-INF/services registration is actually picked up.
        TableConnector connector = ConnectorFactory.loadConnector("clickhouse");
        Assert.assertTrue(connector instanceof ClickHouseTableConnector);
        Assert.assertEquals(connector.getType(), ClickHouseTableConnector.TYPE);
    }

    @Test
    public void testBatchWriteOutperformsRowByRow() throws Exception {
        createTable("bench_batched", "Memory");
        createTable("bench_rowbyrow", "Memory");
        int benchRows = 2000;

        long batchedStart = System.nanoTime();
        writeRows("bench_batched", 2000, benchRows);
        long batchedNanos = System.nanoTime() - batchedStart;

        // batch size 1 flushes every row, i.e. one insert round-trip per row (the row-by-row baseline).
        long rowByRowStart = System.nanoTime();
        writeRows("bench_rowbyrow", 1, benchRows);
        long rowByRowNanos = System.nanoTime() - rowByRowStart;

        Assert.assertEquals(countRows("bench_batched"), benchRows);
        Assert.assertEquals(countRows("bench_rowbyrow"), benchRows);

        double batchedPerSec = benchRows / (batchedNanos / 1_000_000_000.0);
        double rowByRowPerSec = benchRows / (rowByRowNanos / 1_000_000_000.0);
        LOGGER.info("clickhouse write benchmark for {} rows: batched={} rows/s, "
                + "row-by-row={} rows/s, speedup={}x",
            benchRows, String.format("%.0f", batchedPerSec), String.format("%.0f", rowByRowPerSec),
            String.format("%.1f", batchedPerSec / rowByRowPerSec));

        Assert.assertTrue(batchedNanos < rowByRowNanos,
            "batched write should be faster than row-by-row");
    }
}
