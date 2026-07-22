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

package org.apache.geaflow.dsl.connector.doris;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
import org.apache.geaflow.dsl.connector.api.window.AllFetchWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration test for the Doris connector. It boots a real Doris (all-in-one) container using
 * Testcontainers and exercises the Stream Load sink and the partitioned source end to end. The
 * container requires host networking (so the FE-to-BE Stream Load redirect is reachable), which is
 * only available on Linux; the test skips itself when Docker or host networking is not available.
 *
 * <p>An external Doris can be targeted instead by setting the {@code doris.it.fenodes} and
 * {@code doris.it.jdbcUrl} system properties.
 */
public class DorisConnectorIntegrationTest {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(DorisConnectorIntegrationTest.class);

    private static final String DORIS_IMAGE = "apache/doris:doris-all-in-one-2.1.0";
    private static final String DATABASE = "geaflow_it";
    private static final String TABLE = "person";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";

    private static GenericContainer<?> dorisContainer;
    private static String feNodes;
    private static String jdbcUrl;

    @BeforeClass
    public void setUp() throws Exception {
        String externalFe = System.getProperty("doris.it.fenodes");
        String externalJdbc = System.getProperty("doris.it.jdbcUrl");
        if (externalFe != null && externalJdbc != null) {
            feNodes = externalFe;
            jdbcUrl = externalJdbc;
        } else if (Boolean.getBoolean("doris.it.enabled")) {
            // The container is heavy (multi-GB image) and needs host networking so the FE-to-BE
            // Stream Load redirect is reachable, so it is opt-in and never runs in a normal CI.
            if (!DockerClientFactory.instance().isDockerAvailable()) {
                throw new SkipException("Docker is not available, skip Doris integration test.");
            }
            if (!System.getProperty("os.name", "").toLowerCase().contains("linux")) {
                throw new SkipException("Doris Stream Load redirect needs host networking, only "
                    + "supported on Linux, skip integration test.");
            }
            dorisContainer = new GenericContainer<>(DORIS_IMAGE)
                .withNetworkMode("host")
                .waitingFor(Wait.forLogMessage(".*get heartbeat response.*", 1)
                    .withStartupTimeout(java.time.Duration.ofMinutes(5)));
            dorisContainer.start();
            feNodes = "127.0.0.1:8030";
            jdbcUrl = "jdbc:mysql://127.0.0.1:9030/";
        } else {
            throw new SkipException("Doris integration test is disabled by default. Enable it with "
                + "-Ddoris.it.enabled=true (requires Docker on a Linux host), or point it at an "
                + "external Doris with -Ddoris.it.fenodes and -Ddoris.it.jdbcUrl.");
        }
        waitForBackendAlive();
        prepareSchema();
    }

    @AfterClass
    public void tearDown() {
        if (dorisContainer != null) {
            dorisContainer.stop();
        }
    }

    private Connection newConnection(String url) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        return DriverManager.getConnection(url, USERNAME, PASSWORD);
    }

    private void waitForBackendAlive() throws Exception {
        long deadline = System.currentTimeMillis() + java.time.Duration.ofMinutes(3).toMillis();
        while (System.currentTimeMillis() < deadline) {
            try (Connection connection = newConnection(jdbcUrl);
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SHOW BACKENDS")) {
                while (rs.next()) {
                    if ("true".equalsIgnoreCase(rs.getString("Alive"))) {
                        return;
                    }
                }
            } catch (Exception e) {
                LOGGER.info("waiting for doris backend alive: {}", e.getMessage());
            }
            Thread.sleep(5000);
        }
        throw new IllegalStateException("Doris backend did not become alive in time.");
    }

    private void prepareSchema() throws Exception {
        try (Connection connection = newConnection(jdbcUrl);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS " + DATABASE);
            statement.execute("DROP TABLE IF EXISTS " + DATABASE + "." + TABLE);
            statement.execute("CREATE TABLE " + DATABASE + "." + TABLE + " ("
                + "id BIGINT, name VARCHAR(64), age INT) "
                + "UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES(\"replication_num\" = \"1\")");
        }
    }

    private StructType schema() {
        return new StructType(
            new TableField("id", Types.LONG, false),
            new TableField("name", Types.BINARY_STRING, true),
            new TableField("age", Types.INTEGER, true));
    }

    private Configuration sinkConfig() {
        Configuration conf = new Configuration();
        conf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_FENODES, feNodes);
        conf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_DATABASE, DATABASE);
        conf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_TABLE, TABLE);
        conf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_USERNAME, USERNAME);
        conf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_PASSWORD, PASSWORD);
        conf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_SINK_MAX_ROWS, "1000");
        return conf;
    }

    private long countRows() throws Exception {
        try (Connection connection = newConnection(jdbcUrl);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                 "SELECT COUNT(*) FROM " + DATABASE + "." + TABLE)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private void truncate() throws Exception {
        try (Connection connection = newConnection(jdbcUrl);
             Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE " + DATABASE + "." + TABLE);
        }
    }

    @Test
    public void testSinkAndSource() throws Exception {
        truncate();
        int rowCount = 2000;
        DorisTableSink sink = new DorisTableSink();
        sink.init(sinkConfig(), schema());
        sink.open(null);
        for (int i = 0; i < rowCount; i++) {
            sink.write(ObjectRow.create((long) i, "name_" + i, i % 100));
        }
        sink.finish();
        sink.close();
        Assert.assertEquals(countRows(), rowCount);

        Configuration sourceConf = new Configuration();
        sourceConf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_JDBC_URL, jdbcUrl + DATABASE);
        sourceConf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_DATABASE, DATABASE);
        sourceConf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_TABLE, TABLE);
        sourceConf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_USERNAME, USERNAME);
        sourceConf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_PASSWORD, PASSWORD);
        sourceConf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_SOURCE_PARTITION_NUM, "4");
        sourceConf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_SOURCE_PARTITION_COLUMN, "id");
        sourceConf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_SOURCE_PARTITION_LOWERBOUND, "0");
        sourceConf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_SOURCE_PARTITION_UPPERBOUND,
            String.valueOf(rowCount));

        DorisTableSource source = new DorisTableSource();
        source.init(sourceConf, new TableSchema(schema()));
        source.open(null);
        List<Partition> partitions = source.listPartitions();
        Assert.assertTrue(partitions.size() > 1, "source should produce parallel partitions.");
        List<Row> allRows = new ArrayList<>();
        for (Partition partition : partitions) {
            FetchData<Row> fetchData =
                source.fetch(partition, Optional.empty(), new AllFetchWindow(0));
            Iterator<Row> iterator = fetchData.getDataIterator();
            while (iterator.hasNext()) {
                allRows.add(iterator.next());
            }
        }
        source.close();
        Assert.assertEquals(allRows.size(), rowCount);
    }

    @Test
    public void testStreamLoadThroughputBenchmark() throws Exception {
        int rowCount = 20000;

        truncate();
        DorisTableSink sink = new DorisTableSink();
        sink.init(sinkConfig(), schema());
        sink.open(null);
        long streamLoadStart = System.currentTimeMillis();
        for (int i = 0; i < rowCount; i++) {
            sink.write(ObjectRow.create((long) i, "name_" + i, i % 100));
        }
        sink.finish();
        sink.close();
        long streamLoadCost = System.currentTimeMillis() - streamLoadStart;
        Assert.assertEquals(countRows(), rowCount);

        truncate();
        long jdbcStart = System.currentTimeMillis();
        try (Connection connection = newConnection(jdbcUrl);
             Statement statement = connection.createStatement()) {
            for (int i = 0; i < rowCount; i++) {
                statement.execute(String.format(
                    "INSERT INTO %s.%s (id, name, age) VALUES (%d, '%s', %d)",
                    DATABASE, TABLE, i, "name_" + i, i % 100));
            }
        }
        long jdbcCost = System.currentTimeMillis() - jdbcStart;

        LOGGER.info("Doris write benchmark for {} rows: streamLoad={}ms ({} rows/s), "
                + "row-by-row JDBC={}ms ({} rows/s)", rowCount, streamLoadCost,
            rowCount * 1000L / Math.max(1, streamLoadCost), jdbcCost,
            rowCount * 1000L / Math.max(1, jdbcCost));
        Assert.assertTrue(streamLoadCost < jdbcCost,
            "Stream Load should be faster than row-by-row JDBC insert.");
    }
}
