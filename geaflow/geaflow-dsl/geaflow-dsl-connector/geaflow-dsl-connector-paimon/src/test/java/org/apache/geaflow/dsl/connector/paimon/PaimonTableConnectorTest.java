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

package org.apache.geaflow.dsl.connector.paimon;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableConnector;
import org.apache.geaflow.dsl.connector.api.TableReadableConnector;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.util.ConnectorFactory;
import org.apache.geaflow.dsl.connector.api.window.AllFetchWindow;
import org.apache.geaflow.dsl.connector.api.window.SizeFetchWindow;
import org.apache.geaflow.runtime.core.context.DefaultRuntimeContext;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VarCharType;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

public class PaimonTableConnectorTest {

    String tmpDir = "/tmp/geaflow/dsl/paimon/test/";
    String db = "paimon_db";

    GenericRow record1 = GenericRow.of(1, BinaryString.fromString("a1"), 10.0);
    GenericRow record2 = GenericRow.of(2, BinaryString.fromString("ab"), 12.0);
    GenericRow record3 = GenericRow.of(3, BinaryString.fromString("a3"), 12.0);
    GenericRow record4 = GenericRow.of(4, BinaryString.fromString("bcd"), 15.0);
    GenericRow record5 = GenericRow.of(5, BinaryString.fromString("a5"), 10.0);
    GenericRow record6 = GenericRow.of(6, BinaryString.fromString("s1"), 9.0);
    GenericRow record7 = GenericRow.of(7, BinaryString.fromString("sb"), 20.0);
    GenericRow record8 = GenericRow.of(8, BinaryString.fromString("s3"), 16.0);
    GenericRow record9 = GenericRow.of(9, BinaryString.fromString("bad"), 12.0);
    GenericRow record10 = GenericRow.of(10, BinaryString.fromString("aa5"), 11.0);
    GenericRow record11 = GenericRow.of(11, BinaryString.fromString("x11"), 11.2);

    private final StructType dataSchema = new StructType(
        new TableField("id", Types.INTEGER, false),
        new TableField("name", Types.BINARY_STRING),
        new TableField("price", Types.DOUBLE)
    );

    private final StructType partitionSchema = new StructType(
        new TableField("dt", Types.BINARY_STRING, false)
    );

    private final TableSchema tableSchema = new TableSchema(dataSchema, partitionSchema);


    @BeforeTest
    public void prepare() {
        FileUtils.deleteQuietly(new File(tmpDir));
    }

    @AfterTest
    public void clean() {
        FileUtils.deleteQuietly(new File(tmpDir));
    }

    public void createSnapshot(String tableName, List<GenericRow> rows) {
        CatalogContext catalogContext =
                Objects.requireNonNull(CatalogContext.create(new Path(tmpDir)));
        Catalog catalog = Objects.requireNonNull(CatalogFactory.createCatalog(catalogContext));
        try {
            catalog.createDatabase(db, true);
            List<String> dbs = catalog.listDatabases();
            assert dbs.get(0).equals(db);
            Identifier identifier = new Identifier(db, tableName);
            catalog.createTable(identifier,
                    Schema.newBuilder()
                            .column("id", new IntType())
                            .column("name", new VarCharType(256))
                            .column("price", new DoubleType())
                            .build(), true);
            List<String> tables = catalog.listTables(dbs.get(0));
            assert tables.contains(tableName);
            Table table = catalog.getTable(identifier);
            BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
            BatchTableWrite write = writeBuilder.newWrite();
            for (GenericRow row : rows) {
                write.write(row);
            }
            List<CommitMessage> messages = write.prepareCommit();
            BatchTableCommit commit = writeBuilder.newCommit();
            commit.commit(messages);
        } catch (Exception e) {
            throw new GeaFlowDSLException("Test error.", e);
        }
    }

    @Test
    public void testReadPaimonStreamMode() throws IOException {
        String table = "paimon_stream_table";

        Tuple<TableSource, Configuration> tuple = createTableSource(table, true);
        TableSource tableSource = tuple.f0;
        Configuration tableConf = tuple.f1;

        tableSource.init(tableConf, tableSchema);

        // create snapshot 1
        createSnapshot(table, Lists.newArrayList(record1, record2, record3, record4, record5));

        tableSource.open(new DefaultRuntimeContext(tableConf));

        List<Partition> partitions = tableSource.listPartitions();

        Offset nextOffset = null;

        List<Object> readRows = new ArrayList<>();
        for (Partition partition : partitions) {
            FetchData<Object> rows = tableSource.fetch(partition, Optional.empty(), new SizeFetchWindow(0L, 4));
            while (rows.getDataIterator().hasNext()) {
                readRows.add(rows.getDataIterator().next());
            }
            Assert.assertFalse(rows.isFinish());
            nextOffset = rows.getNextOffset();
        }
        Assert.assertEquals(StringUtils.join(readRows, "\n"),
                "[1, a1, 10.0]\n"
                        + "[2, ab, 12.0]\n"
                        + "[3, a3, 12.0]\n"
                        + "[4, bcd, 15.0]");
        readRows.clear();

        // create snapshot 2
        createSnapshot(table, Lists.newArrayList(record6, record7, record8, record9, record10));

        for (Partition partition : partitions) {
            FetchData<Object> rows = tableSource.fetch(partition, Optional.empty(), new SizeFetchWindow(1L, 4));
            while (rows.getDataIterator().hasNext()) {
                readRows.add(rows.getDataIterator().next());
            }
            Assert.assertFalse(rows.isFinish());
        }
        Assert.assertEquals(StringUtils.join(readRows, "\n"),
                "[5, a5, 10.0]\n"
                        + "[6, s1, 9.0]\n"
                        + "[7, sb, 20.0]\n"
                        + "[8, s3, 16.0]");
        readRows.clear();

        for (Partition partition : partitions) {
            FetchData<Object> rows = tableSource.fetch(partition, Optional.empty(), new SizeFetchWindow(2L, 4));
            while (rows.getDataIterator().hasNext()) {
                readRows.add(rows.getDataIterator().next());
            }
            Assert.assertFalse(rows.isFinish());
        }
        Assert.assertEquals(StringUtils.join(readRows, "\n"),
                        "[9, bad, 12.0]\n"
                                + "[10, aa5, 11.0]");
        readRows.clear();

        // no new snapshot
        for (int i = 0; i < 2; i++) {
            for (Partition partition : partitions) {
                FetchData<Object> rows = tableSource.fetch(partition, Optional.empty(), new SizeFetchWindow(3L, 4));
                while (rows.getDataIterator().hasNext()) {
                    readRows.add(rows.getDataIterator().next());
                }
                Assert.assertFalse(rows.isFinish());
                Assert.assertTrue(readRows.isEmpty());
            }
        }

        // create snapshot 3
        createSnapshot(table, Lists.newArrayList(record11));
        for (Partition partition : partitions) {
            FetchData<Object> rows = tableSource.fetch(partition, Optional.empty(), new SizeFetchWindow(3L, 4));
            while (rows.getDataIterator().hasNext()) {
                readRows.add(rows.getDataIterator().next());
            }
            Assert.assertFalse(rows.isFinish());
            Assert.assertFalse(readRows.isEmpty());
        }
        Assert.assertEquals(StringUtils.join(readRows, "\n"),
                "[11, x11, 11.2]");
        readRows.clear();

        // test restore from offset
        for (Partition partition : partitions) {
            FetchData<Object> rows = tableSource.fetch(partition, Optional.of(nextOffset), new SizeFetchWindow(4L, 4));
            while (rows.getDataIterator().hasNext()) {
                readRows.add(rows.getDataIterator().next());
            }
            Assert.assertFalse(rows.isFinish());
        }
        Assert.assertEquals(StringUtils.join(readRows, "\n"),
                "[5, a5, 10.0]\n"
                        + "[6, s1, 9.0]\n"
                        + "[7, sb, 20.0]\n"
                        + "[8, s3, 16.0]");
    }

    @Test
    public void testReadPaimonBatchMode() throws IOException {
        String table = "paimon_batch_table";

        Tuple<TableSource, Configuration> tuple = createTableSource(table, false);
        TableSource tableSource = tuple.f0;
        Configuration tableConf = tuple.f1;
        tableSource.init(tableConf, tableSchema);

        createSnapshot(table, Lists.newArrayList(record1, record2, record3, record4, record5));

        tableSource.open(new DefaultRuntimeContext(tableConf));

        List<Partition> partitions = tableSource.listPartitions();

        List<Object> readRows = new ArrayList<>();
        for (Partition partition : partitions) {
            FetchData<Object> rows = tableSource.fetch(partition, Optional.empty(), new SizeFetchWindow(0L, 4));
            while (rows.getDataIterator().hasNext()) {
                readRows.add(rows.getDataIterator().next());
            }
            Assert.assertFalse(rows.isFinish());
        }
        Assert.assertEquals(StringUtils.join(readRows, "\n"),
                "[1, a1, 10.0]\n"
                        + "[2, ab, 12.0]\n"
                        + "[3, a3, 12.0]\n"
                        + "[4, bcd, 15.0]");
        readRows.clear();

        createSnapshot(table, Lists.newArrayList(record6, record7, record8, record9, record10));

        for (Partition partition : partitions) {
            FetchData<Object> rows = tableSource.fetch(partition, Optional.empty(), new SizeFetchWindow(1L, 4));
            while (rows.getDataIterator().hasNext()) {
                readRows.add(rows.getDataIterator().next());
            }
            Assert.assertTrue(rows.isFinish());
        }
        Assert.assertEquals(StringUtils.join(readRows, "\n"),
                "[5, a5, 10.0]");
        readRows.clear();
    }

    @Test
    public void testReadPaimonFromSnapshot() throws IOException {
        String table = "paimon_batch_table_2";

        TableConnector tableConnector = ConnectorFactory.loadConnector("PAIMON");
        Assert.assertEquals(tableConnector.getType().toLowerCase(Locale.ROOT), "paimon");
        TableReadableConnector readableConnector = (TableReadableConnector) tableConnector;
        Map<String, String> tableConfMap = getTableConf(table).getConfigMap();
        tableConfMap.put(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_SOURCE_MODE.getKey(), SourceMode.BATCH.name());
        tableConfMap.put(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_SCAN_MODE.getKey(), "From-Snapshot");
        tableConfMap.put(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_SCAN_SNAPSHOT_ID.getKey(), "1");
        Configuration tableConf = new Configuration(tableConfMap);
        TableSource tableSource = readableConnector.createSource(tableConf);
        tableSource.init(tableConf, tableSchema);

        createSnapshot(table, Lists.newArrayList(record1, record2, record3, record4, record5));
        createSnapshot(table, Lists.newArrayList(record6, record7, record8, record9, record10));

        tableSource.open(new DefaultRuntimeContext(tableConf));

        List<Partition> partitions = tableSource.listPartitions();

        List<Object> readRows = new ArrayList<>();
        for (Partition partition : partitions) {
            FetchData<Object> rows = tableSource.fetch(partition, Optional.empty(), new AllFetchWindow(0L));
            while (rows.getDataIterator().hasNext()) {
                readRows.add(rows.getDataIterator().next());
            }
            Assert.assertTrue(rows.isFinish());
        }
        Assert.assertEquals(StringUtils.join(readRows, "\n"),
                "[1, a1, 10.0]\n"
                        + "[2, ab, 12.0]\n"
                        + "[3, a3, 12.0]\n"
                        + "[4, bcd, 15.0]\n"
                        + "[5, a5, 10.0]");
    }

    @Test
    public void testReadPaimonFromLastesSnapshot() throws IOException {
        String table = "paimon_batch_table_3";

        TableConnector tableConnector = ConnectorFactory.loadConnector("PAIMON");
        Assert.assertEquals(tableConnector.getType().toLowerCase(Locale.ROOT), "paimon");
        TableReadableConnector readableConnector = (TableReadableConnector) tableConnector;
        Configuration tableConf = getTableConf(table);
        tableConf.put(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_SOURCE_MODE.getKey(), SourceMode.BATCH.name());
        TableSource tableSource = readableConnector.createSource(tableConf);
        tableSource.init(tableConf, tableSchema);

        createSnapshot(table, Lists.newArrayList(record1, record2, record3, record4, record5));
        createSnapshot(table, Lists.newArrayList(record6, record7, record8, record9, record10));

        tableSource.open(new DefaultRuntimeContext(tableConf));

        List<Partition> partitions = tableSource.listPartitions();

        List<Object> readRows = new ArrayList<>();
        for (Partition partition : partitions) {
            FetchData<Object> rows = tableSource.fetch(partition, Optional.empty(), new AllFetchWindow(0L));
            while (rows.getDataIterator().hasNext()) {
                readRows.add(rows.getDataIterator().next());
            }
            Assert.assertTrue(rows.isFinish());
        }
        Assert.assertEquals(StringUtils.join(readRows, "\n"),
                "[6, s1, 9.0]\n"
                    + "[7, sb, 20.0]\n"
                    + "[8, s3, 16.0]\n"
                    + "[9, bad, 12.0]\n"
                    + "[10, aa5, 11.0]");
    }

    private Configuration getTableConf(String table) {
        Map<String, String> tableConfMap = new HashMap<>();
        tableConfMap.put(ConnectorConfigKeys.GEAFLOW_DSL_FILE_PATH.getKey(), tmpDir);
        tableConfMap.put(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_WAREHOUSE.getKey(), tmpDir);
        tableConfMap.put(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_DATABASE_NAME.getKey(), db);
        tableConfMap.put(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_TABLE_NAME.getKey(), table);
        return new Configuration(tableConfMap);
    }

    private Tuple<TableSource, Configuration> createTableSource(String table, boolean streamMode) {
        TableConnector tableConnector = ConnectorFactory.loadConnector("PAIMON");
        Assert.assertEquals(tableConnector.getType().toLowerCase(Locale.ROOT), "paimon");
        TableReadableConnector readableConnector = (TableReadableConnector) tableConnector;
        Map<String, String> tableConfMap = getTableConf(table).getConfigMap();
        if (streamMode) {
            tableConfMap.put(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_SOURCE_MODE.getKey(), SourceMode.STREAM.name());
        }
        Configuration tableConf = new Configuration(tableConfMap);
        return Tuple.of(readableConnector.createSource(tableConf), tableConf);
    }

}
