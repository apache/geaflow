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
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableConnector;
import org.apache.geaflow.dsl.connector.api.TableReadableConnector;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.util.ConnectorFactory;
import org.apache.geaflow.dsl.connector.api.window.AllFetchWindow;
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
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class PaimonTableConnectorTest {

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
        String tmpDir = "/tmp/geaflow/dsl/paimon/test/";
        FileUtils.deleteQuietly(new File(tmpDir));
        String db = "paimon_db";
        String tableName = "paimon_table";
        CatalogContext catalogContext =
            Objects.requireNonNull(CatalogContext.create(new Path(tmpDir)));
        Catalog catalog = Objects.requireNonNull(CatalogFactory.createCatalog(catalogContext));
        try {
            catalog.createDatabase(db, false);
            List<String> dbs = catalog.listDatabases();
            assert dbs.get(0).equals(db);
            Identifier identifier = new Identifier(db, tableName);
            catalog.createTable(identifier,
                Schema.newBuilder()
                    .column("id", new IntType())
                    .column("name", new VarCharType(256))
                    .column("price", new DoubleType())
                    .build(), false);
            List<String> tables = catalog.listTables(dbs.get(0));
            assert tables.get(0).equals(tableName);
            Table table = catalog.getTable(identifier);
            BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
            BatchTableWrite write = writeBuilder.newWrite();
            GenericRow record1 = GenericRow.of(1, BinaryString.fromString("a1"), 10.0);
            GenericRow record2 = GenericRow.of(2, BinaryString.fromString("ab"), 12.0);
            GenericRow record3 = GenericRow.of(3, BinaryString.fromString("a3"), 12.0);
            GenericRow record4 = GenericRow.of(4, BinaryString.fromString("bcd"), 15.0);
            GenericRow record5 = GenericRow.of(5, BinaryString.fromString("a5"), 10.0);
            write.write(record1);
            write.write(record2);
            write.write(record3);
            write.write(record4);
            write.write(record5);
            List<CommitMessage> messages = write.prepareCommit();
            BatchTableCommit commit = writeBuilder.newCommit();
            commit.commit(messages);
        } catch (Exception e) {
            throw new GeaFlowDSLException("Test error.", e);
        }
    }

    @Test
    public void testReadPaimon() throws IOException {
        String tmpDir = "/tmp/geaflow/dsl/paimon/test/";
        String db = "paimon_db";
        String table = "paimon_table";

        TableConnector tableConnector = ConnectorFactory.loadConnector("PAIMON");
        Assert.assertEquals(tableConnector.getType().toLowerCase(Locale.ROOT), "paimon");
        TableReadableConnector readableConnector = (TableReadableConnector) tableConnector;

        Map<String, String> tableConfMap = new HashMap<>();
        tableConfMap.put(ConnectorConfigKeys.GEAFLOW_DSL_FILE_PATH.getKey(), tmpDir);
        tableConfMap.put(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_WAREHOUSE.getKey(), tmpDir);
        tableConfMap.put(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_DATABASE_NAME.getKey(), db);
        tableConfMap.put(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_TABLE_NAME.getKey(), table);
        Configuration tableConf = new Configuration(tableConfMap);
        TableSource tableSource = readableConnector.createSource(tableConf);
        tableSource.init(tableConf, tableSchema);

        tableSource.open(new DefaultRuntimeContext(tableConf));

        List<Partition> partitions = tableSource.listPartitions();

        TableDeserializer deserializer = tableSource.getDeserializer(tableConf);
        deserializer.init(tableConf, tableSchema);
        List<Row> readRows = new ArrayList<>();
        for (Partition partition : partitions) {
            FetchData<Object> rows = tableSource.fetch(partition, Optional.empty(), new AllFetchWindow(-1L));
            while (rows.getDataIterator().hasNext()) {
                readRows.addAll(deserializer.deserialize(rows.getDataIterator().next()));
            }
        }
        Assert.assertEquals(StringUtils.join(readRows, "\n"),
            "[1, a1, 10.0]\n"
                + "[2, ab, 12.0]\n"
                + "[3, a3, 12.0]\n"
                + "[4, bcd, 15.0]\n"
                + "[5, a5, 10.0]");
    }

}
