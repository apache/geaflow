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

package org.apache.geaflow.dsl.connector.hudi;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableConnector;
import org.apache.geaflow.dsl.connector.api.TableReadableConnector;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.util.ConnectorFactory;
import org.apache.geaflow.dsl.connector.api.window.AllFetchWindow;
import org.apache.geaflow.dsl.connector.file.source.format.ParquetFormat;
import org.apache.geaflow.runtime.core.context.DefaultRuntimeContext;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HoodieTableConnectorTest {

    private final StructType dataSchema = new StructType(
        new TableField("id", Types.INTEGER, false),
        new TableField("name", Types.BINARY_STRING),
        new TableField("price", Types.DOUBLE)
    );

    private final StructType partitionSchema = new StructType(
        new TableField("dt", Types.BINARY_STRING, false)
    );

    private final TableSchema tableSchema = new TableSchema(dataSchema, partitionSchema);

    private int commitNo = 1;

    @Test
    public void testReadHudi() throws IOException {
        String tmpDir = "/tmp/hudi/test/" + System.nanoTime();
        FileUtils.deleteQuietly(new File(tmpDir));
        writeData(tmpDir,
            "1,a1,10",
            "2,a2,12",
            "3,a3,12",
            "4,a4,15",
            "5,a5,10");

        TableConnector tableConnector = ConnectorFactory.loadConnector("hudi");
        Assert.assertEquals(tableConnector.getType().toLowerCase(Locale.ROOT), "hudi");
        TableReadableConnector readableConnector = (TableReadableConnector) tableConnector;

        Map<String, String> tableConfMap = new HashMap<>();
        tableConfMap.put(ConnectorConfigKeys.GEAFLOW_DSL_FILE_PATH.getKey(), tmpDir);
        Configuration tableConf = new Configuration(tableConfMap);
        TableSource tableSource = readableConnector.createSource(tableConf);
        tableSource.init(tableConf, tableSchema);

        tableSource.open(new DefaultRuntimeContext(tableConf));

        List<Partition> partitions = tableSource.listPartitions();

        List<Row> readRows = new ArrayList<>();
        for (Partition partition : partitions) {
            FetchData<Row> rows = tableSource.fetch(partition, Optional.empty(), new AllFetchWindow(-1));
            readRows.addAll(Lists.newArrayList(rows.getDataIterator()));
        }
        Assert.assertEquals(StringUtils.join(readRows, "\n"),
            "[1, a1, 10.0]\n"
                + "[2, a2, 12.0]\n"
                + "[3, a3, 12.0]\n"
                + "[4, a4, 15.0]\n"
                + "[5, a5, 10.0]");
    }

    private void writeData(String path, String... lines) throws IOException {
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        HoodieJavaEngineContext context = new HoodieJavaEngineContext(hadoopConf, new TestTaskContextSupplier());

        HoodieTableMetaClient.PropertyBuilder builder =
            HoodieTableMetaClient.withPropertyBuilder()
                .setDatabaseName("default")
                .setTableName("h0")
                .setTableType(HoodieTableType.COPY_ON_WRITE)
                .setPayloadClass(HoodieAvroPayload.class);
        Properties processedProperties = builder.fromProperties(new Properties()).build();

        HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf, path,
            processedProperties);

        Schema schema = ParquetFormat.convertToAvroSchema(dataSchema, false);
        HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
            .withEngineType(EngineType.JAVA)
            .withEmbeddedTimelineServerEnabled(false)
            .withPath(path)
            .withSchema(schema.toString())
            .build();
        HoodieJavaWriteClient writeClient = new HoodieJavaWriteClient(context, writeConfig);

        List<HoodieRecord> records = new ArrayList<>();
        for (String line : lines) {
            String[] fields = line.split(",");
            GenericRecord record = new GenericRecordBuilder(schema)
                .set("id", Integer.parseInt(fields[0]))
                .set("name", fields[1])
                .set("price", Double.parseDouble(fields[2]))
                .build();
            HoodieRecordPayload hoodiePayload = new DefaultHoodieRecordPayload(Option.of(record));
            HoodieKey key = new HoodieKey(fields[0], "2023/07/04");
            HoodieAvroRecord hoodieRecord = new HoodieAvroRecord(key, hoodiePayload);
            records.add(hoodieRecord);
        }

        String commitTime = String.format("%09d", commitNo++);
        writeClient.startCommitWithTime(commitTime);
        writeClient.insert(records, commitTime);
    }

    private static class TestTaskContextSupplier extends TaskContextSupplier {

        private final int partitionId = 0;
        private final int stageId = 0;
        private final long attemptId = 0;

        @Override
        public Supplier<Integer> getPartitionIdSupplier() {
            return () -> partitionId;
        }

        @Override
        public Supplier<Integer> getStageIdSupplier() {
            return () -> stageId;
        }

        @Override
        public Supplier<Long> getAttemptIdSupplier() {
            return () -> attemptId;
        }

        @Override
        public Option<String> getProperty(EngineProperty prop) {
            return Option.empty();
        }
    }
}
