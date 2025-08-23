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

package org.apache.geaflow.dsl.connector.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.primitive.BinaryStringType;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.AllFetchWindow;
import org.apache.geaflow.runtime.core.context.DefaultRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

public class HiveTableSourceTest extends BaseHiveTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveTableSourceTest.class);

    @AfterTest
    public void shutdown() {
        super.shutdown();
    }

    @Test
    public void testReadHiveText() throws IOException {
        String ddl = "CREATE TABLE hive_user (id int, name string, age int) stored as textfile";
        String inserts = "INSERT into hive_user SELECT 1, 'jim', 20;"
            + "INSERT into hive_user SELECT 2, 'kate', 18;"
            + "INSERT into hive_user SELECT 3, 'lily', 22;"
            + "INSERT into hive_user SELECT 4, 'lucy', 25;"
            + "INSERT into hive_user SELECT 5, 'jack', 26";
        StructType dataSchema = new StructType(
            new TableField("id", IntegerType.INSTANCE, false),
            new TableField("name", BinaryStringType.INSTANCE, true),
            new TableField("age", IntegerType.INSTANCE, false)
        );
        checkReadHive(ddl, inserts, dataSchema, new StructType(),
            "[1, jim, 20]\n"
                + "[2, kate, 18]\n"
                + "[3, lily, 22]\n"
                + "[4, lucy, 25]\n"
                + "[5, jack, 26]");
    }

    @Test
    public void testReadHiveParquet() throws IOException {
        String ddl = "CREATE TABLE hive_user (id int, name string, age int) stored as parquet";
        String inserts = "INSERT into hive_user SELECT 1, 'jim', 20;"
            + "INSERT into hive_user SELECT 2, 'kate', 18;"
            + "INSERT into hive_user SELECT 3, 'lily', 22;"
            + "INSERT into hive_user SELECT 4, 'lucy', 25;"
            + "INSERT into hive_user SELECT 5, 'jack', 26";
        StructType dataSchema = new StructType(
            new TableField("id", IntegerType.INSTANCE, false),
            new TableField("name", BinaryStringType.INSTANCE, true),
            new TableField("age", IntegerType.INSTANCE, false)
        );
        checkReadHive(ddl, inserts, dataSchema, new StructType(),
            "[1, jim, 20]\n"
                + "[2, kate, 18]\n"
                + "[3, lily, 22]\n"
                + "[4, lucy, 25]\n"
                + "[5, jack, 26]");
    }

    @Test
    public void testReadHiveOrc() throws IOException {
        String ddl = "CREATE TABLE hive_user (id int, name string, age int) stored as orc";
        String inserts = "INSERT into hive_user SELECT 1, 'jim', 20;"
            + "INSERT into hive_user SELECT 2, 'kate', 18;"
            + "INSERT into hive_user SELECT 3, 'lily', 22;"
            + "INSERT into hive_user SELECT 4, 'lucy', 25;"
            + "INSERT into hive_user SELECT 5, 'jack', 26";
        StructType dataSchema = new StructType(
            new TableField("id", IntegerType.INSTANCE, false),
            new TableField("name", BinaryStringType.INSTANCE, true),
            new TableField("age", IntegerType.INSTANCE, false)
        );
        checkReadHive(ddl, inserts, dataSchema, new StructType(),
            "[1, jim, 20]\n"
                + "[2, kate, 18]\n"
                + "[3, lily, 22]\n"
                + "[4, lucy, 25]\n"
                + "[5, jack, 26]");
    }

    @Test
    public void testReadHiveTextPartitionTable() throws IOException {
        String ddl = "CREATE TABLE hive_user (id int, name string, age int) "
            + "partitioned by(dt string)"
            + "stored as textfile";
        String inserts =
            "INSERT into hive_user partition(dt = '2023-04-23') SELECT 1, 'jim', 20;"
                + "INSERT into hive_user partition(dt = '2023-04-24') SELECT 2, 'kate', 18;"
                + "INSERT into hive_user partition(dt = '2023-04-24') SELECT 3, 'lily', 22;"
                + "INSERT into hive_user partition(dt = '2023-04-25') SELECT 4, 'lucy', 25;"
                + "INSERT into hive_user partition(dt = '2023-04-26') SELECT 5, 'jack', 26";
        StructType dataSchema = new StructType(
            new TableField("id", IntegerType.INSTANCE, false),
            new TableField("name", BinaryStringType.INSTANCE, true),
            new TableField("age", IntegerType.INSTANCE, false)
        );
        StructType partitionSchema = new StructType(
            new TableField("dt", BinaryStringType.INSTANCE, false)
        );
        checkReadHive(ddl, inserts, dataSchema, partitionSchema,
            "[1, jim, 20, 2023-04-23]\n"
                + "[2, kate, 18, 2023-04-24]\n"
                + "[3, lily, 22, 2023-04-24]\n"
                + "[4, lucy, 25, 2023-04-25]\n"
                + "[5, jack, 26, 2023-04-26]");
    }

    @Test
    public void testReadHiveText2PartitionTable() throws IOException {
        String ddl = "CREATE TABLE hive_user (id int, name string, age int) "
            + "partitioned by(dt string, hh string)"
            + "stored as textfile";
        String inserts =
            "INSERT into hive_user partition(dt = '2023-04-23', hh ='10') SELECT 1, 'jim', 20;"
                + "INSERT into hive_user partition(dt = '2023-04-24',hh = '10') SELECT 2, 'kate', 18;"
                + "INSERT into hive_user partition(dt = '2023-04-24',hh = '11') SELECT 3, 'lily', 22;"
                + "INSERT into hive_user partition(dt = '2023-04-25',hh = '12') SELECT 4, 'lucy', 25;"
                + "INSERT into hive_user partition(dt = '2023-04-26',hh = '13') SELECT 5, 'jack', 26";
        StructType dataSchema = new StructType(
            new TableField("id", IntegerType.INSTANCE, false),
            new TableField("name", BinaryStringType.INSTANCE, true),
            new TableField("age", IntegerType.INSTANCE, false)
        );
        StructType partitionSchema = new StructType(
            new TableField("hh", BinaryStringType.INSTANCE, false),
            new TableField("dt", BinaryStringType.INSTANCE, false)
        );
        checkReadHive(ddl, inserts, dataSchema, partitionSchema,
            "[1, jim, 20, 10, 2023-04-23]\n"
                + "[2, kate, 18, 10, 2023-04-24]\n"
                + "[3, lily, 22, 11, 2023-04-24]\n"
                + "[4, lucy, 25, 12, 2023-04-25]\n"
                + "[5, jack, 26, 13, 2023-04-26]");
    }

    private void checkReadHive(String ddl, String inserts, StructType dataSchema,
                               StructType partitionSchema,
                               String expectResult) throws IOException {
        executeHiveSql("Drop table if exists hive_user");
        executeHiveSql(ddl);
        String[] insertArray = inserts.split(";");
        for (String insert : insertArray) {
            if (StringUtils.isNotEmpty(insert)) {
                executeHiveSql(insert);
            }
        }
        HiveTableSource hiveTableSource = new HiveTableSource();
        Configuration tableConf = new Configuration();
        tableConf.put(HiveConfigKeys.GEAFLOW_DSL_HIVE_DATABASE_NAME, "default");
        tableConf.put(HiveConfigKeys.GEAFLOW_DSL_HIVE_TABLE_NAME, "hive_user");
        tableConf.put(HiveConfigKeys.GEAFLOW_DSL_HIVE_METASTORE_URIS,
            "thrift://localhost:" + metastorePort);

        TableSchema tableSchema = new TableSchema(dataSchema, partitionSchema);
        hiveTableSource.init(tableConf, tableSchema);

        hiveTableSource.open(new DefaultRuntimeContext(tableConf));
        List<Partition> partitions = hiveTableSource.listPartitions();

        TableDeserializer<Row> deserializer = hiveTableSource.getDeserializer(tableConf);
        deserializer.init(tableConf, tableSchema);

        List<Row> readRows = new ArrayList<>();
        for (Partition partition : partitions) {
            LOGGER.info("partition: {}", partition.getName());
            FetchData<Row> fetchData = hiveTableSource.fetch(partition, Optional.empty(),
                new AllFetchWindow(1));
            Iterator<Row> rowIterator = fetchData.getDataIterator();
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                readRows.addAll(deserializer.deserialize(row));
            }
        }
        List<String> lines = readRows.stream().map(Object::toString)
            .sorted().collect(Collectors.toList());
        Assert.assertEquals(StringUtils.join(lines, "\n"), expectResult);

        hiveTableSource.close();
    }
}
