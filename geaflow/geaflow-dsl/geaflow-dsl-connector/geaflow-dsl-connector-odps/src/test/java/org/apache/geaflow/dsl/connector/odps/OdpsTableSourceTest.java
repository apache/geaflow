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

package org.apache.geaflow.dsl.connector.odps;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.ArrayRecord;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.primitive.BinaryStringType;
import org.apache.geaflow.common.type.primitive.BooleanType;
import org.apache.geaflow.common.type.primitive.ByteType;
import org.apache.geaflow.common.type.primitive.DecimalType;
import org.apache.geaflow.common.type.primitive.DoubleType;
import org.apache.geaflow.common.type.primitive.FloatType;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.common.type.primitive.LongType;
import org.apache.geaflow.common.type.primitive.TimestampType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.common.types.VoidType;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import org.apache.geaflow.dsl.connector.api.window.SizeFetchWindow;
import org.apache.geaflow.dsl.connector.odps.OdpsTableSource.OdpsShardPartition;
import org.apache.geaflow.dsl.connector.odps.utils.OdpsConnectorUtils;
import org.apache.geaflow.dsl.connector.odps.utils.OdpsRecordWithPartitionSpec;
import org.apache.geaflow.runtime.core.context.DefaultRuntimeContext;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OdpsTableSourceTest {

    @Test(enabled = false)
    public void testOdpsTableSource() throws IOException {
        OdpsTableSource source = new OdpsTableSource();
        Configuration config = new Configuration();
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ENDPOINT, "http://test.odps.com/api");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_PROJECT, "test_project");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_TABLE, "test_table");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ACCESS_KEY, "test_access_key");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ACCESS_ID, "test_access_id");
        TableSchema schema = new TableSchema(
            new TableField("src_id", IntegerType.INSTANCE, false),
            new TableField("target_id", IntegerType.INSTANCE, false),
            new TableField("relation", IntegerType.INSTANCE, false)
        );
        source.init(config, schema);
        try {
            source.open(new DefaultRuntimeContext(config));
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Can't bind xml to com.aliyun.odps.Table$TableModel");
        }
        try {
            List<Partition> odpsPartitions = source.listPartitions();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Cannot list partitions from ODPS, endPoint: http://test.odps.com/api");
        }
        Partition firstPartition = new OdpsShardPartition("prefix-", new PartitionSpec("dt='20000000'"));
        FetchWindow window = new SizeFetchWindow(1, 100L);
        try {
            FetchData data = source.fetch(firstPartition, Optional.empty(), window);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Cannot get Odps session.");
        }
        TableDeserializer deserializer = source.getDeserializer(config);
        deserializer.init(config, schema);
        deserializer.deserialize(new OdpsRecordWithPartitionSpec(new ArrayRecord(
            new Column[]{new Column("src_id", OdpsType.STRING), new Column("test", OdpsType.STRING)},
            new Object[]{"16", "32"}), null));
    }

    @Test(enabled = false)
    public void testPartitionSpec() throws IOException {
        OdpsTableSource source = new OdpsTableSource();
        Configuration config = new Configuration();
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ENDPOINT, "http://test.odps.com/api");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_PROJECT, "test_project");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_TABLE, "test_table");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ACCESS_KEY, "test_access_key");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ACCESS_ID, "test_access_id");
        TableSchema schema = new TableSchema(
            new TableField("src_id", IntegerType.INSTANCE, false),
            new TableField("target_id", IntegerType.INSTANCE, false),
            new TableField("relation", IntegerType.INSTANCE, false)
        );
        source.init(config, schema);
        try {
            source.open(new DefaultRuntimeContext(config));
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Can't bind xml to com.aliyun.odps.Table$TableModel");
        }
        try {
            List<Partition> odpsPartitions = source.listPartitions();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Cannot list partitions from ODPS, endPoint: http://test.odps.com/api");
        }
        Partition firstPartition = new OdpsShardPartition("prefix-", new PartitionSpec("dt='20000000'"));
        FetchWindow window = new SizeFetchWindow(1, 100L);
        try {
            FetchData data = source.fetch(firstPartition, Optional.empty(), window);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Cannot get Odps session.");
        }
        TableDeserializer deserializer = source.getDeserializer(config);
        deserializer.init(config, schema);
        deserializer.deserialize(new OdpsRecordWithPartitionSpec(new ArrayRecord(
            new Column[]{new Column("src_id", OdpsType.STRING), new Column("test", OdpsType.STRING)},
            new Object[]{"16", "32"}), null));
    }

    @Test
    public void testOdpsConnectorUtils() {
        Assert.assertTrue(OdpsConnectorUtils.typeEquals(OdpsType.SMALLINT, LongType.INSTANCE));
        Assert.assertTrue(OdpsConnectorUtils.typeEquals(OdpsType.INT, LongType.INSTANCE));
        Assert.assertTrue(OdpsConnectorUtils.typeEquals(OdpsType.FLOAT, FloatType.INSTANCE));
        Assert.assertTrue(OdpsConnectorUtils.typeEquals(OdpsType.DOUBLE, DoubleType.INSTANCE));
        Assert.assertTrue(OdpsConnectorUtils.typeEquals(OdpsType.BOOLEAN, BooleanType.INSTANCE));
        Assert.assertTrue(OdpsConnectorUtils.typeEquals(OdpsType.STRING, BinaryStringType.INSTANCE));
        Assert.assertTrue(OdpsConnectorUtils.typeEquals(OdpsType.BINARY, ByteType.INSTANCE));
        Assert.assertTrue(OdpsConnectorUtils.typeEquals(OdpsType.DECIMAL, DecimalType.INSTANCE));
        Assert.assertTrue(OdpsConnectorUtils.typeEquals(OdpsType.VOID, VoidType.INSTANCE));
        Assert.assertTrue(OdpsConnectorUtils.typeEquals(OdpsType.DATE, TimestampType.INSTANCE));
    }
}
