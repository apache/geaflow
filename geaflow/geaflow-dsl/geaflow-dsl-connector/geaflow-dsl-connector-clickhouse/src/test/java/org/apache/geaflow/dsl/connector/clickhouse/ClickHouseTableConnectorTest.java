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

import java.util.List;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableConnector;
import org.apache.geaflow.dsl.connector.api.util.ConnectorFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseTableConnectorTest {

    private static TableField[] fields() {
        return new TableField[]{
            new TableField("id", Types.LONG, false),
            new TableField("name", Types.BINARY_STRING, false),
            new TableField("score", Types.DOUBLE, false)
        };
    }

    private static Configuration baseConf(String tableName) {
        Configuration conf = new Configuration();
        conf.put(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_URL, "jdbc:clickhouse://localhost:8123/default");
        conf.put(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_TABLE_NAME, tableName);
        return conf;
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
}
