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

import java.util.List;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.doris.DorisTableSource.DorisPartition;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DorisTableSourceTest {

    private TableSchema schema() {
        return new TableSchema(new StructType(
            new TableField("id", Types.LONG, false),
            new TableField("name", Types.BINARY_STRING, true)));
    }

    private DorisTableSource newSource(Configuration conf) {
        Configuration base = conf;
        base.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_JDBC_URL, "jdbc:mysql://127.0.0.1:9030/db");
        base.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_DATABASE, "db");
        base.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_TABLE, "t");
        DorisTableSource source = new DorisTableSource();
        source.init(base, schema());
        return source;
    }

    @Test
    public void testSinglePartition() {
        List<Partition> partitions = newSource(new Configuration()).listPartitions();
        Assert.assertEquals(partitions.size(), 1);
        Assert.assertEquals(((DorisPartition) partitions.get(0)).getWhereClause(), "");
    }

    @Test
    public void testRangePartitions() {
        Configuration conf = new Configuration();
        conf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_SOURCE_PARTITION_NUM, "4");
        conf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_SOURCE_PARTITION_COLUMN, "id");
        conf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_SOURCE_PARTITION_LOWERBOUND, "0");
        conf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_SOURCE_PARTITION_UPPERBOUND, "100");
        List<Partition> partitions = newSource(conf).listPartitions();
        Assert.assertEquals(partitions.size(), 4);
        // The first partition also covers rows whose partition column is entirely NULL.
        Assert.assertTrue(((DorisPartition) partitions.get(0)).getWhereClause()
            .contains("id IS NULL"));
    }

    @Test
    public void testCustomPartitions() {
        Configuration conf = new Configuration();
        conf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_SOURCE_PARTITION_MODE, "custom");
        conf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_SOURCE_PARTITION_CLAUSES,
            "dt='2024-01-01';dt='2024-01-02';dt='2024-01-03'");
        List<Partition> partitions = newSource(conf).listPartitions();
        Assert.assertEquals(partitions.size(), 3);
        Assert.assertEquals(((DorisPartition) partitions.get(1)).getWhereClause(),
            "WHERE dt='2024-01-02'");
    }

    @Test
    public void testCustomPartitionsEmptyFallsBackToSingle() {
        Configuration conf = new Configuration();
        conf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_SOURCE_PARTITION_MODE, "custom");
        List<Partition> partitions = newSource(conf).listPartitions();
        Assert.assertEquals(partitions.size(), 1);
        Assert.assertEquals(((DorisPartition) partitions.get(0)).getWhereClause(), "");
    }
}
