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

package org.apache.geaflow.dsl.connector.elasticsearch;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ElasticsearchTableSourceTest {

    private ElasticsearchTableSource source;
    private Configuration config;
    private TableSchema schema;

    @BeforeMethod
    public void setUp() {
        source = new ElasticsearchTableSource();
        config = new Configuration();
        config.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS, "localhost:9200");
        config.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX, "test_index");

        TableField idField = new TableField("id", Types.INTEGER, false);
        TableField nameField = new TableField("name", Types.STRING, false);
        schema = new TableSchema(new StructType(Arrays.asList(idField, nameField)));
    }

    @Test
    public void testInit() {
        source.init(config, schema);
        Assert.assertNotNull(source);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testInitWithoutIndex() {
        Configuration invalidConfig = new Configuration();
        invalidConfig.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS, "localhost:9200");
        source.init(invalidConfig, schema);
    }

    @Test
    public void testListPartitions() {
        source.init(config, schema);
        List<Partition> partitions = source.listPartitions();

        Assert.assertNotNull(partitions);
        Assert.assertEquals(partitions.size(), 1);
        Assert.assertEquals(partitions.get(0).getName(), "test_index");
    }

    @Test
    public void testGetDeserializer() {
        source.init(config, schema);
        Assert.assertNotNull(source.getDeserializer(config));
    }

    @Test
    public void testPartitionName() {
        ElasticsearchTableSource.ElasticsearchPartition partition =
                new ElasticsearchTableSource.ElasticsearchPartition("my_index");
        Assert.assertEquals(partition.getName(), "my_index");
    }

    @Test
    public void testOffsetHumanReadable() {
        ElasticsearchTableSource.ElasticsearchOffset offset =
                new ElasticsearchTableSource.ElasticsearchOffset("scroll_123");
        Assert.assertTrue(offset.humanReadable().contains("scroll_123"));
    }
}
