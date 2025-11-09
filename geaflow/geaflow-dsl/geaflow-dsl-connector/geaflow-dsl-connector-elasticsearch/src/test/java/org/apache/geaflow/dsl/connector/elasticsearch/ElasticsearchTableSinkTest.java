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
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ElasticsearchTableSinkTest {

    private ElasticsearchTableSink sink;
    private Configuration config;
    private StructType schema;

    @BeforeMethod
    public void setUp() {
        sink = new ElasticsearchTableSink();
        config = new Configuration();
        config.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS, "localhost:9200");
        config.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX, "test_index");
        config.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_DOCUMENT_ID_FIELD, "id");

        TableField idField = new TableField("id", Types.INTEGER, false);
        TableField nameField = new TableField("name", Types.STRING, false);
        TableField ageField = new TableField("age", Types.INTEGER, false);
        schema = new StructType(Arrays.asList(idField, nameField, ageField));
    }

    @Test
    public void testInit() {
        sink.init(config, schema);
        Assert.assertNotNull(sink);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testInitWithoutIndex() {
        Configuration invalidConfig = new Configuration();
        invalidConfig.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS, "localhost:9200");
        sink.init(invalidConfig, schema);
    }

    @Test
    public void testInitWithoutIdField() {
        Configuration invalidConfig = new Configuration();
        invalidConfig.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS, "localhost:9200");
        invalidConfig.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX, "test_index");
        sink.init(invalidConfig, schema);
        Assert.assertNotNull(sink);
    }

    @Test
    public void testBatchSizeConfiguration() {
        config.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_BATCH_SIZE, "500");
        sink.init(config, schema);
        Assert.assertNotNull(sink);
    }

    @Test
    public void testWriteRow() {
        sink.init(config, schema);

        Row row = ObjectRow.create(1, "Alice", 25);
        Assert.assertNotNull(row);
    }

    @Test
    public void testMultipleWrites() {
        sink.init(config, schema);

        for (int i = 0; i < 10; i++) {
            Row row = ObjectRow.create(i, "User" + i, 20 + i);
            Assert.assertNotNull(row);
        }
    }
}
