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

import java.util.List;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ElasticsearchTableConnectorTest {

    private ElasticsearchTableConnector connector;
    private Configuration config;
    private TableSchema schema;

    @BeforeMethod
    public void setUp() {
        connector = new ElasticsearchTableConnector();
        config = new Configuration();
        config.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS, "localhost:9200");
        config.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX, "test_index");
        config.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_ID_FIELD, "id");

        TableField idField = new TableField("id", Types.INTEGER, false);
        TableField nameField = new TableField("name", Types.STRING, false);
        schema = new TableSchema(new StructType(List.of(idField, nameField)));
    }

    @Test
    public void testGetName() {
        Assert.assertEquals(connector.getType(), "elasticsearch");
    }

    @Test
    public void testGetSource() {
        TableSource source = connector.createSource(config);
        Assert.assertNotNull(source);
        Assert.assertTrue(source instanceof ElasticsearchTableSource);
    }

    @Test
    public void testGetSink() {
        TableSink sink = connector.createSink(config);
        Assert.assertNotNull(sink);
        Assert.assertTrue(sink instanceof ElasticsearchTableSink);
    }

    @Test
    public void testMultipleSourceInstances() {
        TableSource source1 = connector.createSource(config);
        TableSource source2 = connector.createSource(config);

        Assert.assertNotNull(source1);
        Assert.assertNotNull(source2);
        Assert.assertNotSame(source1, source2);
    }

    @Test
    public void testMultipleSinkInstances() {
        TableSink sink1 = connector.createSink(config);
        TableSink sink2 = connector.createSink(config);

        Assert.assertNotNull(sink1);
        Assert.assertNotNull(sink2);
        Assert.assertNotSame(sink1, sink2);
    }
}
