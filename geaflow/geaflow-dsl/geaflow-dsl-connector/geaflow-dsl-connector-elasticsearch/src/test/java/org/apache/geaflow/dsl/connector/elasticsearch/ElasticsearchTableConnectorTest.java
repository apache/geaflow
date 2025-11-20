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

import org.testng.annotations.Test;
import static org.testng.Assert.*;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.DataType;
import org.apache.geaflow.dsl.common.types.Types;

public class ElasticsearchTableConnectorTest {

    @Test
    public void testGetType() {
        ElasticsearchTableConnector connector = new ElasticsearchTableConnector();
        assertEquals(connector.getType(), "ELASTICSEARCH");
    }

    @Test
    public void testCreateSource() {
        ElasticsearchTableConnector connector = new ElasticsearchTableConnector();
        Configuration conf = new Configuration();
        conf.setString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS, "localhost:9200");
        conf.setString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX, "test_index");
        
        // Test that source can be created without throwing exception
        assertNotNull(connector.createSource(conf));
    }

    @Test
    public void testCreateSink() {
        ElasticsearchTableConnector connector = new ElasticsearchTableConnector();
        Configuration conf = new Configuration();
        conf.setString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS, "localhost:9200");
        conf.setString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX, "test_index");
        
        // Test that sink can be created without throwing exception
        assertNotNull(connector.createSink(conf));
    }
    
    @Test
    public void testConfigKeys() {
        // Test that all config keys are properly defined
        assertNotNull(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS);
        assertNotNull(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX);
        assertNotNull(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_DOCUMENT_ID_FIELD);
        assertNotNull(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_USERNAME);
        assertNotNull(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_PASSWORD);
        assertNotNull(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_BATCH_SIZE);
        assertNotNull(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_SCROLL_TIMEOUT);
        assertNotNull(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_CONNECTION_TIMEOUT);
        assertNotNull(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_SOCKET_TIMEOUT);
    }
    
    @Test
    public void testConstants() {
        // Test that constants have valid values
        assertTrue(ElasticsearchConstants.DEFAULT_BATCH_SIZE > 0);
        assertNotNull(ElasticsearchConstants.DEFAULT_SCROLL_TIMEOUT);
        assertTrue(ElasticsearchConstants.DEFAULT_CONNECTION_TIMEOUT >= 0);
        assertTrue(ElasticsearchConstants.DEFAULT_SOCKET_TIMEOUT >= 0);
    }
}