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


public class ElasticsearchTableConnectorTest {

    @Test
    public void testGetType() {
        ElasticSearchTableConnector connector = new ElasticSearchTableConnector();
        assertEquals(connector.getType(), "ELASTICSEARCH");
    }

    @Test
    public void testCreateSource() {
        ElasticSearchTableConnector connector = new ElasticSearchTableConnector();
        Configuration conf = new Configuration();
        conf.put(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS, "localhost:9200");
        conf.put(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX, "test_index");

        // Test that source can be created without throwing exception
        assertNotNull(connector.createSource(conf));
    }

    @Test
    public void testCreateSink() {
        ElasticSearchTableConnector connector = new ElasticSearchTableConnector();
        Configuration conf = new Configuration();
        conf.put(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS, "localhost:9200");
        conf.put(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX, "test_index");

        // Test that sink can be created without throwing exception
        assertNotNull(connector.createSink(conf));
    }

    @Test
    public void testConfigKeys() {
        // Test that all config keys are properly defined
        assertNotNull(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS);
        assertNotNull(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX);
        assertNotNull(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_USERNAME);
        assertNotNull(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_PASSWORD);
        assertNotNull(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_SOCKET_TIMEOUT);
        assertNotNull(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_BACKOFF);
        assertNotNull(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_BULK_SIZE_MB);
    }
}