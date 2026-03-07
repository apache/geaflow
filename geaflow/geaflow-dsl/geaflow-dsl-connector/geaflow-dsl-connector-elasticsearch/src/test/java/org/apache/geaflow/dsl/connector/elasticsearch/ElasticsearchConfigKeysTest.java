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

import org.apache.geaflow.common.config.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ElasticsearchConfigKeysTest {

    @Test
    public void testConfigKeys() {
        Configuration config = new Configuration();

        config.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS, "localhost:9200");
        config.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX, "test_index");
        config.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_USERNAME, "elastic");

        Assert.assertEquals(config.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS),
                (Object) "localhost:9200");
        Assert.assertEquals(config.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX),
                (Object) "test_index");
        Assert.assertEquals(config.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_USERNAME),
                (Object) "elastic");
    }

    @Test
    public void testDefaultValues() {
        Configuration config = new Configuration();

        String batchSize = config.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_BATCH_SIZE);
        String scrollTimeout = config.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_SCROLL_TIMEOUT);

        Assert.assertEquals(batchSize, (Object) String.valueOf(ElasticsearchConstants.DEFAULT_BATCH_SIZE));
        Assert.assertEquals(scrollTimeout, (Object) ElasticsearchConstants.DEFAULT_SCROLL_TIMEOUT);
    }

    @Test
    public void testTimeoutValues() {
        Configuration config = new Configuration();

        String connectionTimeout = config.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_CONNECTION_TIMEOUT);
        String socketTimeout = config.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_SOCKET_TIMEOUT);

        Assert.assertEquals(connectionTimeout,
                (Object) String.valueOf(ElasticsearchConstants.DEFAULT_CONNECTION_TIMEOUT));
        Assert.assertEquals(socketTimeout,
                (Object) String.valueOf(ElasticsearchConstants.DEFAULT_SOCKET_TIMEOUT));
    }
}
