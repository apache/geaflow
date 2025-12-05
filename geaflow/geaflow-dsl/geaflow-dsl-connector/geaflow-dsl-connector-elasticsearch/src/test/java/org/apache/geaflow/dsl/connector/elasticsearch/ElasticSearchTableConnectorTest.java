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


import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Assert;
import org.junit.Test;
import org.apache.geaflow.common.config.Configuration;


public class ElasticSearchTableConnectorTest {

    @Test
    public void testElasticSearchConnector() {
        ElasticSearchTableConnector connector = new ElasticSearchTableConnector();
        Assert.assertEquals(connector.getType(), "ELASTICSEARCH");
        
        // Test that we can create a sink
        Configuration conf = new Configuration();
        conf.put(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS.getKey(), "http://127.0.0.1:9200");
        conf.put(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX.getKey(), "test");
        
        TableSink sink = connector.createSink(conf);
        Assert.assertNotNull(sink);
        Assert.assertEquals(ElasticSearchTableSink.class, sink.getClass());
    }

    @Test
    public void testElasticSearchConfigKeys() {
        // Test that config keys are properly defined
        Assert.assertNotNull(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS);
        Assert.assertNotNull(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX);
        Assert.assertNotNull(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_USERNAME);
        Assert.assertNotNull(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_PASSWORD);
        Assert.assertNotNull(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_FLUSH_SIZE);
        Assert.assertNotNull(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_RETRY_TIMES);
        
        // Test default values
        Assert.assertEquals(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_FLUSH_SIZE.getDefaultValue(), Integer.valueOf(100));
        Assert.assertEquals(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_RETRY_TIMES.getDefaultValue(), Integer.valueOf(3));
    }
    
    @Test
    public void testElasticSearchTableSinkInit() throws NoSuchFieldException, IllegalAccessException {
        ElasticSearchTableSink sink = new ElasticSearchTableSink();
        
        Configuration conf = new Configuration();
        conf.put(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS.getKey(), "http://127.0.0.1:9200");
        conf.put(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX.getKey(), "test");
        conf.put(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_USERNAME.getKey(), "zhangsan");
        conf.put(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_PASSWORD, "123");

        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.INTEGER, true));
        fields.add(new TableField("name", Types.STRING, true));
        StructType schema = new StructType(fields);
        
        // This should not throw an exception
        sink.init(conf, schema);

        Field hosts = sink.getClass().getDeclaredField("hosts");
        hosts.setAccessible(true);
        String[] hostsStr = (String[]) hosts.get(sink);;
        Field index = sink.getClass().getDeclaredField("index");
        index.setAccessible(true);
        Field username = sink.getClass().getDeclaredField("username");
        username.setAccessible(true);
        Field password = sink.getClass().getDeclaredField("password");
        password.setAccessible(true);


        // Verify the configuration was set correctly
        Assert.assertEquals(ElasticSearchTableSink.class, sink.getClass());
        Assert.assertEquals("http://127.0.0.1:9200", hostsStr[0]);
        Assert.assertEquals("test",index.get(sink));
        Assert.assertEquals("zhangsan",username.get(sink));
        Assert.assertEquals("123",password.get(sink));
    }
    
    @Test
    public void testElasticSearchTableSinkOpen() {
        ElasticSearchTableSink sink = new ElasticSearchTableSink();
        
        Configuration conf = new Configuration();
        conf.put(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS.getKey(), "http://127.0.0.1:9200");
        conf.put(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX.getKey(), "test");
        
        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.INTEGER, true));
        fields.add(new TableField("name", Types.STRING, true));
        StructType schema = new StructType(fields);
        
        // Initialize the sink
        sink.init(conf, schema);
        
        // Test open method - verify it doesn't throw unexpected exceptions
        try {
            sink.open(null);
            Field client = sink.getClass().getDeclaredField("client");
            client.setAccessible(true);
            RestHighLevelClient restHighLevelClient = (RestHighLevelClient) client.get(sink);
            Assert.assertNotNull(restHighLevelClient);
            // No exception means the client initialized successfully (or mock is working)
        } catch (Exception e) {
            // Verify the exception is equal to define
            Assert.assertEquals(GeaFlowDSLException.class, e.getClass());
        }
    }
    
    @Test
    public void testElasticSearchTableSinkWrite() {
        ElasticSearchTableSink sink = new ElasticSearchTableSink();
        
        Configuration conf = new Configuration();
        conf.put(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS.getKey(), "http://127.0.0.1:9200");
        conf.put(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX.getKey(), "test");
        conf.put(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_FLUSH_SIZE.getKey(), "1"); // Small flush size for testing
        
        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.INTEGER, true));
        fields.add(new TableField("name", Types.STRING, true));
        StructType schema = new StructType(fields);
        
        // Initialize the sink
        sink.init(conf, schema);

        // Initialize the client and batchRequests
        sink.open(null);
        
        // Create a proper row that matches the schema
        Row row = ObjectRow.create(1, "张三");
        
        // Test write method - verify it doesn't throw unexpected exceptions
        try {
            sink.write(row);
        } catch (Exception e) {
            // Verify the exception is equal to define
            Assert.assertEquals("Failed to execute bulk request after 4 attempts", e.getMessage());

            // Verify the cause is connection-related,because we don't have a real Elasticsearch server
            Assert.assertEquals("java.util.concurrent.ExecutionException: java.net.ConnectException: Connection refused",e.getCause().getMessage());
        }
    }
    
    @Test
    public void testElasticSearchTableSinkWriteWithMultipleRows() {
        ElasticSearchTableSink sink = new ElasticSearchTableSink();
        
        Configuration conf = new Configuration();
        conf.put(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS.getKey(), "http://127.0.0.1:9200");
        conf.put(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX.getKey(), "test");
        conf.put(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_FLUSH_SIZE.getKey(), "3"); // Flush after 3 rows
        
        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", Types.INTEGER, true));
        fields.add(new TableField("name", Types.STRING, true));
        StructType schema = new StructType(fields);
        
        // Initialize the sink
        sink.init(conf, schema);

        // Initialize the client and batchRequests
        sink.open(null);
        
        // Create multiple proper rows that match the schema
        Row row1 = ObjectRow.create(1, "张三");
        Row row2 = ObjectRow.create(2, "李四"); 
        Row row3 = ObjectRow.create(3, "王五");
        
        // Test write method with multiple rows
        try {
            sink.write(row1);
            sink.write(row2);

            // Verify batchRequests contains 2 requests, flush when up to 3
            Field batchRequests = sink.getClass().getDeclaredField("batchRequests");
            batchRequests.setAccessible(true);
            List<IndexRequest> batchRequestsList = (List<IndexRequest>) batchRequests.get(sink);
            Assert.assertEquals(2, batchRequestsList.size());

            sink.write(row3);
        } catch (Exception e) {
            // Verify the exception is equal to define
            Assert.assertEquals("Failed to execute bulk request after 4 attempts", e.getMessage());

            // Verify the cause is connection-related,because we don't have a real Elasticsearch server
            Assert.assertEquals("java.util.concurrent.ExecutionException: java.net.ConnectException: Connection refused",e.getCause().getMessage());
        }
    }
}