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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchTableSink implements TableSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchTableSink.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private Configuration tableConf;
    private StructType schema;
    private String[] hosts;
    private String index;
    private String username;
    private String password;
    private int flushSize;
    private int retryTimes;

    private transient RestHighLevelClient client;
    private transient List<IndexRequest> batchRequests;

    @Override
    public void init(Configuration tableConf, StructType schema) {
        LOGGER.info("Initializing ElasticSearch sink with config: {}, schema: {}", tableConf, schema);
        this.tableConf = tableConf;
        this.schema = schema;

        String hostsStr = tableConf.getString(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS);
        this.hosts = hostsStr.split(",");
        this.index = tableConf.getString(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX);
        this.username = tableConf.getString(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_USERNAME, (String) null);
        this.password = tableConf.getString(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_PASSWORD, (String) null);
        this.flushSize = tableConf.getInteger(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_FLUSH_SIZE);
        this.retryTimes = tableConf.getInteger(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_RETRY_TIMES);
    }

    @Override
    public void open(RuntimeContext context) {
        LOGGER.info("Opening ElasticSearch sink");
        try {
            // Create ElasticSearch client
            HttpHost[] httpHosts = new HttpHost[hosts.length];
            for (int i = 0; i < hosts.length; i++) {
                httpHosts[i] = HttpHost.create(hosts[i]);
            }
            
            RestClientBuilder builder = RestClient.builder(httpHosts);
            
            if (username != null && password != null) {
                // 设置认证信息
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY,
                        new UsernamePasswordCredentials(username, password));

                builder.setHttpClientConfigCallback(httpClientBuilder ->
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

            }
            
            client = new RestHighLevelClient(builder);
            batchRequests = new ArrayList<>();
        } catch (Exception e) {
            throw new GeaFlowDSLException("Failed to create ElasticSearch client", e);
        }
    }

    @Override
    public void write(Row row) throws IOException {
        // Convert row to JSON document
        Map<String, Object> document = new HashMap<>();
        for (int i = 0; i < schema.size(); i++) {
            document.put(schema.getField(i).getName(), row.getField(i, schema.getField(i).getType()));
        }

        // Create index request
        String jsonDoc = OBJECT_MAPPER.writeValueAsString(document);
        IndexRequest request = new IndexRequest(index)
            .source(jsonDoc, XContentType.JSON);

        batchRequests.add(request);

        // Flush if batch size reached
        if (batchRequests.size() >= flushSize) {
            flush();
        }
    }

    @Override
    public void finish() throws IOException {
        flush();
    }

    @Override
    public void close() {
        LOGGER.info("Closing ElasticSearch sink");
        try {
            flush();
        } catch (IOException e) {
            LOGGER.error("Failed to flush remaining records", e);
        }
        
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                LOGGER.error("Failed to close ElasticSearch client", e);
            }
        }
    }

    private void flush() throws IOException {
        if (batchRequests.isEmpty()) {
            return;
        }

        LOGGER.info("Flushing {} records to ElasticSearch", batchRequests.size());
        
        BulkRequest bulkRequest = new BulkRequest();
        for (IndexRequest request : batchRequests) {
            bulkRequest.add(request);
        }

        boolean success = false;
        Exception lastException = null;
        
        for (int i = 0; i <= retryTimes; i++) {
            try {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                if (!bulkResponse.hasFailures()) {
                    success = true;
                    break;
                } else {
                    lastException = new IOException("Bulk request failed: " + bulkResponse.buildFailureMessage());
                }
            } catch (Exception e) {
                lastException = e;
                LOGGER.warn("Failed to execute bulk request, attempt {}/{}", i + 1, retryTimes + 1, e);
                
                if (i < retryTimes) {
                    // Wait before retry
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted while waiting for retry", ie);
                    }
                }
            }
        }

        if (!success) {
            batchRequests.clear();
            throw new IOException("Failed to execute bulk request after " + (retryTimes + 1) + " attempts", lastException);
        }

        batchRequests.clear();
        LOGGER.info("Successfully flushed records to ElasticSearch");
    }
}
