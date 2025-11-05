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

import com.google.gson.Gson;
import java.io.IOException;
import java.util.Objects;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchTableSink implements TableSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchTableSink.class);
    private static final Gson GSON = new Gson();

    private StructType schema;
    private String hosts;
    private String indexName;
    private String documentIdField;
    private String username;
    private String password;
    private int batchSize;
    private int connectionTimeout;
    private int socketTimeout;
    
    private RestHighLevelClient client;
    private BulkRequest bulkRequest;
    private int batchCounter = 0;

    @Override
    public void init(Configuration tableConf, StructType schema) {
        LOGGER.info("Prepare with config: {}, \n schema: {}", tableConf, schema);
        this.schema = schema;

        this.hosts = tableConf.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS);
        this.indexName = tableConf.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX);
        this.documentIdField = tableConf.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_DOCUMENT_ID_FIELD, null);
        this.username = tableConf.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_USERNAME, null);
        this.password = tableConf.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_PASSWORD, null);
        this.batchSize = tableConf.getInteger(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_BATCH_SIZE, 
            ElasticsearchConstants.DEFAULT_BATCH_SIZE);
        this.connectionTimeout = tableConf.getInteger(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_CONNECTION_TIMEOUT,
            ElasticsearchConstants.DEFAULT_CONNECTION_TIMEOUT);
        this.socketTimeout = tableConf.getInteger(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_SOCKET_TIMEOUT,
            ElasticsearchConstants.DEFAULT_SOCKET_TIMEOUT);
    }

    @Override
    public void open(RuntimeContext context) {
        try {
            this.client = createElasticsearchClient();
            this.bulkRequest = new BulkRequest();
        } catch (Exception e) {
            throw new GeaFlowDSLException("Failed to create Elasticsearch client", e);
        }
    }

    @Override
    public void write(Row row) throws IOException {
        // Convert row to JSON document
        String jsonDocument = rowToJson(row);
        
        // Create index request
        IndexRequest request = new IndexRequest(indexName);
        request.source(jsonDocument, XContentType.JSON);
        
        // Set document ID if specified
        if (documentIdField != null && !documentIdField.isEmpty()) {
            int idFieldIndex = schema.indexOf(documentIdField);
            if (idFieldIndex >= 0) {
                Object idValue = row.getField(idFieldIndex, schema.getType(idFieldIndex));
                if (idValue != null) {
                    request.id(idValue.toString());
                }
            }
        }
        
        // Add to bulk request
        bulkRequest.add(request);
        batchCounter++;
        
        // Flush if batch size reached
        if (batchCounter >= batchSize) {
            flush();
        }
    }

    @Override
    public void finish() throws IOException {
        flush();
    }

    @Override
    public void close() {
        try {
            if (Objects.nonNull(this.client)) {
                client.close();
            }
        } catch (IOException e) {
            throw new GeaFlowDSLException("Failed to close Elasticsearch client", e);
        }
    }

    private void flush() throws IOException {
        if (batchCounter > 0 && client != null) {
            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            if (bulkResponse.hasFailures()) {
                LOGGER.error("Bulk request failed: {}", bulkResponse.buildFailureMessage());
                throw new IOException("Bulk request failed: " + bulkResponse.buildFailureMessage());
            }
            bulkRequest = new BulkRequest();
            batchCounter = 0;
        }
    }

    private String rowToJson(Row row) {
        // Convert Row to JSON string
        java.util.Map<String, Object> map = new java.util.HashMap<>();
        java.util.List<String> fieldNames = schema.getFieldNames();
        
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            Object fieldValue = row.getField(i, schema.getType(i));
            map.put(fieldName, fieldValue);
        }
        
        return GSON.toJson(map);
    }
    
    private RestHighLevelClient createElasticsearchClient() {
        try {
            String[] hostArray = hosts.split(",");
            HttpHost[] httpHosts = new HttpHost[hostArray.length];
            
            for (int i = 0; i < hostArray.length; i++) {
                String host = hostArray[i].trim();
                if (host.startsWith("http://")) {
                    host = host.substring(7);
                } else if (host.startsWith("https://")) {
                    host = host.substring(8);
                }
                
                String[] parts = host.split(":");
                String hostname = parts[0];
                int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9200;
                httpHosts[i] = new HttpHost(hostname, port, "http");
            }
            
            RestClientBuilder builder = RestClient.builder(httpHosts);
            
            // Configure timeouts
            builder.setRequestConfigCallback(requestConfigBuilder -> {
                requestConfigBuilder.setConnectTimeout(connectionTimeout);
                requestConfigBuilder.setSocketTimeout(socketTimeout);
                return requestConfigBuilder;
            });
            
            // Configure authentication if provided
            if (username != null && !username.isEmpty() && password != null) {
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, 
                    new UsernamePasswordCredentials(username, password));
                
                builder.setHttpClientConfigCallback(httpClientBuilder -> {
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    return httpClientBuilder;
                });
            }
            
            return new RestHighLevelClient(builder);
        } catch (Exception e) {
            throw new GeaFlowDSLException("Failed to create Elasticsearch client", e);
        }
    }
}