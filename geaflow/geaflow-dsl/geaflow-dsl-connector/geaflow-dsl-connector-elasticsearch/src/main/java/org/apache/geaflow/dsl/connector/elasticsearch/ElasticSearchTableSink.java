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

import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.connector.elasticsearch.utils.ElasticSearchConnectorUtils;
import org.apache.geaflow.dsl.connector.elasticsearch.utils.ElasticSearchJsonSerializer;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ElasticSearchTableSink implements TableSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchTableSink.class);

    private String hosts;
    private String indexName;
    private String timestampField;
    private String idField;
    private int connectTimeout;
    private int socketTimeout;
    private int requestTimeout;
    private long flushIntervalMs;
    private long bulkSizeMb;
    private int concurrentRequests;
    private String backoffEnabled;
    private StructType schema;
    private String username;
    private String password;


    private transient RestHighLevelClient esClient;
    private transient BulkProcessor bulkProcessor;
    private transient AtomicInteger pendingDocs = new AtomicInteger(0);
    private transient Map<String, Integer> failedDocs = new ConcurrentHashMap<>();
    private transient ElasticSearchJsonSerializer elasticSearchJsonSerializer;

    @Override
    public void init(Configuration conf, StructType schema) {
        this.schema = schema;
        this.hosts = conf.getString(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS);
        this.indexName = conf.getString(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX);
        this.timestampField = conf.getString(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_TIMESTAMP_FIELD);
        this.idField = conf.getString(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_ID_FIELD);
        this.flushIntervalMs = conf.getLong(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_FLUSH_INTERVAL_MS);
        this.bulkSizeMb = conf.getLong(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_BULK_SIZE_MB);
        this.concurrentRequests = conf.getInteger(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_CONCURRENT_REQUESTS);
        this.backoffEnabled = conf.getString(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_BACKOFF);

        this.connectTimeout = conf.getInteger(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_CONNECT_TIMEOUT);
        this.socketTimeout = conf.getInteger(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_SOCKET_TIMEOUT);
        this.requestTimeout = conf.getInteger(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_REQUEST_TIMEOUT);

        this.username = conf.getString(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_USERNAME, "");
        this.password = conf.getString(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_PASSWORD, "");
    }

    @Override
    public void open(RuntimeContext context) {
        elasticSearchJsonSerializer = new ElasticSearchJsonSerializer(schema, timestampField);

        HttpHost[] httpHosts = ElasticSearchConnectorUtils.toHttpHosts(this.hosts.split(","));
        RestClientBuilder builder= RestClient.builder(httpHosts)
                .setRequestConfigCallback(
                        requestConfigBuilder -> requestConfigBuilder
                                .setConnectTimeout(connectTimeout)
                                .setSocketTimeout(socketTimeout)
                                .setConnectionRequestTimeout(requestTimeout)
                );

        if (username != null && !username.isEmpty() && password != null) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password));

            builder.setHttpClientConfigCallback(httpClientBuilder ->
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
            );

        }
        this.esClient = new RestHighLevelClient(builder);



        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                LOGGER.debug("Executing bulk request with {} actions", request.numberOfActions());
            }
            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                int count = request.numberOfActions();
                pendingDocs.addAndGet(-count);

                if (response.hasFailures()) {
                    handleBulkFailures(response);
                }
            }
            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                int count = request.numberOfActions();
                pendingDocs.addAndGet(-count);
                LOGGER.error("Bulk request failed", failure);
            }
        };
        BulkProcessor.Builder bulkBuilder = BulkProcessor.builder(
                (request, bulkListener) -> esClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                listener,
                "geaflow-bulk-processor"
        );
        // bulk size
        bulkBuilder.setBulkSize(new ByteSizeValue(bulkSizeMb, ByteSizeUnit.MB));
        // flush interval
        bulkBuilder.setFlushInterval(TimeValue.timeValueMillis(flushIntervalMs));
        // concurrent requests
        bulkBuilder.setConcurrentRequests(concurrentRequests);
        // backoff policy
        if ("true".equalsIgnoreCase(backoffEnabled)) {
            bulkBuilder.setBackoffPolicy(
                    BackoffPolicy.exponentialBackoff(
                            TimeValue.timeValueMillis(100), 3));
        }
        this.bulkProcessor = bulkBuilder.build();
    }

    @Override
    public void write(Row row) throws IOException {
        try{
            Map<String, Object> document = elasticSearchJsonSerializer.convert((row));

            IndexRequest request = new IndexRequest(indexName)
                    .source(document);

            if (idField != null) {
                int idFieldIndex = schema.indexOf(idField);
                if(idFieldIndex >= 0){
                    Object idValue = row.getField(idFieldIndex, schema.getType(idFieldIndex));
                    request.id(String.valueOf(idValue));
                }
            }
            bulkProcessor.add(request);
            pendingDocs.incrementAndGet();
        }catch (Exception e){
            LOGGER.error("Failed to convert row to document", e);
            throw new IOException("Row conversion error", e);
        }


    }

    @Override
    public void finish() throws IOException {
        try {
            flush();

            // close bulkProcessor and wait for completion
            if (bulkProcessor != null) {
                bulkProcessor.awaitClose(30, java.util.concurrent.TimeUnit.SECONDS);
            }

            // check for failed documents
            if (!failedDocs.isEmpty()) {
                LOGGER.error("Failed to index {} documents with errors:", pendingDocs.get());
                failedDocs.forEach((msg, count) ->
                        LOGGER.error(" - {} occurrences: {}", count, msg)
                );
                throw new GeaFlowDSLException(
                        "Failed to index " + failedDocs.values().stream().mapToInt(i->i).sum() +
                                " documents. See logs for details.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Bulk processor close interrupted", e);
        }
    }

    private void flush(){
        if (bulkProcessor != null) {
            bulkProcessor.flush();
        }
    }

    @Override
    public void close() {
        try {
            // flush remaining documents
            flush();
            if (bulkProcessor != null) {
                bulkProcessor.close();
            }
            if (esClient != null) {
                esClient.close();
            }
        } catch (IOException e) {
            throw new GeaFlowDSLException("Failed to close Elasticsearch client", e);
        }
    }


    private void handleBulkFailures(BulkResponse response) {
        AtomicInteger failureCount = new AtomicInteger();
        for (BulkItemResponse item : response) {
            if (item.isFailed()) {
                failureCount.incrementAndGet();
                String docId = extractDocId(item);
                failedDocs.merge(item.getFailureMessage(), 1, Integer::sum);

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Failed to index document {}: {}", docId, item.getFailureMessage());
                }
            }
        }
        LOGGER.warn("Bulk operation had {} failures out of {} requests",
                failureCount.get(), response.getItems().length);
    }

    private String extractDocId(BulkItemResponse item) {
        return item.getFailure().getId() != null ?
                item.getFailure().getId() :
                "unknown";
    }
}