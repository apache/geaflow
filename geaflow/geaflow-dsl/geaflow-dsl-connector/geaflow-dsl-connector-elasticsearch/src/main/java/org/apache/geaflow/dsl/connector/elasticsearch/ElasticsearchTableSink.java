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

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchTableSink implements TableSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchTableSink.class);

    private Configuration config;
    private StructType schema;
    private ElasticsearchClient client;
    private RestClient restClient;

    private String indexName;
    private String idFieldName;
    private int batchSize;
    private List<Row> buffer;
    private ObjectMapper objectMapper;

    @Override
    public void init(Configuration conf, StructType schema) {
        this.config = conf;
        this.schema = schema;

        this.indexName = config.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX);
        if (indexName == null || indexName.isEmpty()) {
            throw new GeaFlowDSLException("Elasticsearch index name is required");
        }

        this.idFieldName = config.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_ID_FIELD);
        this.batchSize = Integer.parseInt(
                config.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_BATCH_SIZE));

        this.buffer = new ArrayList<>();
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void open(RuntimeContext context) {
        String hosts = config.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS);
        if (hosts == null || hosts.isEmpty()) {
            throw new GeaFlowDSLException("Elasticsearch hosts are required");
        }

        String[] hostArray = hosts.split(",");
        HttpHost[] httpHosts = new HttpHost[hostArray.length];

        for (int i = 0; i < hostArray.length; i++) {
            String[] parts = hostArray[i].trim().split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9200;
            httpHosts[i] = new HttpHost(host, port, "http");
        }

        RestClientBuilder builder = RestClient.builder(httpHosts);

        // Configure authentication if provided
        String username = config.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_USERNAME);
        String password = config.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_PASSWORD);

        if (username != null && !username.isEmpty()) {
            BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password));

            builder.setHttpClientConfigCallback(httpClientBuilder ->
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }

        // Configure timeouts
        int connectTimeout = Integer.parseInt(
                config.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_CONNECT_TIMEOUT));
        int socketTimeout = Integer.parseInt(
                config.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_SOCKET_TIMEOUT));

        builder.setRequestConfigCallback(requestConfigBuilder ->
                requestConfigBuilder
                        .setConnectTimeout(connectTimeout)
                        .setSocketTimeout(socketTimeout));

        this.restClient = builder.build();
        this.client = new ElasticsearchClient(
                new RestClientTransport(restClient, new JacksonJsonpMapper()));

        LOGGER.info("Elasticsearch sink opened for index: {}", indexName);
    }

    @Override
    public void write(Row row) throws IOException {
        buffer.add(row);

        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    @Override
    public void finish() throws IOException {
        if (!buffer.isEmpty()) {
            flush();
        }
    }

    @Override
    public void close() {
        try {
            if (restClient != null) {
                restClient.close();
                LOGGER.info("Elasticsearch client closed");
            }
        } catch (IOException e) {
            LOGGER.error("Error closing Elasticsearch client", e);
        }
    }

    private void flush() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }

        List<BulkOperation> operations = new ArrayList<>();

        for (Row row : buffer) {
            final ObjectNode document = rowToJsonNode(row);

            // Extract ID if id field is specified
            final String docId;
            if (!idFieldName.equals(ElasticsearchConstants.DEFAULT_ID_FIELD)) {
                JsonNode idNode = document.get(idFieldName);
                if (idNode != null) {
                    docId = idNode.asText();
                } else {
                    docId = null;
                }
            } else {
                docId = null;
            }

            IndexOperation<JsonNode> indexOp = IndexOperation.of(io -> {
                io.index(indexName).document(document);
                if (docId != null) {
                    io.id(docId);
                }
                return io;
            });

            operations.add(BulkOperation.of(bo -> bo.index(indexOp)));
        }

        BulkRequest bulkRequest = BulkRequest.of(br -> br.operations(operations));

        BulkResponse bulkResponse = client.bulk(bulkRequest);

        if (bulkResponse.errors()) {
            LOGGER.error("Bulk indexing had errors");
            bulkResponse.items().forEach(item -> {
                if (item.error() != null) {
                    LOGGER.error("Error indexing document {}: {}",
                            item.id(), item.error().reason());
                }
            });
            throw new GeaFlowDSLException("Bulk indexing failed");
        }

        LOGGER.info("Successfully indexed {} documents", buffer.size());
        buffer.clear();
    }

    private ObjectNode rowToJsonNode(Row row) {
        ObjectNode node = objectMapper.createObjectNode();

        List<TableField> fields = schema.getFields();

        for (int i = 0; i < fields.size(); i++) {
            TableField field = fields.get(i);
            Object value = row.getField(i, field.getType());

            if (value == null) {
                node.putNull(field.getName());
            } else {
                IType<?> type = field.getType();
                switch (type.getName()) {
                    case "INTEGER":
                        node.put(field.getName(), (Integer) value);
                        break;
                    case "LONG":
                    case "BIGINT":
                        node.put(field.getName(), (Long) value);
                        break;
                    case "DOUBLE":
                        node.put(field.getName(), (Double) value);
                        break;
                    case "FLOAT":
                        node.put(field.getName(), (Float) value);
                        break;
                    case "BOOLEAN":
                        node.put(field.getName(), (Boolean) value);
                        break;
                    case "STRING":
                    case "VARCHAR":
                        node.put(field.getName(), (String) value);
                        break;
                    default:
                        node.put(field.getName(), value.toString());
                }
            }
        }

        return node;
    }
}
