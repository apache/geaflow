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
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.ClearScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchTableSource implements TableSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchTableSource.class);

    private Configuration config;
    private TableSchema tableSchema;
    private ElasticsearchClient client;
    private RestClient restClient;

    private String indexName;
    private String scrollTimeout;
    private int scrollSize;
    private String queryDsl;
    private ObjectMapper objectMapper;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema) {
        this.config = tableConf;
        this.tableSchema = tableSchema;

        this.indexName = config.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX);
        if (indexName == null || indexName.isEmpty()) {
            throw new GeaFlowDSLException("Elasticsearch index name is required");
        }

        this.scrollTimeout = config.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_SCROLL_TIMEOUT);
        this.scrollSize = Integer.parseInt(
                config.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_SCROLL_SIZE));
        this.queryDsl = config.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_QUERY);

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

        LOGGER.info("Elasticsearch source opened for index: {}", indexName);
    }

    @Override
    public List<Partition> listPartitions() {
        // Elasticsearch doesn't have explicit partitions like databases,
        // so we return a single partition representing the entire index
        return Collections.singletonList(new ElasticsearchPartition(indexName));
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        throw new UnsupportedOperationException("Elasticsearch uses internal deserialization");
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset,
                                  FetchWindow windowInfo) throws IOException {
        List<Row> rows = new ArrayList<>();
        String scrollId = null;
        boolean isFinished = false;

        try {
            if (startOffset.isPresent()) {
                // Continue scrolling with existing scroll ID
                ElasticsearchOffset esOffset = (ElasticsearchOffset) startOffset.get();
                final String existingScrollId = esOffset.getScrollId();

                ScrollRequest scrollRequest = ScrollRequest.of(sr -> sr
                        .scrollId(existingScrollId)
                        .scroll(Time.of(t -> t.time(scrollTimeout)))
                );

                ScrollResponse<JsonNode> scrollResponse = client.scroll(scrollRequest, JsonNode.class);

                for (Hit<JsonNode> hit : scrollResponse.hits().hits()) {
                    rows.add(jsonNodeToRow(hit.source()));
                }

                scrollId = scrollResponse.scrollId();
                isFinished = scrollResponse.hits().hits().isEmpty();

            } else {
                // Initial search with scroll
                final String index = indexName;
                final int size = scrollSize;
                final String timeout = scrollTimeout;
                final String query = queryDsl;
                
                SearchRequest searchRequest = SearchRequest.of(sr -> sr
                        .index(index)
                        .scroll(Time.of(t -> t.time(timeout)))
                        .size(size)
                        .query(q -> q.queryString(qs -> qs.query(query)))
                );

                SearchResponse<JsonNode> searchResponse = client.search(searchRequest, JsonNode.class);

                for (Hit<JsonNode> hit : searchResponse.hits().hits()) {
                    rows.add(jsonNodeToRow(hit.source()));
                }

                scrollId = searchResponse.scrollId();
                isFinished = searchResponse.hits().hits().isEmpty();
            }

            ElasticsearchOffset nextOffset = new ElasticsearchOffset(scrollId);
            return (FetchData<T>) FetchData.createStreamFetch(rows, nextOffset, isFinished);

        } catch (IOException e) {
            // Clear scroll on error
            if (scrollId != null) {
                try {
                    clearScroll(scrollId);
                } catch (IOException clearEx) {
                    LOGGER.warn("Failed to clear scroll context", clearEx);
                }
            }
            throw e;
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

    private void clearScroll(String scrollId) throws IOException {
        ClearScrollRequest clearScrollRequest = ClearScrollRequest.of(csr -> csr
                .scrollId(scrollId)
        );
        client.clearScroll(clearScrollRequest);
    }

    private Row jsonNodeToRow(JsonNode jsonNode) {
        List<TableField> fields = tableSchema.getFields();
        Object[] values = new Object[fields.size()];

        for (int i = 0; i < fields.size(); i++) {
            TableField field = fields.get(i);
            JsonNode fieldNode = jsonNode.get(field.getName());

            if (fieldNode == null || fieldNode.isNull()) {
                values[i] = null;
            } else {
                IType<?> type = field.getType();
                values[i] = convertJsonValue(fieldNode, type);
            }
        }

        return ObjectRow.create(values);
    }

    private Object convertJsonValue(JsonNode node, IType<?> type) {
        switch (type.getName()) {
            case "INTEGER":
                return node.asInt();
            case "LONG":
            case "BIGINT":
                return node.asLong();
            case "DOUBLE":
                return node.asDouble();
            case "FLOAT":
                return (float) node.asDouble();
            case "BOOLEAN":
                return node.asBoolean();
            case "STRING":
            case "VARCHAR":
                return node.asText();
            default:
                return node.asText();
        }
    }

    public static class ElasticsearchPartition implements Partition {
        private final String indexName;

        public ElasticsearchPartition(String indexName) {
            this.indexName = indexName;
        }

        @Override
        public String getName() {
            return indexName;
        }
    }

    public static class ElasticsearchOffset implements Offset {
        private final String scrollId;

        public ElasticsearchOffset(String scrollId) {
            this.scrollId = scrollId;
        }

        public String getScrollId() {
            return scrollId;
        }

        @Override
        public String humanReadable() {
            return "ScrollId: " + scrollId;
        }

        @Override
        public long getOffset() {
            return 0;
        }

        @Override
        public boolean isTimestamp() {
            return false;
        }
    }
}
