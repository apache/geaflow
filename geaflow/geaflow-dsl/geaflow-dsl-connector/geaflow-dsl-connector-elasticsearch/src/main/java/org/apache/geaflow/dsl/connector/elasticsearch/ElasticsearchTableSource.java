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
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class ElasticsearchTableSource implements TableSource {

    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>(){}.getType();

    private StructType schema;
    private String hosts;
    private String indexName;
    private String username;
    private String password;
    private String scrollTimeout;
    private int connectionTimeout;
    private int socketTimeout;

    private RestHighLevelClient client;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema) {
        this.schema = tableSchema;
        this.hosts = tableConf.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS);
        this.indexName = tableConf.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX);
        this.username = tableConf.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_USERNAME, "");
        this.password = tableConf.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_PASSWORD, "");
        this.scrollTimeout = tableConf.getString(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_SCROLL_TIMEOUT,
                ElasticsearchConstants.DEFAULT_SCROLL_TIMEOUT);
        this.connectionTimeout = tableConf.getInteger(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_CONNECTION_TIMEOUT,
                ElasticsearchConstants.DEFAULT_CONNECTION_TIMEOUT);
        this.socketTimeout = tableConf.getInteger(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_SOCKET_TIMEOUT,
                ElasticsearchConstants.DEFAULT_SOCKET_TIMEOUT);
    }

    @Override
    public void open(RuntimeContext context) {
        try {
            this.client = createElasticsearchClient();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Elasticsearch client", e);
        }
    }

    @Override
    public List<Partition> listPartitions() {
        return java.util.Collections.singletonList(new ElasticsearchPartition(indexName));
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return new TableDeserializer<IN>() {
            @Override
            public void init(Configuration configuration, StructType structType) {
                // Initialization if needed
            }

            @Override
            public java.util.List<org.apache.geaflow.dsl.common.data.Row> deserialize(IN record) {
                if (record instanceof SearchHit) {
                    SearchHit hit = (SearchHit) record;
                    Map<String, Object> source = hit.getSourceAsMap();
                    if (source == null) {
                        source = GSON.fromJson(hit.getSourceAsString(), MAP_TYPE);
                    }

                    // Convert map to Row based on schema
                    Object[] values = new Object[schema.size()];
                    for (int i = 0; i < schema.size(); i++) {
                        String fieldName = schema.getFields().get(i).getName();
                        values[i] = source.get(fieldName);
                    }
                    org.apache.geaflow.dsl.common.data.Row row = 
                        org.apache.geaflow.dsl.common.data.impl.ObjectRow.create(values);
                    return java.util.Collections.singletonList(row);
                }
                return java.util.Collections.emptyList();
            }
        };
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<org.apache.geaflow.dsl.connector.api.Offset> startOffset,
                                  FetchWindow windowInfo) throws IOException {
        try {
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(1000); // Batch size

            searchRequest.source(searchSourceBuilder);

            // Use scroll for large dataset reading
            Scroll scroll = new Scroll(TimeValue.parseTimeValue(scrollTimeout, "scroll_timeout"));
            searchRequest.scroll(scroll);

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();

            List<T> dataList = new ArrayList<>();
            for (SearchHit hit : searchHits) {
                dataList.add((T) hit);
            }

            // Clear scroll
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);

            ElasticsearchOffset nextOffset = new ElasticsearchOffset(scrollId);
            return (FetchData<T>) FetchData.createStreamFetch(dataList, nextOffset, false);
        } catch (Exception e) {
            throw new IOException("Failed to fetch data from Elasticsearch", e);
        }
    }

    @Override
    public void close() {
        try {
            if (client != null) {
                client.close();
            }
        } catch (IOException e) {
            // Log error but don't throw exception in close method
            e.printStackTrace();
        }
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
            throw new RuntimeException("Failed to create Elasticsearch client", e);
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

    public static class ElasticsearchOffset implements org.apache.geaflow.dsl.connector.api.Offset {
        private final String scrollId;
        private final long timestamp;

        public ElasticsearchOffset(String scrollId) {
            this(scrollId, System.currentTimeMillis());
        }

        public ElasticsearchOffset(String scrollId, long timestamp) {
            this.scrollId = scrollId;
            this.timestamp = timestamp;
        }

        public String getScrollId() {
            return scrollId;
        }

        @Override
        public String humanReadable() {
            return "ElasticsearchOffset{scrollId='" + scrollId + "', timestamp=" + timestamp + "}";
        }

        @Override
        public long getOffset() {
            return timestamp;
        }

        @Override
        public boolean isTimestamp() {
            return true;
        }
    }
}
