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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import org.apache.http.HttpHost;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.geaflow.dsl.connector.elasticsearch.ElasticsearchConstants.DEFAULT_SEARCH_SIZE;
import static org.apache.geaflow.dsl.connector.elasticsearch.ElasticsearchConstants.ES_HTTPS_SCHEME;
import static org.apache.geaflow.dsl.connector.elasticsearch.ElasticsearchConstants.ES_HTTP_SCHEME;
import static org.apache.geaflow.dsl.connector.elasticsearch.ElasticsearchConstants.ES_SCHEMA_SUFFIX;
import static org.apache.geaflow.dsl.connector.elasticsearch.ElasticsearchConstants.ES_SPLIT_COLON;
import static org.apache.geaflow.dsl.connector.elasticsearch.ElasticsearchConstants.ES_SPLIT_COMMA;

public class ElasticsearchTableSource implements TableSource {

    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>(){}.getType();

    private Logger logger = LoggerFactory.getLogger(ElasticsearchTableSource.class);

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
            throw new GeaFlowDSLException("Failed to initialize Elasticsearch client", e);
        }
    }

    @Override
    public List<Partition> listPartitions() {
        return Collections.singletonList(new ElasticsearchPartition(indexName));
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return new TableDeserializer<IN>() {
            @Override
            public void init(Configuration configuration, StructType structType) {
                // Initialization if needed
            }

            @Override
            public List<Row> deserialize(IN record) {
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
                    Row row = ObjectRow.create(values);
                    return Collections.singletonList(row);
                }
                return Collections.emptyList();
            }
        };
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset,
                                  FetchWindow windowInfo) throws IOException {
        try {
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(DEFAULT_SEARCH_SIZE); // Batch size

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
            logger.warn("Failed to close Elasticsearch client", e);
        }
    }

    private RestHighLevelClient createElasticsearchClient() {
        try {
            String[] hostArray = hosts.split(ES_SPLIT_COMMA);
            HttpHost[] httpHosts = new HttpHost[hostArray.length];

            for (int i = 0; i < hostArray.length; i++) {
                String host = hostArray[i].trim();
                if (host.startsWith(ES_HTTP_SCHEME + ES_SCHEMA_SUFFIX)) {
                    host = host.substring(7);
                } else if (host.startsWith(ES_HTTPS_SCHEME + ES_SCHEMA_SUFFIX)) {
                    host = host.substring(8);
                }

                String[] parts = host.split(ES_SPLIT_COLON);
                String hostname = parts[0];
                int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9200;
                httpHosts[i] = new HttpHost(hostname, port, ES_HTTP_SCHEME);
            }

            RestClientBuilder builder = RestClient.builder(httpHosts);

            // Configure timeouts
            builder.setRequestConfigCallback(requestConfigBuilder -> {
                requestConfigBuilder.setConnectTimeout(connectionTimeout);
                requestConfigBuilder.setSocketTimeout(socketTimeout);
                return requestConfigBuilder;
            });

            return new RestHighLevelClient(builder);
        } catch (Exception e) {
            throw new GeaFlowDSLException("Failed to create Elasticsearch client", e);
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
