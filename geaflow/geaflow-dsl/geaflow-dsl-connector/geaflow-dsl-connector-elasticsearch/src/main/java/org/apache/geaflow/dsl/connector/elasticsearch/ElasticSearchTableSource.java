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
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.AbstractTableSource;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.serde.DeserializerFactory;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.SizeFetchWindow;
import org.apache.geaflow.dsl.connector.api.window.TimeFetchWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.dsl.connector.elasticsearch.utils.ElasticSearchConnectorUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ElasticSearchTableSource extends AbstractTableSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchTableSource.class);

    private String hosts;
    private String indexName;
    private String timestampField;
    private int maxFetchSize;
    private int connectTimeout;
    private int socketTimeout;
    private int requestTimeout;
    private String username;
    private String password;



    private transient RestHighLevelClient esClient;

    @Override
    public void init(Configuration conf, TableSchema schema) {
        this.hosts = conf.getString(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS);
        this.indexName = conf.getString(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX);
        this.timestampField = conf.getString(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_TIMESTAMP_FIELD);
        this.maxFetchSize = conf.getInteger(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_MAX_FETCH_SIZE);

        this.connectTimeout = conf.getInteger(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_CONNECT_TIMEOUT);
        this.socketTimeout = conf.getInteger(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_SOCKET_TIMEOUT);
        this.requestTimeout = conf.getInteger(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_REQUEST_TIMEOUT);

        this.username = conf.getString(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_USERNAME, "");
        this.password = conf.getString(ElasticSearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_PASSWORD, "");

    }


    @Override
    public void open(RuntimeContext context) {
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
    }

    @Override
    public List<Partition> listPartitions() {
        List<Partition> partitions = new ArrayList<>();

        try {
            GetSettingsRequest request = new GetSettingsRequest();
            request.indices(indexName);

            GetSettingsResponse response = esClient.indices().getSettings(request, RequestOptions.DEFAULT);

            int shardCount = Integer.parseInt(response.getIndexToSettings()
                    .get(indexName)
                    .get("index.number_of_shards"));
            for (int shardId = 0; shardId < shardCount; shardId++) {
                partitions.add(new ElasticSearchPartition(indexName, shardId));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch Elasticsearch partitions for index: " + indexName, e);
        }

        return partitions;
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return DeserializerFactory.loadDeserializer(conf);
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset, TimeFetchWindow window) throws IOException {
        ElasticSearchPartition esPartition = (ElasticSearchPartition) partition;
        long windowStartTime = window.getStartWindowTime(0);
        long windowEndTime = window.getEndWindowTime(0);

        // build range query
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery(timestampField)
                .gte(windowStartTime)
                .lt(windowEndTime);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(rangeQuery)
                .size(maxFetchSize)
                .sort(timestampField, SortOrder.ASC)
                .sort("_shard_doc", SortOrder.ASC);

        if (startOffset.isPresent()) {
            ElasticSearchOffset offset = (ElasticSearchOffset) startOffset.get();
            sourceBuilder.searchAfter(offset.getSortValues());
        }

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(sourceBuilder);
        if(esPartition.getShardId() >= 0){
            searchRequest.preference("_shards:" + esPartition.getShardId());
        }

        SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();

        ProcessedHits processedHits = processSearchHits(hits, startOffset, maxFetchSize);

        return (FetchData<T>) FetchData.createStreamFetch(processedHits.records, processedHits.nextOffset, processedHits.hasMore);
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset, SizeFetchWindow window) throws IOException {
        ElasticSearchPartition esPartition = (ElasticSearchPartition) partition;
        long windowSize = Math.min(window.windowSize(), maxFetchSize);
        if (window.windowSize() > maxFetchSize) {
            LOGGER.warn("windowSize {} exceeds maxFetchSize {}, using {}",
                    window.windowSize(), maxFetchSize, maxFetchSize);
        }

        List<String> records = new ArrayList<>();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .size((int) windowSize)
                .sort(timestampField, SortOrder.ASC)
                .sort("_shard_doc", SortOrder.ASC);

        if (startOffset.isPresent()) {
            ElasticSearchOffset esOffset = (ElasticSearchOffset) startOffset.get();
            sourceBuilder.searchAfter(esOffset.getSortValues());
        }

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(sourceBuilder);
        if(esPartition.getShardId() >= 0){
            searchRequest.preference("_shards:" + esPartition.getShardId());
        }

        SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        ProcessedHits processedHits = processSearchHits(hits, startOffset, (int) windowSize);

        return (FetchData<T>) FetchData.createStreamFetch(processedHits.records, processedHits.nextOffset, processedHits.hasMore);
    }

    private static class ProcessedHits {
        List<String> records = new ArrayList<>();
        ElasticSearchOffset nextOffset;
        boolean hasMore;
    }

    // process search results and nextOffset
    private ProcessedHits processSearchHits(SearchHit[] hits, Optional<Offset> startOffset, int requestedSize) {
        ProcessedHits result = new ProcessedHits();

        for (SearchHit hit : hits) {
            result.records.add(hit.getSourceAsString());
        }

        ElasticSearchOffset nextOffset = null;
        boolean hasMore = false;
        if (hits.length > 0) {
            nextOffset = new ElasticSearchOffset(hits[hits.length - 1].getSortValues());
            hasMore = hits.length == requestedSize;
        } else {
            if (startOffset.isPresent()){
                nextOffset = new ElasticSearchOffset(((ElasticSearchOffset) startOffset.get()).getSortValues());
            }
        }
        result.hasMore = hasMore;
        result.nextOffset = nextOffset;
        return result;
    }


    @Override
    public void close() {
        try {
            if(esClient != null){
                esClient.close();
            }
        } catch (IOException e) {
            throw new GeaFlowDSLException("Failed to close Elasticsearch client", e);
        }
    }

    public static class ElasticSearchPartition implements Partition {
        private final String index;
        private final int shardId;

        public ElasticSearchPartition(String index, int shardId) {
            this.index = index;
            this.shardId = shardId;
        }

        @Override
        public String getName() {
            return index + "-" + shardId;
        }

        public String getIndex() {
            return index;
        }

        public int getShardId() {
            return shardId;
        }
    }


    public static class ElasticSearchOffset implements Offset {

        private final Object[] sortValues;
        private final String sortValuesStr;  // for serialization

        public ElasticSearchOffset(Object[] sortValues) {
            this.sortValues = sortValues;
            this.sortValuesStr = Arrays.stream(sortValues)
                    .map(Objects::toString)
                    .collect(Collectors.joining(","));
        }
        public Object[] getSortValues() {
            return sortValues;
        }
        @Override
        public long getOffset() {
            if (sortValues != null && sortValues.length > 0 && sortValues[0] instanceof Number) {
                return ((Number) sortValues[0]).longValue();
            }
            return 0L;
        }
        @Override
        public boolean isTimestamp() {
            return true;
        }

        @Override
        public String humanReadable() {
            if (sortValues == null || sortValues.length == 0) {
                return "EmptyOffset";
            }
            StringBuilder sb = new StringBuilder("[");
            for (Object value : sortValues) {
                if (sb.length() > 1) sb.append(",");
                sb.append(value);
            }
            sb.append("]");
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ElasticSearchOffset that = (ElasticSearchOffset) o;
            return Arrays.equals(sortValues, that.sortValues);
        }
        @Override
        public int hashCode() {
            return Arrays.hashCode(sortValues);
        }
    }
}