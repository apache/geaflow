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

package org.apache.geaflow.console.core.service.store.impl;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.QueryApi;
import com.influxdb.client.internal.InfluxDBClientImpl;
import com.influxdb.query.FluxTable;
import com.influxdb.query.dsl.Flux;
import com.influxdb.query.dsl.functions.restriction.Restrictions;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import org.apache.commons.collections.CollectionUtils;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.core.model.metric.GeaflowMetric;
import org.apache.geaflow.console.core.model.metric.GeaflowMetricQuery;
import org.apache.geaflow.console.core.model.metric.GeaflowMetricQueryRequest;
import org.apache.geaflow.console.core.model.plugin.config.InfluxdbPluginConfigClass;
import org.apache.geaflow.console.core.model.task.GeaflowTask;
import org.apache.geaflow.console.core.service.store.GeaflowMetricStore;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class InfluxdbStore implements GeaflowMetricStore {

    private static final Cache<String, InfluxDBClientImpl> INFLUXDB_CLIENT_CACHE =
        CacheBuilder.newBuilder().initialCapacity(10).maximumSize(100)
            .expireAfterWrite(120, TimeUnit.SECONDS)
            .removalListener(
                (RemovalListener<String, InfluxDBClientImpl>) removalNotification -> {
                    String key = removalNotification.getKey();
                    InfluxDBClientImpl client = removalNotification.getValue();
                    if (client != null) {
                        log.debug("influxdb {} close", key);
                        client.close();
                    }
                })
            .build();

    @Override
    public PageList<GeaflowMetric> queryMetrics(GeaflowTask task, GeaflowMetricQueryRequest queryRequest) {
        if (CollectionUtils.isEmpty(queryRequest.getQueries())) {
            return new PageList<>(new ArrayList<>());
        }

        InfluxdbPluginConfigClass influxdbConfig = task.getMetricPluginConfig().getConfig().parse(InfluxdbPluginConfigClass.class);
        Flux flux = buildQueryFlux(influxdbConfig.getBucket(), queryRequest);
        InfluxDBClientImpl client = getInfluxdbClient(influxdbConfig);
        QueryApi queryApi = client.getQueryApi();
        List<FluxTable> tables = queryApi.query(flux.toString());
        return new PageList<>(buildGeaflowMetrics(tables));
    }

    protected Flux buildQueryFlux(String bucketName, GeaflowMetricQueryRequest queryRequest) {
        long startMs = queryRequest.getStart();
        long endMs = queryRequest.getEnd();
        List<GeaflowMetricQuery> queries = queryRequest.getQueries();
        GeaflowMetricQuery metricQuery = queries.get(0);
        Restrictions restriction = Restrictions.and(
            Restrictions.tag("jobName").equal(metricQuery.getTags().get("jobName")),
            Restrictions.measurement().equal(metricQuery.getMetric())
        );
        Flux flux = Flux
            .from(bucketName)
            .range(startMs / 1000, endMs / 1000)
            .filter(restriction)
            .window(60L, ChronoUnit.SECONDS);
        String[] downsample = metricQuery.getDownsample().split("-");
        flux = setAggregator(flux, downsample[1]);
        return flux.duplicate("_stop", "_time");
    }

    protected Flux setAggregator(Flux flux, String aggregator) {
        switch (aggregator) {
            case "avg":
                return flux.mean();
            case "sum":
                return flux.sum();
            default:
                throw new GeaflowException("not supported aggregator: {}", aggregator);
        }
    }

    protected List<GeaflowMetric> buildGeaflowMetrics(List<FluxTable> tables) {
        List<GeaflowMetric> geaflowMetricList = new ArrayList<>();
        tables.forEach(fluxTable -> fluxTable.getRecords().forEach(fluxRecord -> {
            GeaflowMetric geaflowMetric = new GeaflowMetric();
            String lineName =
                Optional.ofNullable((String) fluxRecord.getValues().get("worker")).orElse(fluxRecord.getMeasurement());
            geaflowMetric.setMetric(lineName);
            geaflowMetric.setTime(fluxRecord.getTime().toEpochMilli());
            geaflowMetric.setValue(fluxRecord.getValue());
            geaflowMetricList.add(geaflowMetric);
        }));
        return geaflowMetricList;
    }

    protected synchronized InfluxDBClientImpl getInfluxdbClient(InfluxdbPluginConfigClass influxdbConfig) {
        String cacheKey = influxdbConfig.toString();
        InfluxDBClientImpl client = INFLUXDB_CLIENT_CACHE.getIfPresent(cacheKey);
        if (client == null) {
            client = buildInfluxDBClientImpl(influxdbConfig);
            INFLUXDB_CLIENT_CACHE.put(cacheKey, client);
        }
        return client;
    }

    private InfluxDBClientImpl buildInfluxDBClientImpl(InfluxdbPluginConfigClass influxdbConfig) {
        log.debug("build influxdb: {}", influxdbConfig.toString());
        Integer connectTimeout = Optional.ofNullable(influxdbConfig.getConnectTimeout()).orElse(30000);
        Integer writeTimeout = Optional.ofNullable(influxdbConfig.getWriteTimeout()).orElse(30000);
        OkHttpClient.Builder httpClient = new OkHttpClient.Builder()
            .connectTimeout(connectTimeout, TimeUnit.MILLISECONDS)
            .writeTimeout(writeTimeout, TimeUnit.MILLISECONDS);
        InfluxDBClientOptions options = InfluxDBClientOptions.builder()
            .okHttpClient(httpClient)
            .url(influxdbConfig.getUrl())
            .org(influxdbConfig.getOrg())
            .bucket(influxdbConfig.getBucket())
            .authenticateToken(influxdbConfig.getToken().toCharArray())
            .build();
        return new InfluxDBClientImpl(options);
    }
}
