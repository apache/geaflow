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

package org.apache.geaflow.metrics.reporter;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistryListener;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.utils.ProcessUtil;
import org.apache.geaflow.metrics.common.AggType;
import org.apache.geaflow.metrics.common.DownSample;
import org.apache.geaflow.metrics.common.HistAggType;
import org.apache.geaflow.metrics.common.MetricType;
import org.apache.geaflow.metrics.common.reporter.MetricReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractReporter implements MetricReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractReporter.class);

    public static final String TAG_JOB_NAME = "jobName";
    public static final String TAG_WORKER = "worker";
    public static final String TAG_ENGINE = "engine";

    public static final String KEY_TAGS = "tags";
    public static final String KEY_METRIC = "metric";
    public static final String KEY_AGGREGATOR = "aggregator";
    public static final String KEY_DOWN_SAMPLE = "downsample";
    public static final String KEY_GEAFLOW = "Geaflow";

    protected MetricRegistry metricRegistry;
    private MetricMetaClient metricMetaClient;
    protected Map<String, String> globalTags;
    protected String jobName;

    public void open(Configuration config, MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        this.jobName = config.getString(ExecutionConfigKeys.JOB_APP_NAME);
        this.globalTags = new HashMap<>();
        this.globalTags.put(TAG_JOB_NAME, jobName);
        this.globalTags.put(TAG_WORKER, ProcessUtil.getHostAndPid());
        this.globalTags.put(TAG_ENGINE, KEY_GEAFLOW);
    }

    protected void addMetricRegisterListener(Configuration config) {
        this.metricMetaClient = MetricMetaClient.getInstance(config);
        this.metricRegistry.addListener(new MetricRegisterListener(this.metricMetaClient));
        LOGGER.info("add metric register listener");
    }

    @Override
    public void close() {
        if (this.metricMetaClient != null) {
            this.metricMetaClient.close();
            LOGGER.info("close metric meta client");
        }
    }

    protected class MetricRegisterListener extends MetricRegistryListener.Base {

        private final MetricMetaClient metricMetaClient;

        public MetricRegisterListener(MetricMetaClient metricMetaClient) {
            this.metricMetaClient = metricMetaClient;
        }

        @Override
        public void onGaugeAdded(String metricName, Gauge<?> gauge) {
            String query = wrapQuery(metricName, DownSample.AVG, AggType.AVG);
            metricMetaClient.registerMetricMeta(metricName, MetricType.GAUGE, query);
        }

        @Override
        public void onCounterAdded(String metricName, Counter counter) {
            String query = wrapQuery(metricName, DownSample.SUM, AggType.SUM);
            metricMetaClient.registerMetricMeta(metricName, MetricType.COUNTER, query);
        }

        @Override
        public void onMeterAdded(String metricName, Meter meter) {
            String query = wrapQuery(metricName, DownSample.SUM, AggType.SUM);
            metricMetaClient.registerMetricMeta(metricName, MetricType.METER, query);
        }

        @Override
        public void onHistogramAdded(String metricName, Histogram histogram) {
            JSONObject queryTags = buildQueryTags();

            HistAggType aggType = HistAggType.DEFAULT;
            JSONArray histogramQueries = new JSONArray();
            for (String aggregator : aggType.getAggTypes()) {
                JSONObject query = new JSONObject();
                query.put(KEY_TAGS, queryTags);
                query.put(KEY_METRIC, metricName);
                query.put(KEY_AGGREGATOR, aggregator);
                query.put(KEY_DOWN_SAMPLE, DownSample.AVG.getValue());
                histogramQueries.add(query);
            }

            metricMetaClient.registerMetricMeta(metricName, MetricType.HISTOGRAM,
                JSON.toJSONString(histogramQueries, SerializerFeature.DisableCircularReferenceDetect));
        }

        private String wrapQuery(String metricName, DownSample downSample, AggType aggregator) {
            JSONObject query = new JSONObject();
            query.put(KEY_METRIC, metricName);
            query.put(KEY_AGGREGATOR, aggregator.getValue());
            query.put(KEY_DOWN_SAMPLE, downSample.getValue());

            JSONObject tags = buildQueryTags();
            query.put(KEY_TAGS, tags);

            JSONArray meterQueries = new JSONArray();
            meterQueries.add(query);
            return meterQueries.toJSONString();
        }
    }

    private JSONObject buildQueryTags() {
        JSONObject tags = new JSONObject();
        tags.put(TAG_JOB_NAME, this.jobName);
        return tags;
    }

}
