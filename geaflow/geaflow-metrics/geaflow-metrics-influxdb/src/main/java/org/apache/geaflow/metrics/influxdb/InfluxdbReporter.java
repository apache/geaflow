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

package org.apache.geaflow.metrics.influxdb;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.internal.InfluxDBClientImpl;
import com.influxdb.client.write.Point;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.metrics.common.api.CounterImpl;
import org.apache.geaflow.metrics.common.api.MeterImpl;
import org.apache.geaflow.metrics.common.reporter.ScheduledReporter;
import org.apache.geaflow.metrics.reporter.AbstractReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxdbReporter extends AbstractReporter implements ScheduledReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxdbReporter.class);

    private static final Map<String, String> EMPTY_TAGS = Collections.emptyMap();
    private static final String TYPE_INFLUXDB = "influxdb";
    private static final String FIELD = "value";
    private InfluxDBClientImpl influxDB;

    @Override
    public void open(Configuration config, MetricRegistry metricRegistry) {
        super.open(config, metricRegistry);
        InfluxdbConfig influxdbConfig = new InfluxdbConfig(config);
        OkHttpClient.Builder httpClient = new OkHttpClient.Builder()
            .connectTimeout(influxdbConfig.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
            .writeTimeout(influxdbConfig.getWriteTimeoutMs(), TimeUnit.MILLISECONDS);
        InfluxDBClientOptions options = InfluxDBClientOptions.builder()
            .okHttpClient(httpClient)
            .url(influxdbConfig.getUrl())
            .org(influxdbConfig.getOrg())
            .bucket(influxdbConfig.getBucket())
            .authenticateToken(influxdbConfig.getToken().toCharArray())
            .build();
        this.influxDB = new InfluxDBClientImpl(options);
        this.addMetricRegisterListener(config);
        LOGGER.info("load influxdb config: {}", influxdbConfig);
    }

    @Override
    public void report() {
        List<Point> points = new ArrayList<>();
        for (Map.Entry<String, Gauge> gauge : metricRegistry.getGauges().entrySet()) {
            points.add(this.buildPoint(gauge.getKey(), gauge.getValue().getValue()));
        }
        for (Map.Entry<String, Meter> meter : metricRegistry.getMeters().entrySet()) {
            MeterImpl meterWrapper = (MeterImpl) meter.getValue();
            points.add(this.buildPoint(meter.getKey(), meterWrapper.getCountAndReset()));
        }
        for (Map.Entry<String, Counter> counter : metricRegistry.getCounters().entrySet()) {
            CounterImpl counterWrapper = (CounterImpl) counter.getValue();
            points.add(this.buildPoint(counter.getKey(), counterWrapper.getCountAndReset()));
        }
        for (Map.Entry<String, Histogram> histogram : metricRegistry.getHistograms().entrySet()) {
            points.add(this.buildPoint(histogram.getKey(), histogram.getValue().getSnapshot().getMean()));
        }
        try {
            this.writePoints(points);
        } catch (Exception e) {
            LOGGER.error("save metric to influxdb err", e);
        }
    }

    protected void writePoints(List<Point> points) {
        this.influxDB.getWriteApiBlocking().writePoints(points);
    }

    private Point buildPoint(String name, Object value) {
        if (value instanceof Number) {
            return Point.measurement(name)
                .addTags(this.globalTags)
                .time(System.currentTimeMillis(), WritePrecision.MS)
                .addField(FIELD, (Number) value);
        } else {
            return Point.measurement(name)
                .addTags(this.globalTags)
                .time(System.currentTimeMillis(), WritePrecision.MS)
                .addField(FIELD, String.valueOf(value));
        }
    }

    @Override
    public void close() {
        super.close();
        if (this.influxDB != null) {
            this.influxDB.close();
            LOGGER.info("close influxdb client");
        }
    }

    @Override
    public String getReporterType() {
        return TYPE_INFLUXDB;
    }

    @VisibleForTesting
    protected InfluxDBClientImpl getInfluxDB() {
        return this.influxDB;
    }

}
