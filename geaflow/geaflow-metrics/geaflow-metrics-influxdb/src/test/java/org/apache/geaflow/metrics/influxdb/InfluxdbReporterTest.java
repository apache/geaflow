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

import com.codahale.metrics.MetricRegistry;
import com.influxdb.client.write.Point;
import java.util.List;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.metrics.common.api.Counter;
import org.apache.geaflow.metrics.common.api.Gauge;
import org.apache.geaflow.metrics.common.api.Histogram;
import org.apache.geaflow.metrics.common.api.Meter;
import org.apache.geaflow.metrics.common.api.MetricGroupImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

public class InfluxdbReporterTest {

    private static final String url = "http://localhost:8086";
    private static final String org = "geaflow";
    private static final String token = "test";
    private static final String bucket = "test-bucket";
    private static final Configuration config = new Configuration();

    static {
        config.put(InfluxdbConfigKeys.URL, url);
        config.put(InfluxdbConfigKeys.ORG, org);
        config.put(InfluxdbConfigKeys.TOKEN, token);
        config.put(InfluxdbConfigKeys.BUCKET, bucket);
    }

    @Test
    public void testInfluxdbConfig() {
        InfluxdbConfig influxdbConfig = new InfluxdbConfig(config);
        Assert.assertEquals(influxdbConfig.getUrl(), url);
        Assert.assertEquals(influxdbConfig.getOrg(), org);
        Assert.assertEquals(influxdbConfig.getToken(), token);
        Assert.assertEquals(influxdbConfig.getBucket(), bucket);
        Assert.assertEquals(
            influxdbConfig.getConnectTimeoutMs(),
            InfluxdbConfigKeys.CONNECT_TIMEOUT_MS.getDefaultValue()
        );
        Assert.assertEquals(
            influxdbConfig.getWriteTimeoutMs(),
            InfluxdbConfigKeys.WRITE_TIMEOUT_MS.getDefaultValue()
        );
    }

    @Test
    public void testInfluxdbReporter() {
        MetricRegistry metricRegistry = new MetricRegistry();
        MetricGroupImpl metricGroup = new MetricGroupImpl(metricRegistry);
        Gauge gauge = metricGroup.gauge("test-gauge");
        gauge.setValue(1.0);
        Counter counter = metricGroup.counter("test-counter");
        counter.inc();
        Meter meter = metricGroup.meter("test-meter");
        meter.mark();
        Histogram histogram = metricGroup.histogram("test-histogram");
        histogram.update(123);

        InfluxdbReporter reporter = new MockInfluxdbReporter();
        reporter.open(config, metricRegistry);
        reporter.report();
        reporter.close();
        Assert.assertNotNull(reporter.getInfluxDB());
    }

    private static class MockInfluxdbReporter extends InfluxdbReporter {

        @Override
        protected void writePoints(List<Point> points) {
        }

        @Override
        protected void addMetricRegisterListener(Configuration config) {
        }

    }

}
