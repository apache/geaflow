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

package org.apache.geaflow.metrics.prometheus;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.GEAFLOW_GW_ENDPOINT;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.JOB_APP_NAME;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.METRIC_META_REPORT_DELAY;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.METRIC_META_REPORT_PERIOD;

import com.codahale.metrics.MetricRegistry;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.SleepUtils;
import org.apache.geaflow.metrics.common.api.Counter;
import org.apache.geaflow.metrics.common.api.Gauge;
import org.apache.geaflow.metrics.common.api.Histogram;
import org.apache.geaflow.metrics.common.api.MetricGroup;
import org.apache.geaflow.metrics.common.api.MetricGroupImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PrometheusReporterTest {

    @Test
    public void test() {
        MetricRegistry metricRegistry = new MetricRegistry();
        MetricGroup metricGroup = new MetricGroupImpl(metricRegistry);
        Counter counter = metricGroup.counter("system/iteration.MetricNameFormatterTest[1:0](ms)");
        counter.inc();

        Gauge gauge = metricGroup.gauge("gaugeTest");
        gauge.setValue(10);

        Histogram histogram = metricGroup.histogram("histTest");
        histogram.update(1);

        Configuration config = new Configuration();
        config.put(JOB_APP_NAME, "geaflow123");
        config.put(GEAFLOW_GW_ENDPOINT, "http://localhost:8888/");
        config.put(METRIC_META_REPORT_DELAY, "0");
        config.put(METRIC_META_REPORT_PERIOD, "1");
        config.put(PrometheusConfigKeys.GATEWAY_URL.getKey(), "http://localhost:9091");
        PrometheusReporter metricReporter = new PrometheusReporter();
        metricReporter.open(config, metricRegistry);
        metricReporter.report();
        SleepUtils.sleepSecond(3);

        metricReporter.close();
        Assert.assertTrue(true);
    }

}
