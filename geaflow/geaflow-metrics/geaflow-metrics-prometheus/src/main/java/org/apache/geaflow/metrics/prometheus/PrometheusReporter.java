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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.exporter.BasicAuthHttpConnectionFactory;
import io.prometheus.client.exporter.PushGateway;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.utils.ProcessUtil;
import org.apache.geaflow.metrics.common.api.CounterImpl;
import org.apache.geaflow.metrics.common.api.MeterImpl;
import org.apache.geaflow.metrics.common.reporter.ScheduledReporter;
import org.apache.geaflow.metrics.reporter.AbstractReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrometheusReporter extends AbstractReporter implements ScheduledReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusReporter.class);

    private static final String TYPE_PROMETHEUS = "prometheus";

    private static final String INSTANCE_KEY = "instance";

    private static final String INVALID_METRIC_NAME_PATTERN = "[^a-zA-Z0-9_:]";

    private static final String METRIC_NAME_SEPARATOR = "_";

    private Configuration configuration;

    private PushGateway pushGateway;

    private String[] prometheusLabelNames;

    private String[] prometheusLabelValues;

    private final Map<String, io.prometheus.client.Gauge> gaugeMap = new HashMap<>();

    @Override
    public void open(Configuration config, MetricRegistry metricRegistry) {
        this.configuration = config;
        super.open(config, metricRegistry);
        this.initPushGateway(config);
        this.initLabels();
        this.addMetricRegisterListener(config);
    }

    @Override
    public void report() {
        for (Map.Entry<String, Counter> counter : metricRegistry.getCounters().entrySet()) {
            CounterImpl counterWrapper = (CounterImpl) counter.getValue();
            doReport(counter.getKey(), counterWrapper.getCountAndReset());
        }
        for (Map.Entry<String, Gauge> gaugeEntry : metricRegistry.getGauges().entrySet()) {
            doReport(gaugeEntry.getKey(), gaugeEntry.getValue().getValue());
        }
        for (Map.Entry<String, Meter> meter : metricRegistry.getMeters().entrySet()) {
            MeterImpl meterWrapper = (MeterImpl) meter.getValue();
            doReport(meter.getKey(), meterWrapper.getCountAndReset());
        }
        for (Map.Entry<String, Histogram> histogram : metricRegistry.getHistograms().entrySet()) {
            doReport(histogram.getKey(), histogram.getValue().getSnapshot().getMean());
        }
    }

    private void doReport(String name, Object value) {
        String metricName = getMetricName4Prometheus(name);
        if (value instanceof Number) {
            io.prometheus.client.Gauge gauge = getOrRegisterGauge(metricName);
            String jobName = configuration.getString(PrometheusConfigKeys.JOB_NAME);
            gauge.labels(prometheusLabelValues).set(((Number) value).doubleValue());
            try {
                pushGateway.pushAdd(gauge, jobName);
            } catch (IOException e) {
                LOGGER.error("push metric {} to gateway failed. {}", metricName, e.getMessage(), e);
            }
        }
    }

    private io.prometheus.client.Gauge getOrRegisterGauge(String metricName) {
        return gaugeMap.computeIfAbsent(metricName,
            name -> io.prometheus.client.Gauge.build().name(metricName).help(metricName)
                .labelNames(prometheusLabelNames).register());
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public String getReporterType() {
        return TYPE_PROMETHEUS;
    }

    private void initPushGateway(Configuration configuration) {
        String gatewayUrl = configuration.getString(PrometheusConfigKeys.GATEWAY_URL);
        try {
            pushGateway = new PushGateway(new URL(gatewayUrl));
            LOGGER.info("Load prometheus push gateway: {}", gatewayUrl);
            if (configuration.contains(PrometheusConfigKeys.USER) && configuration.contains(
                PrometheusConfigKeys.PASSWORD)) {
                pushGateway.setConnectionFactory(new BasicAuthHttpConnectionFactory(
                    configuration.getString(PrometheusConfigKeys.USER),
                    configuration.getString(PrometheusConfigKeys.PASSWORD)));
            }
        } catch (Exception e) {
            LOGGER.error("Load push gateway of url {} error. {}", gatewayUrl, e.getMessage(), e);
            throw new GeaflowRuntimeException(e);
        }
    }

    private void initLabels() {
        String instanceName = ProcessUtil.getHostAndPid();
        Map<String, String> prometheusLabels = new HashMap<>(globalTags);
        prometheusLabels.put(INSTANCE_KEY, instanceName);
        this.prometheusLabelNames = prometheusLabels.keySet().toArray(new String[0]);
        this.prometheusLabelValues = prometheusLabels.values().toArray(new String[0]);
    }

    private String getMetricName4Prometheus(String metricName) {
        metricName = metricName.replaceAll(INVALID_METRIC_NAME_PATTERN, METRIC_NAME_SEPARATOR);
        return metricName;
    }
}
