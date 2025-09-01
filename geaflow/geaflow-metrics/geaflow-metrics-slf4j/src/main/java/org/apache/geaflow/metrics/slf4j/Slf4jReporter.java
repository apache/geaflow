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

package org.apache.geaflow.metrics.slf4j;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.metrics.common.reporter.ScheduledReporter;
import org.apache.geaflow.metrics.reporter.AbstractReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Slf4jReporter extends AbstractReporter implements ScheduledReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(Slf4jReporter.class);

    private static final String TYPE_SLF4J = "slf4j";
    private static final String LINE_SEPARATOR = System.lineSeparator();

    @Override
    public void open(Configuration jobConfig, MetricRegistry metricRegistry) {
        super.open(jobConfig, metricRegistry);
    }

    @Override
    public void report() {
        StringBuilder builder = new StringBuilder();
        builder
            .append(LINE_SEPARATOR)
            .append("=========================== Starting metrics report ===========================")
            .append(LINE_SEPARATOR);

        builder
            .append(LINE_SEPARATOR)
            .append("-- Counters -------------------------------------------------------------------")
            .append(LINE_SEPARATOR);
        for (Map.Entry<String, Counter> metric : metricRegistry.getCounters().entrySet()) {
            builder
                .append(metric.getKey()).append(": ").append(metric.getValue().getCount())
                .append(LINE_SEPARATOR);
        }

        builder
            .append(LINE_SEPARATOR)
            .append("-- Gauges ---------------------------------------------------------------------")
            .append(LINE_SEPARATOR);
        for (Map.Entry<String, Gauge> metric : metricRegistry.getGauges().entrySet()) {
            builder
                .append(metric.getKey()).append(": ").append(metric.getValue().getValue())
                .append(LINE_SEPARATOR);
        }

        builder
            .append(LINE_SEPARATOR)
            .append("-- Meters ---------------------------------------------------------------------")
            .append(LINE_SEPARATOR);
        for (Map.Entry<String, Meter> metric : metricRegistry.getMeters().entrySet()) {
            Meter meter = metric.getValue();
            builder
                .append(metric.getKey()).append(": ")
                .append(meter.getMeanRate())
                .append(", 1mRate=").append(meter.getOneMinuteRate())
                .append(", 5mRate=").append(meter.getFiveMinuteRate())
                .append(", 15mRate=").append(meter.getFifteenMinuteRate())
                .append(LINE_SEPARATOR);
        }

        builder
            .append(LINE_SEPARATOR)
            .append("-- Histograms -----------------------------------------------------------------")
            .append(LINE_SEPARATOR);
        for (Map.Entry<String, Histogram> metric : metricRegistry.getHistograms().entrySet()) {
            Snapshot stats = metric.getValue().getSnapshot();
            builder
                .append(metric.getValue()).append(": count=").append(stats.size())
                .append(", min=").append(stats.getMin())
                .append(", max=").append(stats.getMax())
                .append(", mean=").append(stats.getMean())
                .append(", stddev=").append(stats.getStdDev())
                .append(", p75=").append(stats.get75thPercentile())
                .append(", p95=").append(stats.get95thPercentile())
                .append(", p98=").append(stats.get98thPercentile())
                .append(", p99=").append(stats.get99thPercentile())
                .append(", p999=").append(stats.get999thPercentile())
                .append(LINE_SEPARATOR);
        }

        builder
            .append(LINE_SEPARATOR)
            .append("=========================== Finished metrics report ===========================")
            .append(LINE_SEPARATOR);
        LOGGER.info(builder.toString());
    }

    @Override
    public void close() {
    }

    @Override
    public String getReporterType() {
        return TYPE_SLF4J;
    }

}
