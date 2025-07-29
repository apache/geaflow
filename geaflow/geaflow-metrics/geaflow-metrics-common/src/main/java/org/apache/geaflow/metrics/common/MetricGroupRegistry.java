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

package org.apache.geaflow.metrics.common;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.utils.ThreadUtil;
import org.apache.geaflow.metrics.common.api.MetricGroup;
import org.apache.geaflow.metrics.common.api.MetricGroupImpl;
import org.apache.geaflow.metrics.common.reporter.MetricReporter;
import org.apache.geaflow.metrics.common.reporter.MetricReporterFactory;
import org.apache.geaflow.metrics.common.reporter.ScheduledReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricGroupRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricGroupRegistry.class);

    private static final String REPORTER_SEPARATOR = ",";
    private static MetricGroupRegistry INSTANCE;

    private final MetricRegistry metricRegistry;
    private final List<MetricReporter> reporterList;
    private final ScheduledExecutorService executor;
    private final Map<String, MetricGroup> metricGroupMap;

    private MetricGroupRegistry() {
        this.metricRegistry = new MetricRegistry();
        this.reporterList = new ArrayList<>();
        this.executor = new ScheduledThreadPoolExecutor(1,
            ThreadUtil.namedThreadFactory(true, "metricService"));
        this.metricGroupMap = new ConcurrentHashMap<>();
    }

    public static synchronized MetricGroupRegistry getInstance(Configuration config) {
        if (INSTANCE == null) {
            INSTANCE = new MetricGroupRegistry();
            INSTANCE.open(config);
        }
        return INSTANCE;
    }

    public static MetricGroupRegistry getInstance() {
        return INSTANCE;
    }

    public MetricGroup getMetricGroup() {
        return metricGroupMap.computeIfAbsent(MetricConstants.MODULE_DEFAULT,
            name -> new MetricGroupImpl(metricRegistry));
    }

    public MetricGroup getMetricGroup(String groupName) {
        return metricGroupMap.computeIfAbsent(groupName, name -> new MetricGroupImpl(name,
            metricRegistry));
    }

    @VisibleForTesting
    public List<MetricReporter> getReporterList() {
        return reporterList;
    }

    private void open(Configuration config) {
        MetricConfig metricConfig = new MetricConfig(config);

        String reporterList = metricConfig.getReporterList();
        if (StringUtils.isEmpty(reporterList)) {
            LOGGER.warn("report list is empty");
            return;
        }
        String[] reporters = reporterList.split(REPORTER_SEPARATOR);
        try {
            for (String reporter : reporters) {
                MetricReporter metricReporter = MetricReporterFactory.getMetricReporter(reporter.toLowerCase());
                metricReporter.open(config, metricRegistry);
                if (metricReporter instanceof ScheduledReporter) {
                    int period = metricConfig.getSchedulePeriodSec(reporter);
                    executor.scheduleWithFixedDelay(new ReporterTask(
                        (ScheduledReporter) metricReporter), period, period, TimeUnit.SECONDS);
                    LOGGER.info("schedule {} with duration {}s", reporter, period);
                }
                this.reporterList.add(metricReporter);
            }
        } catch (Exception e) {
            LOGGER.error("failed to initialized metricReporters", e);
            throw new GeaflowRuntimeException(e);
        }
    }

    public void close() {
        LOGGER.info("close metric service");
        for (MetricReporter metricReporter : reporterList) {
            metricReporter.close();
        }
    }

    private static final class ReporterTask extends TimerTask {

        private final ScheduledReporter reporter;

        private ReporterTask(ScheduledReporter reporter) {
            this.reporter = reporter;
        }

        @Override
        public void run() {
            try {
                reporter.report();
            } catch (Throwable t) {
                LOGGER.warn("Error while reporting metrics", t);
            }
        }
    }
}
