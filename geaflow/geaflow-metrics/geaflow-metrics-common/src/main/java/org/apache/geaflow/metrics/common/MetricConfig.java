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

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.METRIC_META_REPORT_DELAY;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.METRIC_META_REPORT_PERIOD;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.METRIC_META_REPORT_RETRIES;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.REPORTER_LIST;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SCHEDULE_PERIOD;

import java.io.Serializable;
import java.util.Random;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;

public class MetricConfig implements Serializable {

    private static final int WAIT_SECONDS = 5;
    private static final int RANDOM_SECONDS = 5;
    private static final int PERIOD_SECONDS = 5;

    private static final String REPORTER_SCHEDULE_PERIOD = "geaflow.metric.%s.schedule.period.sec";

    private final int schedulePeriod;
    private final Configuration config;
    private final Random random;

    public MetricConfig(Configuration configuration) {
        this.config = configuration;
        this.random = new Random();
        this.schedulePeriod = configuration.getInteger(SCHEDULE_PERIOD);
    }

    public String getReporterList() {
        boolean isLocal = config.getBoolean(ExecutionConfigKeys.RUN_LOCAL_MODE);
        if (isLocal) {
            return config.getString(REPORTER_LIST, "");
        } else {
            return config.getString(REPORTER_LIST);
        }
    }

    public int getSchedulePeriodSec(String reporterName) {
        String periodKey = String.format(REPORTER_SCHEDULE_PERIOD, reporterName);
        return config.getInteger(periodKey, schedulePeriod);
    }

    public int getRandomDelaySec() {
        int randomDelay = random.nextInt(RANDOM_SECONDS) + WAIT_SECONDS;
        return config.getInteger(METRIC_META_REPORT_DELAY, randomDelay);
    }

    public int getRandomPeriodSec() {
        int randomPeriod = random.nextInt(PERIOD_SECONDS) + 1;
        return config.getInteger(METRIC_META_REPORT_PERIOD, randomPeriod);
    }

    public int getReportMaxRetries() {
        return config.getInteger(METRIC_META_REPORT_RETRIES);
    }
}
