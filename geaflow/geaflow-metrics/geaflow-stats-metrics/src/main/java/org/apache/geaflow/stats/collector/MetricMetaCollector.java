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

package org.apache.geaflow.stats.collector;

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.metrics.common.MetricMeta;
import org.apache.geaflow.stats.model.StatsMetricType;
import org.apache.geaflow.stats.sink.IStatsWriter;

public class MetricMetaCollector extends BaseStatsCollector {

    private static final String KEY_SPLIT = "_";

    private static final String META_KEY = "META";

    MetricMetaCollector(IStatsWriter statsWriter, Configuration configuration) {
        super(statsWriter, configuration);
    }

    public void reportMetricMeta(MetricMeta metricMeta) {
        addToWriterQueue(getMetricMetaKey(metricMeta.getMetricName()), metricMeta);
    }

    private String getMetricMetaKey(String metricName) {
        return jobName + StatsMetricType.Metrics.getValue() + META_KEY + KEY_SPLIT + metricName;
    }

}
