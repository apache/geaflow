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
import org.apache.geaflow.common.metric.CycleMetrics;
import org.apache.geaflow.common.metric.PipelineMetrics;
import org.apache.geaflow.stats.model.MetricCache;
import org.apache.geaflow.stats.model.StatsMetricType;
import org.apache.geaflow.stats.sink.IStatsWriter;

public class PipelineStatsCollector extends BaseStatsCollector {

    private static final String KEY_SPLIT = "_";
    private final MetricCache metricCache;

    PipelineStatsCollector(IStatsWriter statsWriter, Configuration configuration,
                           MetricCache metricCache) {
        super(statsWriter, configuration);
        this.metricCache = metricCache;
    }

    public void reportPipelineMetrics(PipelineMetrics pipelineMetric) {
        addToWriterQueue(genMetricKey(PipelineMetricsType.PIPELINE, pipelineMetric.getName()), pipelineMetric);
        metricCache.addPipelineMetrics(pipelineMetric);
    }

    public void reportCycleMetrics(CycleMetrics cycleMetrics) {
        String name = cycleMetrics.getPipelineName() + KEY_SPLIT + cycleMetrics.getName();
        addToWriterQueue(genMetricKey(PipelineMetricsType.CYCLE, name), cycleMetrics);
        metricCache.addCycleMetrics(cycleMetrics);
    }

    private String genMetricKey(PipelineMetricsType type, String name) {
        return jobName + StatsMetricType.Metrics.getValue() + type + KEY_SPLIT + name;
    }

    private enum PipelineMetricsType {
        PIPELINE,
        CYCLE
    }
}
