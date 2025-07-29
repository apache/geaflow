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
import org.apache.geaflow.stats.model.MetricCache;
import org.apache.geaflow.stats.sink.IStatsWriter;
import org.apache.geaflow.stats.sink.StatsWriterFactory;

public class StatsCollectorFactory {

    private final ExceptionCollector exceptionCollector;
    private final EventCollector eventCollector;
    private final PipelineStatsCollector pipelineStatsCollector;
    private final ProcessStatsCollector processStatsCollector;
    private final MetricMetaCollector metricMetaCollector;
    private final HeartbeatCollector heartbeatCollector;
    private final MetricCache metricCache;
    private final IStatsWriter syncWriter;
    private static StatsCollectorFactory INSTANCE;

    private StatsCollectorFactory(Configuration configuration) {
        this.syncWriter = StatsWriterFactory.getStatsWriter(configuration, true);
        this.exceptionCollector = new ExceptionCollector(syncWriter, configuration);
        this.eventCollector = new EventCollector(syncWriter, configuration);
        this.metricCache = new MetricCache(configuration);
        IStatsWriter statsWriter = StatsWriterFactory.getStatsWriter(configuration);
        this.pipelineStatsCollector = new PipelineStatsCollector(statsWriter, configuration, metricCache);
        this.metricMetaCollector = new MetricMetaCollector(statsWriter, configuration);
        this.processStatsCollector = new ProcessStatsCollector(configuration);
        this.heartbeatCollector = new HeartbeatCollector(statsWriter, configuration);
    }

    public static synchronized StatsCollectorFactory init(Configuration configuration) {
        if (INSTANCE == null) {
            INSTANCE = new StatsCollectorFactory(configuration);
        }
        return INSTANCE;
    }

    public static StatsCollectorFactory getInstance() {
        return INSTANCE;
    }

    public ExceptionCollector getExceptionCollector() {
        return exceptionCollector;
    }

    public EventCollector getEventCollector() {
        return eventCollector;
    }

    public PipelineStatsCollector getPipelineStatsCollector() {
        return pipelineStatsCollector;
    }

    public HeartbeatCollector getHeartbeatCollector() {
        return heartbeatCollector;
    }

    public MetricMetaCollector getMetricMetaCollector() {
        return metricMetaCollector;
    }

    public ProcessStatsCollector getProcessStatsCollector() {
        return processStatsCollector;
    }

    public MetricCache getMetricCache() {
        return metricCache;
    }

    public IStatsWriter getStatsWriter() {
        return syncWriter;
    }

}
