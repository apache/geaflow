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

package org.apache.geaflow.stats.model;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.METRIC_MAX_CACHED_PIPELINES;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.metric.CycleMetrics;
import org.apache.geaflow.common.metric.PipelineMetrics;

public class MetricCache implements Serializable {
    private static final int DEFAULT_MAX_JOBS = 50;

    private final int maxPipelines;
    private final BoundedHashMap<Long, String> submittedPipelines;
    private Map<String, PipelineMetricCache> pipelineMetricCacheMap;

    public MetricCache() {
        this(DEFAULT_MAX_JOBS);
    }

    public MetricCache(Configuration configuration) {
        this(configuration.getInteger(METRIC_MAX_CACHED_PIPELINES));
    }

    public MetricCache(int maxSize) {
        this.maxPipelines = maxSize;
        this.submittedPipelines = new BoundedHashMap<>(maxSize);
        this.pipelineMetricCacheMap = new ConcurrentHashMap<>();
    }

    public synchronized void addPipelineMetrics(PipelineMetrics pipelineMetrics) {
        submittedPipelines.put(pipelineMetrics.getStartTime(), pipelineMetrics.getName());
        PipelineMetricCache cache = pipelineMetricCacheMap.computeIfAbsent(pipelineMetrics.getName(),
            key -> new PipelineMetricCache());
        cache.updatePipelineMetrics(pipelineMetrics);
        if (pipelineMetricCacheMap.size() > maxPipelines) {
            pipelineMetricCacheMap.keySet().retainAll(submittedPipelines.values());
        }
    }

    public synchronized void addCycleMetrics(CycleMetrics cycleMetrics) {
        PipelineMetricCache cache = pipelineMetricCacheMap.computeIfAbsent(cycleMetrics.getPipelineName(),
            key -> new PipelineMetricCache());
        cache.addCycleMetrics(cycleMetrics);
    }

    public Map<String, PipelineMetricCache> getPipelineMetricCaches() {
        return pipelineMetricCacheMap;
    }

    private void readObject(ObjectInputStream inputStream) throws ClassNotFoundException,
        IOException {
        this.pipelineMetricCacheMap = (Map<String, PipelineMetricCache>) inputStream.readObject();
    }

    private void writeObject(ObjectOutputStream outputStream) throws IOException {
        outputStream.writeObject(pipelineMetricCacheMap);
    }

    public void mergeMetricCache(MetricCache metricCache) {
        this.pipelineMetricCacheMap.putAll(metricCache.pipelineMetricCacheMap);
    }

    public void clearAll() {
        this.pipelineMetricCacheMap.clear();
    }

    public static class PipelineMetricCache implements Serializable {
        private PipelineMetrics pipelineMetrics;
        private Map<String, CycleMetrics> cycleMetricMap;

        public PipelineMetricCache() {
            this.cycleMetricMap = new HashMap<>();
        }

        public void updatePipelineMetrics(PipelineMetrics pipelineMetrics) {
            this.pipelineMetrics = pipelineMetrics;
        }

        public void addCycleMetrics(CycleMetrics cycleMetrics) {
            this.cycleMetricMap.put(cycleMetrics.getName(), cycleMetrics);
        }

        public PipelineMetrics getPipelineMetrics() {
            return pipelineMetrics;
        }

        public void setPipelineMetrics(PipelineMetrics pipelineMetrics) {
            this.pipelineMetrics = pipelineMetrics;
        }

        public Map<String, CycleMetrics> getCycleMetricList() {
            return cycleMetricMap;
        }

        public void setCycleMetricList(Map<String, CycleMetrics> cycleMetricList) {
            this.cycleMetricMap = cycleMetricList;
        }
    }

    public static class BoundedHashMap<K, V> extends LinkedHashMap<K, V> {
        private final int maxSize;

        public BoundedHashMap(int capacity) {
            super(capacity, 0.75f, true);
            this.maxSize = capacity;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return this.size() > maxSize;
        }
    }

}
