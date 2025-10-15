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

import org.apache.geaflow.common.metric.PipelineMetrics;
import org.apache.geaflow.stats.model.MetricCache.BoundedHashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MetricCacheTest {

    @Test
    public void testBoundedHashMap() {
        BoundedHashMap<Integer, Integer> hashMap = new BoundedHashMap<Integer, Integer>(3);
        hashMap.put(1, 1);
        hashMap.put(2, 2);
        hashMap.put(3, 3);
        Assert.assertEquals(hashMap.size(), 3);
        hashMap.put(4, 4);
        Assert.assertEquals(hashMap.size(), 3);
        Assert.assertFalse(hashMap.containsKey(1));
    }

    @Test
    public void testMetricCache() {
        MetricCache metricCache = new MetricCache(2);
        PipelineMetrics metric1 = new PipelineMetrics("1");
        metric1.setStartTime(1);
        PipelineMetrics metric2 = new PipelineMetrics("2");
        metric2.setStartTime(2);
        metricCache.addPipelineMetrics(metric1);
        metricCache.addPipelineMetrics(metric2);
        Assert.assertEquals(metricCache.getPipelineMetricCaches().size(), 2);
        PipelineMetrics metric3 = new PipelineMetrics("3");
        metric3.setStartTime(3);
        metricCache.addPipelineMetrics(metric3);
        Assert.assertEquals(metricCache.getPipelineMetricCaches().size(), 2);
        Assert.assertFalse(metricCache.getPipelineMetricCaches().containsKey("1"));
    }

}
