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

package org.apache.geaflow.cluster.web.metrics;

import org.apache.geaflow.common.metric.CycleMetrics;
import org.apache.geaflow.common.metric.PipelineMetrics;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.stats.model.MetricCache;
import org.junit.Test;
import org.testng.Assert;

public class MetricCacheTest {

    @Test
    public void test() {
        PipelineMetrics pipelineMetrics = new PipelineMetrics("1");
        MetricCache metricCache = new MetricCache();
        metricCache.addPipelineMetrics(pipelineMetrics);
        CycleMetrics cycleMetrics = new CycleMetrics("1", "1", "");
        metricCache.addCycleMetrics(cycleMetrics);
        byte[] bytes = SerializerFactory.getKryoSerializer().serialize(metricCache);
        MetricCache newCache = (MetricCache) SerializerFactory.getKryoSerializer().deserialize(bytes);
        Assert.assertNotNull(newCache);
        Assert.assertEquals(newCache.getPipelineMetricCaches().size(), 1);
    }

}
