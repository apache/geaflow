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
import org.apache.geaflow.metrics.common.api.Counter;
import org.apache.geaflow.metrics.common.api.Gauge;
import org.apache.geaflow.metrics.common.api.Meter;
import org.apache.geaflow.metrics.common.api.MetricGroupImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MetricGroupTest {

    @Test
    public void testGauge() {
        MetricRegistry registry = new MetricRegistry();
        MetricGroupImpl metricGroup = new MetricGroupImpl(registry);

        String gaugeName = "newGauge";
        Gauge gauge = metricGroup.gauge(metricGroup.getMetricName(gaugeName));
        gauge.setValue(1.0);
        Assert.assertEquals(gauge.getValue(), 1.0);
    }

    @Test
    public void testCounter() {
        MetricRegistry registry = new MetricRegistry();
        MetricGroupImpl metricGroup = new MetricGroupImpl(registry);

        String counterName = "newCounter";
        Counter counter = metricGroup.counter(counterName);
        counter.inc();
        counter.inc(1);
        Assert.assertEquals(counter.getCount(), 2);
        Assert.assertTrue(counter instanceof com.codahale.metrics.Counter);

        Counter counter1 = (Counter) registry.getCounters().get(metricGroup.getMetricName(counterName));
        Assert.assertEquals(counter1.getCountAndReset(), 2);
        Assert.assertEquals(counter1.getCount(), 0);
    }

    @Test
    public void testMeter() {
        MetricRegistry registry = new MetricRegistry();
        MetricGroupImpl metricGroup = new MetricGroupImpl(registry);

        String meterName = "testMeter";
        Meter meter = metricGroup.meter(meterName);
        meter.mark();
        meter.mark(1);
        Assert.assertEquals(meter.getCount(), 2);
        Assert.assertTrue(meter instanceof com.codahale.metrics.Meter);

        Meter meter1 = (Meter) registry.getMeters().get(metricGroup.getMetricName(meterName));
        Assert.assertEquals(meter1.getCountAndReset(), 2);
        Assert.assertEquals(meter1.getCount(), 0);
    }

}
