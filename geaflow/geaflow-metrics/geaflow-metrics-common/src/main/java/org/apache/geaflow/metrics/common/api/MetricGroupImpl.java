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

package org.apache.geaflow.metrics.common.api;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.MetricRegistry;
import org.apache.geaflow.metrics.common.MetricConstants;

public class MetricGroupImpl implements MetricGroup {

    private final String groupName;
    private final MetricRegistry metricRegistry;

    public MetricGroupImpl(MetricRegistry metricRegistry) {
        this(MetricConstants.MODULE_DEFAULT, metricRegistry);
    }

    public MetricGroupImpl(String name, MetricRegistry metricRegistry) {
        this.groupName = name == null ? MetricConstants.MODULE_DEFAULT : name;
        this.metricRegistry = metricRegistry;
    }

    @Override
    public <T> void register(String name, Gauge<T> gauge) {
        metricRegistry.register(getMetricName(name), gauge);
    }

    @Override
    public <T> Gauge<T> gauge(String name) {
        return metricRegistry.gauge(getMetricName(name), GaugeImpl::new);
    }

    @Override
    public Counter counter(String name) {
        return (Counter) metricRegistry.counter(getMetricName(name), CounterImpl::new);
    }

    @Override
    public Meter meter(String name) {
        return (Meter) metricRegistry.meter(getMetricName(name), MeterImpl::new);
    }

    @Override
    public Histogram histogram(String name) {
        return (Histogram) metricRegistry.histogram(getMetricName(name), () -> new HistogramImpl(new ExponentiallyDecayingReservoir()));
    }

    @Override
    public void remove(String name) {
        metricRegistry.remove(getMetricName(name));
    }

    public String getMetricName(String name) {
        return this.groupName + MetricConstants.GROUP_DELIMITER + name;
    }

}
