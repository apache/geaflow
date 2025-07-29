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

import java.io.Serializable;

/**
 * Interface to register or get metric.
 */
public interface MetricGroup extends Serializable {

    /**
     * Registers a new {@link Gauge}.
     *
     * @param name  name of the gauge
     * @param gauge gauge to register
     */
    <T> void register(String name, Gauge<T> gauge);

    /**
     * Register a {@link com.codahale.metrics.SettableGauge} or get a existing one.
     *
     * @param name gauge name
     * @return gauge
     */
    <T> Gauge<T> gauge(String name);

    /**
     * Register or get a {@link Counter}.
     *
     * @param name name of the counter
     * @return the created counter
     */
    Counter counter(String name);

    /**
     * Registers or get a {@link Meter}.
     *
     * @param name name of the meter
     * @return the registered meter
     */
    Meter meter(String name);

    /**
     * Registers or get a {@link Histogram}.
     *
     * @param name name of the histogram
     * @return the registered histogram
     */
    Histogram histogram(String name);

    /**
     * remove a metric by name.
     *
     * @param name metricName.
     */
    void remove(String name);

}
