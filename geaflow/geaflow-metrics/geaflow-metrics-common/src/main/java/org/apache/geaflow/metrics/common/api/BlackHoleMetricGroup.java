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

public class BlackHoleMetricGroup implements MetricGroup {

    public static final BlackHoleMetricGroup INSTANCE = new BlackHoleMetricGroup();

    @Override
    public <T> void register(String name, Gauge<T> gauge) {
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Gauge<T> gauge(String name) {
        return (Gauge<T>) BlackHoleGauge.INSTANCE;
    }

    @Override
    public Counter counter(String name) {
        return BlackHoleCounter.INSTANCE;
    }

    @Override
    public Meter meter(String name) {
        return BlackHoleMeter.INSTANCE;
    }

    @Override
    public Histogram histogram(String name) {
        return BlackHoleHistogram.INSTANCE;
    }

    @Override
    public void remove(String name) {
    }

    public static class BlackHoleGauge<T> implements Gauge<T> {

        public static final BlackHoleGauge<?> INSTANCE = new BlackHoleGauge<>();

        @Override
        public T getValue() {
            return null;
        }

        @Override
        public void setValue(T value) {
        }

    }

    public static class BlackHoleCounter implements Counter {

        public static final BlackHoleCounter INSTANCE = new BlackHoleCounter();

        @Override
        public void inc() {
        }

        @Override
        public void inc(long n) {
        }

        @Override
        public void dec() {
        }

        @Override
        public void dec(long n) {
        }

        @Override
        public long getCount() {
            return 0;
        }

        @Override
        public long getCountAndReset() {
            return 0;
        }

    }

    public static class BlackHoleMeter implements Meter {

        public static final BlackHoleMeter INSTANCE = new BlackHoleMeter();

        @Override
        public void mark() {
        }

        @Override
        public void mark(long n) {
        }

        @Override
        public double getFifteenMinuteRate() {
            return 0;
        }

        @Override
        public double getFiveMinuteRate() {
            return 0;
        }

        @Override
        public double getMeanRate() {
            return 0;
        }

        @Override
        public double getOneMinuteRate() {
            return 0;
        }

        @Override
        public long getCount() {
            return 0;
        }

        @Override
        public long getCountAndReset() {
            return 0;
        }

    }

    public static class BlackHoleHistogram implements Histogram {

        public static final BlackHoleHistogram INSTANCE = new BlackHoleHistogram();

        @Override
        public void update(int value) {
        }

        @Override
        public void update(long value) {
        }

        @Override
        public long getCount() {
            return 0;
        }

    }

}
