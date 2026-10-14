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

package org.apache.geaflow.ai.retrieval.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.noear.solon.annotation.Component;

/** Process-local counters for the Week 1 retrieval endpoint. */
@Component
public final class RetrievalMetrics {

    private final AtomicLong success = new AtomicLong();
    private final AtomicLong failure = new AtomicLong();
    private final AtomicLong timeout = new AtomicLong();
    private final AtomicLong total = new AtomicLong();
    private final AtomicLong totalElapsedMs = new AtomicLong();
    private final List<Long> elapsedSamples = new ArrayList<>();
    private final java.util.concurrent.ConcurrentHashMap<String, AtomicLong> channelCounters =
        new java.util.concurrent.ConcurrentHashMap<>();

    public void recordSuccess() {
        recordSuccess(0L);
    }

    public void recordSuccess(long elapsedMs) {
        total.incrementAndGet();
        success.incrementAndGet();
        totalElapsedMs.addAndGet(Math.max(0L, elapsedMs));
        sample(elapsedMs);
    }

    public void recordFailure(RetrievalErrorCode code) {
        recordFailure(code, 0L);
    }

    public void recordFailure(RetrievalErrorCode code, long elapsedMs) {
        total.incrementAndGet();
        failure.incrementAndGet();
        totalElapsedMs.addAndGet(Math.max(0L, elapsedMs));
        sample(elapsedMs);
        if (code == RetrievalErrorCode.RETRIEVAL_TIMEOUT) {
            timeout.incrementAndGet();
        }
    }

    public Snapshot snapshot() {
        return new Snapshot(total.get(), success.get(), failure.get(), timeout.get(),
            totalElapsedMs.get(), percentile(0.50D), percentile(0.95D), channelCounters);
    }

    public void recordChannel(String channel) {
        if (channel != null) {
            channelCounters.computeIfAbsent(channel, ignored -> new AtomicLong()).incrementAndGet();
        }
    }

    private void sample(long elapsedMs) {
        synchronized (elapsedSamples) {
            if (elapsedSamples.size() >= 2048) {
                elapsedSamples.remove(0);
            }
            elapsedSamples.add(Math.max(0L, elapsedMs));
        }
    }

    private long percentile(double fraction) {
        synchronized (elapsedSamples) {
            if (elapsedSamples.isEmpty()) {
                return 0L;
            }
            List<Long> sorted = new ArrayList<>(elapsedSamples);
            Collections.sort(sorted);
            int index = (int) Math.ceil(fraction * sorted.size()) - 1;
            return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
        }
    }

    public static final class Snapshot {
        private final long total;
        private final long success;
        private final long failure;
        private final long timeout;
        private final long totalElapsedMs;
        private final long p50ElapsedMs;
        private final long p95ElapsedMs;
        private final java.util.Map<String, Long> channelCounts;

        private Snapshot(long total, long success, long failure, long timeout, long totalElapsedMs,
                         long p50ElapsedMs, long p95ElapsedMs,
                         java.util.Map<String, AtomicLong> channelCounters) {
            this.total = total;
            this.success = success;
            this.failure = failure;
            this.timeout = timeout;
            this.totalElapsedMs = totalElapsedMs;
            this.p50ElapsedMs = p50ElapsedMs;
            this.p95ElapsedMs = p95ElapsedMs;
            this.channelCounts = new java.util.LinkedHashMap<>();
            for (java.util.Map.Entry<String, AtomicLong> entry : channelCounters.entrySet()) {
                this.channelCounts.put(entry.getKey(), entry.getValue().get());
            }
        }

        public long getTotal() {
            return total;
        }

        public long getSuccess() {
            return success;
        }

        public long getFailure() {
            return failure;
        }

        public long getTimeout() {
            return timeout;
        }

        public long getTotalElapsedMs() {
            return totalElapsedMs;
        }

        public long getP50ElapsedMs() {
            return p50ElapsedMs;
        }

        public long getP95ElapsedMs() {
            return p95ElapsedMs;
        }

        public java.util.Map<String, Long> getChannelCounts() {
            return java.util.Collections.unmodifiableMap(channelCounts);
        }
    }
}
