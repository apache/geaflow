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

    public void recordSuccess() {
        recordSuccess(0L);
    }

    public void recordSuccess(long elapsedMs) {
        total.incrementAndGet();
        success.incrementAndGet();
        totalElapsedMs.addAndGet(Math.max(0L, elapsedMs));
    }

    public void recordFailure(RetrievalErrorCode code) {
        recordFailure(code, 0L);
    }

    public void recordFailure(RetrievalErrorCode code, long elapsedMs) {
        total.incrementAndGet();
        failure.incrementAndGet();
        totalElapsedMs.addAndGet(Math.max(0L, elapsedMs));
        if (code == RetrievalErrorCode.RETRIEVAL_TIMEOUT) {
            timeout.incrementAndGet();
        }
    }

    public Snapshot snapshot() {
        return new Snapshot(total.get(), success.get(), failure.get(), timeout.get(),
            totalElapsedMs.get());
    }

    public static final class Snapshot {
        private final long total;
        private final long success;
        private final long failure;
        private final long timeout;
        private final long totalElapsedMs;

        private Snapshot(long total, long success, long failure, long timeout, long totalElapsedMs) {
            this.total = total;
            this.success = success;
            this.failure = failure;
            this.timeout = timeout;
            this.totalElapsedMs = totalElapsedMs;
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
    }
}
