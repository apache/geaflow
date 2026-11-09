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

package org.apache.geaflow.infer;

import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.INFER_CONTEXT_POOL_BORROW_TIMEOUT_SEC;
import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.INFER_CONTEXT_POOL_MAX_SIZE;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared pool for infer contexts keyed by stable configuration fingerprint.
 */
public class InferContextPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(InferContextPool.class);

    private static final ConcurrentHashMap<String, PoolEntry> CONTEXT_POOL =
        new ConcurrentHashMap<>();

    private static final AtomicLong CREATED_CONTEXT_COUNT = new AtomicLong();
    private static final AtomicLong CLOSED_CONTEXT_COUNT = new AtomicLong();
    private static volatile InferContextFactory contextFactory = InferContext::new;

    private InferContextPool() {
    }

    @SuppressWarnings("unchecked")
    public static <OUT> InferContextLease<OUT> borrow(Configuration config) {
        String key = generateConfigKey(config);
        int maxSize = Math.max(1, config.getInteger(INFER_CONTEXT_POOL_MAX_SIZE));
        long timeoutMillis = Math.max(0L,
            TimeUnit.SECONDS.toMillis(config.getInteger(INFER_CONTEXT_POOL_BORROW_TIMEOUT_SEC)));
        while (true) {
            PoolEntry entry = CONTEXT_POOL.computeIfAbsent(key, PoolEntry::new);
            try {
                return new InferContextLease<>(entry,
                    (InferContext<OUT>) entry.borrow(config, maxSize, timeoutMillis));
            } catch (PoolEntryClosedException e) {
                CONTEXT_POOL.remove(key, entry);
            }
        }
    }

    public static void closeAll() {
        List<PoolEntry> entries = new ArrayList<>(CONTEXT_POOL.values());
        for (PoolEntry entry : entries) {
            entry.closeEntry();
        }
        CONTEXT_POOL.clear();
        LOGGER.info("Closed all InferContext instances, closedPoolSize={}, createdCount={}, closedCount={}",
            entries.size(), CREATED_CONTEXT_COUNT.get(), CLOSED_CONTEXT_COUNT.get());
    }

    public static String getStatus() {
        return String.format("InferContextPool{size=%d, instances=%s, createdCount=%d, closedCount=%d}",
            CONTEXT_POOL.size(), CONTEXT_POOL.keySet(), CREATED_CONTEXT_COUNT.get(),
            CLOSED_CONTEXT_COUNT.get());
    }

    private static String generateConfigKey(Configuration config) {
        StringBuilder builder = new StringBuilder();
        builder.append("masterId=").append(nullToEmpty(config.getMasterId())).append('\n');
        Map<String, String> sortedConfig = new TreeMap<>(config.getConfigMap());
        for (Map.Entry<String, String> entry : sortedConfig.entrySet()) {
            builder.append(entry.getKey())
                .append('=')
                .append(nullToEmpty(entry.getValue()))
                .append('\n');
        }
        return "infer_" + sha256Hex(builder.toString());
    }

    static void setContextFactoryForTest(InferContextFactory factory) {
        contextFactory = factory;
    }

    static void resetContextFactoryForTest() {
        contextFactory = InferContext::new;
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private static String sha256Hex(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                builder.append(String.format("%02x", b));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new GeaflowRuntimeException("SHA-256 is not available", e);
        }
    }

    interface InferContextFactory {

        InferContext<?> create(Configuration config);
    }

    static class PoolEntry {

        private final String key;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition available = lock.newCondition();
        private final Deque<InferContext<?>> idleContexts = new ArrayDeque<>();
        private final Set<InferContext<?>> allContexts = new HashSet<>();
        private int creatingCount;
        private int borrowedCount;
        private int waitingBorrowers;
        private boolean closed;

        PoolEntry(String key) {
            this.key = key;
        }

        InferContext<?> borrow(Configuration config, int maxSize, long timeoutMillis) {
            long remainingNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            while (true) {
                InferContext<?> idleContext = tryBorrowIdle();
                if (idleContext != null) {
                    return idleContext;
                }
                if (reserveCreation(maxSize)) {
                    return createContext(config);
                }
                remainingNanos = awaitAvailable(remainingNanos, timeoutMillis);
            }
        }

        void release(InferContext<?> inferContext, boolean broken) {
            List<InferContext<?>> contextsToClose = new ArrayList<>();
            boolean removeEntry;
            lock.lock();
            try {
                if (!allContexts.contains(inferContext)) {
                    return;
                }
                borrowedCount--;
                if (closed || broken) {
                    allContexts.remove(inferContext);
                    contextsToClose.add(inferContext);
                } else {
                    idleContexts.addLast(inferContext);
                }
                available.signal();
                removeEntry = (closed || allContexts.isEmpty()) && borrowedCount == 0
                    && waitingBorrowers == 0;
            } finally {
                lock.unlock();
            }
            closeContexts(contextsToClose);
            if (removeEntry) {
                CONTEXT_POOL.remove(key, this);
            }
        }

        void closeEntry() {
            List<InferContext<?>> contextsToClose;
            lock.lock();
            try {
                if (closed && allContexts.isEmpty() && idleContexts.isEmpty()) {
                    return;
                }
                closed = true;
                contextsToClose = new ArrayList<>(allContexts);
                idleContexts.clear();
                allContexts.clear();
                borrowedCount = 0;
                available.signalAll();
            } finally {
                lock.unlock();
            }
            closeContexts(contextsToClose);
        }

        private InferContext<?> tryBorrowIdle() {
            lock.lock();
            try {
                if (closed) {
                    throw new PoolEntryClosedException();
                }
                InferContext<?> inferContext = idleContexts.pollFirst();
                if (inferContext != null) {
                    borrowedCount++;
                }
                return inferContext;
            } finally {
                lock.unlock();
            }
        }

        private boolean reserveCreation(int maxSize) {
            lock.lock();
            try {
                if (closed) {
                    throw new PoolEntryClosedException();
                }
                if (allContexts.size() + creatingCount >= maxSize) {
                    return false;
                }
                creatingCount++;
                return true;
            } finally {
                lock.unlock();
            }
        }

        private InferContext<?> createContext(Configuration config) {
            InferContext<?> inferContext;
            try {
                inferContext = contextFactory.create(config);
            } catch (RuntimeException e) {
                lock.lock();
                try {
                    creatingCount--;
                    available.signal();
                } finally {
                    lock.unlock();
                }
                throw e;
            }

            lock.lock();
            try {
                creatingCount--;
                if (closed) {
                    available.signal();
                    closeContexts(java.util.Collections.singletonList(inferContext));
                    throw new PoolEntryClosedException();
                }
                allContexts.add(inferContext);
                borrowedCount++;
                long createdCount = CREATED_CONTEXT_COUNT.incrementAndGet();
                LOGGER.info("Created InferContext for key {}, poolSize={}, createdCount={}",
                    key, CONTEXT_POOL.size(), createdCount);
                return inferContext;
            } finally {
                lock.unlock();
            }
        }

        private long awaitAvailable(long remainingNanos, long timeoutMillis) {
            lock.lock();
            try {
                if (closed) {
                    throw new PoolEntryClosedException();
                }
                if (timeoutMillis == 0L) {
                    throw borrowTimeoutException(key, timeoutMillis);
                }
                waitingBorrowers++;
                try {
                    if (remainingNanos <= 0L) {
                        throw borrowTimeoutException(key, timeoutMillis);
                    }
                    return available.awaitNanos(remainingNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new GeaflowRuntimeException("infer context borrow interrupted", e);
                } finally {
                    waitingBorrowers--;
                }
            } finally {
                lock.unlock();
            }
        }

        private void closeContexts(List<InferContext<?>> contexts) {
            for (InferContext<?> inferContext : contexts) {
                try {
                    inferContext.close();
                    CLOSED_CONTEXT_COUNT.incrementAndGet();
                } catch (Exception e) {
                    LOGGER.warn("Failed to close InferContext for key {}", key, e);
                }
            }
        }
    }

    private static GeaflowRuntimeException borrowTimeoutException(String key, long timeoutMillis) {
        return new GeaflowRuntimeException(String.format(
            "borrow infer context timeout, key=%s, timeoutMillis=%d", key, timeoutMillis));
    }

    static final class PoolEntryClosedException extends RuntimeException {

        private static final long serialVersionUID = 1L;
    }
}
