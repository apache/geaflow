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

package org.apache.geaflow.stats.sink;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SYSTEM_META_TABLE;

import com.alibaba.fastjson.JSON;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.common.utils.SleepUtils;
import org.apache.geaflow.common.utils.ThreadUtil;
import org.apache.geaflow.state.DataModel;
import org.apache.geaflow.state.serializer.DefaultKVSerializer;
import org.apache.geaflow.store.IStoreBuilder;
import org.apache.geaflow.store.api.StoreBuilderFactory;
import org.apache.geaflow.store.api.key.IKVStore;
import org.apache.geaflow.store.context.StoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncKvStoreWriter implements IStatsWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncKvStoreWriter.class);
    private static final int MAX_METRIC_QUEUE_SIZE = 1024;
    private static final String DEFAULT_NAMESPACE = "default";

    private final int batchFlushSize;
    private final int flushIntervalMs;
    private volatile boolean running;

    private final IKVStore<String, String> kvStore;
    private final Queue<Tuple<String, Object>> metricQueue;
    private final ExecutorService executorService;

    public AsyncKvStoreWriter(Configuration configuration) {
        this.batchFlushSize = configuration
            .getInteger(ExecutionConfigKeys.STATS_METRIC_FLUSH_BATCH_SIZE);
        this.flushIntervalMs =
            configuration.getInteger(ExecutionConfigKeys.STATS_METRIC_FLUSH_INTERVAL_MS);
        this.kvStore = createKvStore(configuration);

        this.metricQueue = new LinkedBlockingQueue<>(MAX_METRIC_QUEUE_SIZE);
        int threadNum = configuration.getInteger(ExecutionConfigKeys.STATS_METRIC_FLUSH_THREADS);
        this.executorService = new ThreadPoolExecutor(threadNum, threadNum, 30, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(threadNum),
            ThreadUtil.namedThreadFactory(true, "stats-flusher"));
        for (int i = 0; i < threadNum; i++) {
            this.executorService.submit(new MetricFlushTask());
        }
        this.running = true;
    }

    private IKVStore<String, String> createKvStore(Configuration configuration) {
        String namespace = DEFAULT_NAMESPACE;
        if (configuration.contains(SYSTEM_META_TABLE)) {
            namespace = configuration.getString(SYSTEM_META_TABLE);
        }
        StoreContext storeContext = new StoreContext(namespace);
        storeContext.withKeySerializer(new DefaultKVSerializer(String.class, String.class));
        storeContext.withConfig(configuration);

        String storeType = configuration.getString(ExecutionConfigKeys.STATS_METRIC_STORE_TYPE);
        IStoreBuilder builder = StoreBuilderFactory.build(storeType);
        IKVStore kvStore = (IKVStore) builder.getStore(DataModel.KV, configuration);
        kvStore.init(storeContext);
        LOGGER.info("create stats store with type:{} namespace:{}", storeType, namespace);
        return kvStore;
    }

    @Override
    public void addMetric(String key, Object value) {
        Tuple<String, Object> tuple = Tuple.of(key, value);
        boolean result = metricQueue.offer(tuple);
        while (!result) {
            Tuple<String, Object> expired = metricQueue.poll();
            if (expired != null) {
                LOGGER.warn("discard metric: {} due to capacity limit", expired.getF0());
            }
            result = metricQueue.offer(tuple);
        }
    }

    @Override
    public void close() {
        if (!running) {
            return;
        }
        if (executorService != null) {
            executorService.shutdown();
        }
        running = false;
    }

    public class MetricFlushTask implements Runnable {

        private int flushSize;
        private final Queue<Tuple<String, Object>> buffers;

        public MetricFlushTask() {
            this.flushSize = 0;
            this.buffers = new LinkedList<>();
        }

        @Override
        public void run() {
            while (true) {
                try {
                    fillBuffers();
                    if (flushSize > 0) {
                        doFlush();
                    }
                    SleepUtils.sleepMilliSecond(flushIntervalMs);
                } catch (Throwable e) {
                    LOGGER.warn("flush stats metrics failed:{}", e.getMessage(), e);
                }

            }
        }

        private void fillBuffers() {
            int count = 0;
            while (count < batchFlushSize) {
                Tuple<String, Object> tuple = metricQueue.poll();
                if (tuple == null) {
                    break;
                }
                buffers.add(tuple);
                count++;
            }
            flushSize = count;
        }

        private void doFlush() {
            try {
                while (!buffers.isEmpty()) {
                    Tuple<String, Object> tuple = buffers.poll();
                    kvStore.put(tuple.f0, JSON.toJSONString(tuple.f1));
                }
                kvStore.flush();
            } catch (Throwable e) {
                LOGGER.warn("discard {} metrics due to: {}", flushSize, e.getMessage(), e);
            } finally {
                flushSize = 0;
            }
        }
    }

}
