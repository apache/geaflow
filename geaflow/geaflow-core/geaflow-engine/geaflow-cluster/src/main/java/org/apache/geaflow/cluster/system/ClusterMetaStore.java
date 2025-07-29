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

package org.apache.geaflow.cluster.system;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CLUSTER_ID;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.cluster.protocol.IEvent;
import org.apache.geaflow.cluster.resourcemanager.WorkerSnapshot;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.pipeline.Pipeline;
import org.apache.geaflow.store.context.StoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterMetaStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterMetaStore.class);

    private static final String CLUSTER_META_NAMESPACE_LABEL = "framework";
    private static final String CLUSTER_NAMESPACE_PREFIX = "cluster";
    private static final String OFFSET_NAMESPACE = "offset";

    private static ClusterMetaStore INSTANCE;

    private final int componentId;
    private final String componentName;
    private final String clusterId;
    private final Configuration configuration;
    private final IClusterMetaKVStore<String, Object> componentBackend;
    private Map<String, IClusterMetaKVStore<String, Object>> backends;

    private ClusterMetaStore(int id, String name, Configuration configuration) {
        this.componentId = id;
        this.componentName = name;
        this.configuration = configuration;
        this.backends = new ConcurrentHashMap<>();
        this.clusterId = configuration.getString(CLUSTER_ID);
        String namespace = String.format("%s/%s/%s", CLUSTER_NAMESPACE_PREFIX, clusterId, componentName);
        this.componentBackend = createBackend(namespace);
        this.backends.put(namespace, componentBackend);
    }

    public static void init(int id, String name, Configuration configuration) {
        if (INSTANCE == null) {
            synchronized (ClusterMetaStore.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ClusterMetaStore(id, name, configuration);
                }
            }
        }
    }

    public static ClusterMetaStore getInstance(int id, String name, Configuration configuration) {
        if (INSTANCE == null) {
            init(id, name, configuration);
        }
        return INSTANCE;
    }

    public static ClusterMetaStore getInstance() {
        return INSTANCE;
    }

    public static synchronized void close() {
        LOGGER.info("close ClusterMetaStore");
        if (INSTANCE != null) {
            Map<String, IClusterMetaKVStore<String, Object>> backends = INSTANCE.backends;
            INSTANCE.backends = null;
            for (IClusterMetaKVStore<String, Object> backend : backends.values()) {
                backend.close();
            }
            INSTANCE = null;
        }
    }

    public ClusterMetaStore savePipeline(Pipeline pipeline) {
        save(ClusterMetaKey.PIPELINE, pipeline);
        return this;
    }

    public ClusterMetaStore savePipelineTaskIds(List<Long> pipelineTaskIds) {
        save(ClusterMetaKey.PIPELINE_TASK_IDS, pipelineTaskIds);
        return this;
    }

    public ClusterMetaStore savePipelineTasks(List<Integer> taskIndices) {
        save(ClusterMetaKey.PIPELINE_TASKS, taskIndices);
        return this;
    }

    /**
     * Auto flush after save value.
     */
    public void saveWindowId(Long windowId, long pipelineTaskId) {
        save(ClusterMetaKey.WINDOW_ID, windowId, pipelineTaskId);
    }

    public ClusterMetaStore saveCycle(Object cycle, long pipelineTaskId) {
        save(ClusterMetaKey.CYCLE, cycle, pipelineTaskId);
        return this;
    }

    public ClusterMetaStore saveEvent(List<IEvent> event) {
        save(ClusterMetaKey.EVENTS, event);
        return this;
    }

    /**
     * Auto flush after save value.
     */
    public void saveWorkers(WorkerSnapshot workers) {
        save(ClusterMetaKey.WORKERS, workers);
    }

    public ClusterMetaStore saveContainerIds(Map<Integer, String> containerIds) {
        save(ClusterMetaKey.CONTAINER_IDS, containerIds);
        return this;
    }

    public ClusterMetaStore saveDriverIds(Map<Integer, String> driverIds) {
        save(ClusterMetaKey.DRIVER_IDS, driverIds);
        return this;
    }

    public ClusterMetaStore saveMaxContainerId(int containerId) {
        save(ClusterMetaKey.MAX_CONTAINER_ID, containerId);
        return this;
    }

    public Pipeline getPipeline() {
        return get(ClusterMetaKey.PIPELINE);
    }

    public List<Long> getPipelineTaskIds() {
        return get(ClusterMetaKey.PIPELINE_TASK_IDS);
    }

    public List<Integer> getPipelineTasks() {
        return get(ClusterMetaKey.PIPELINE_TASKS);
    }

    public Long getWindowId(long pipelineTaskId) {
        return get(ClusterMetaKey.WINDOW_ID, pipelineTaskId);
    }

    public Object getCycle(long pipelineTaskId) {
        return get(ClusterMetaKey.CYCLE, pipelineTaskId);
    }

    public List<IEvent> getEvents() {
        return get(ClusterMetaKey.EVENTS);
    }

    public WorkerSnapshot getWorkers() {
        return get(ClusterMetaKey.WORKERS);
    }

    public int getMaxContainerId() {
        return get(ClusterMetaKey.MAX_CONTAINER_ID);
    }

    public Map<Integer, String> getContainerIds() {
        return get(ClusterMetaKey.CONTAINER_IDS);
    }

    public Map<Integer, String> getDriverIds() {
        return get(ClusterMetaKey.DRIVER_IDS);
    }

    public void flush() {
        componentBackend.flush();
    }

    public void clean() {
        // TODO Clean namespace directly from backend.
    }

    private <T> void save(ClusterMetaKey key, T value) {
        getBackend(key).put(key.name(), value);
    }

    private <T> void save(ClusterMetaKey key, T value, long pipelineTaskId) {
        getBackend(key).put(getKeyTag(key.name(), pipelineTaskId), value);
    }

    private <T> T get(ClusterMetaKey key) {
        return (T) getBackend(key).get(key.name());
    }

    private <T> T get(ClusterMetaKey key, long pipelineTaskId) {
        return (T) getBackend(key).get(getKeyTag(key.name(), pipelineTaskId));
    }

    private String getKeyTag(String key, long pipelineTaskId) {
        return String.format("%s#%s", key, pipelineTaskId);
    }

    private IClusterMetaKVStore<String, Object> getBackend(ClusterMetaKey metaKey) {
        String namespace;
        switch (metaKey) {
            case WORKERS:
                namespace = String.format("%s/%s/%s", CLUSTER_NAMESPACE_PREFIX, clusterId, metaKey.name().toLowerCase());
                break;
            case WINDOW_ID:
                namespace = OFFSET_NAMESPACE;
                break;
            default:
                return componentBackend;
        }
        // Cluster meta store is closed.
        if (backends == null) {
            return null;
        }
        if (!backends.containsKey(namespace)) {
            synchronized (ClusterMetaStore.class) {
                if (!backends.containsKey(namespace)) {
                    IClusterMetaKVStore<String, Object> backend = createBackend(namespace);
                    backends.put(namespace, new ClusterMetaKVStoreProxy<>(backend));
                }
            }
        }
        return backends.get(namespace);
    }

    private IClusterMetaKVStore<String, Object> createBackend(String namespace) {
        String storeKey = String.format("%s/%s", CLUSTER_META_NAMESPACE_LABEL, namespace);
        IClusterMetaKVStore<String, Object> backend =
            ClusterMetaStoreFactory.create(storeKey, componentId, configuration);
        LOGGER.info("create ClusterMetaStore, store key {}, id {}", storeKey, componentId);
        return backend;
    }

    public enum ClusterMetaKey {

        PIPELINE,
        PIPELINE_TASK_IDS,
        PIPELINE_TASKS,
        WINDOW_ID,
        CYCLE,
        EVENTS,
        WORKERS,
        CONTAINER_IDS,
        DRIVER_IDS,
        MAX_CONTAINER_ID
    }

    /**
     * A proxy that flush immediately once put entry to store.
     */
    private class ClusterMetaKVStoreProxy<K, V> implements IClusterMetaKVStore<K, V> {

        public ClusterMetaKVStoreProxy(IClusterMetaKVStore<K, V> store) {
            this.store = store;
        }

        private IClusterMetaKVStore<K, V> store;

        @Override
        public void init(StoreContext storeContext) {
            store.init(storeContext);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
            store.close();
        }

        @Override
        public V get(K key) {
            return store.get(key);
        }

        @Override
        public void put(K key, V value) {
            store.put(key, value);
            store.flush();
        }

        @Override
        public void remove(K key) {
            store.remove(key);
        }
    }
}
