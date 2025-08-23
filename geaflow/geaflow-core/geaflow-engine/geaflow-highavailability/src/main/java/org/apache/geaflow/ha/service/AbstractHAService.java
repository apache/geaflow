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

package org.apache.geaflow.ha.service;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.utils.SleepUtils;
import org.apache.geaflow.store.api.key.IKVStore;
import org.apache.geaflow.utils.NetworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractHAService implements IHAService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHAService.class);
    protected static final String TABLE_PREFIX = "WORKERS_";
    protected static final int LOAD_INTERVAL_MS = 200;

    protected int connectTimeout;
    protected int recoverTimeout;
    protected Map<String, ResourceData> resourceDataCache;
    protected IKVStore<String, ResourceData> kvStore;

    public AbstractHAService() {
        this.resourceDataCache = new ConcurrentHashMap<>();
    }

    @Override
    public void open(Configuration configuration) {
        this.recoverTimeout = configuration.getInteger(ExecutionConfigKeys.FO_TIMEOUT_MS);
        this.connectTimeout = configuration.getInteger(ExecutionConfigKeys.RPC_CONNECT_TIMEOUT_MS);
    }

    @Override
    public void register(String resourceId, ResourceData resourceData) {
        if (kvStore != null) {
            kvStore.put(resourceId, resourceData);
        }
    }

    @Override
    public ResourceData resolveResource(String resourceId) {
        return resourceDataCache.computeIfAbsent(resourceId, key -> loadDataFromStore(key, true));
    }

    @Override
    public ResourceData loadResource(String resourceId) {
        return resourceDataCache.computeIfAbsent(resourceId, key -> loadDataFromStore(key, false));
    }

    @Override
    public ResourceData invalidateResource(String resourceId) {
        return resourceDataCache.remove(resourceId);
    }

    @Override
    public void close() {
        if (kvStore != null) {
            kvStore.close();
        }
    }

    protected ResourceData getResourceData(String resourceId) {
        if (kvStore != null) {
            return kvStore.get(resourceId);
        }
        return null;
    }

    private ResourceData loadDataFromStore(String resourceId, boolean resolve) {
        return loadDataFromStore(resourceId, resolve, recoverTimeout, ResourceData::getRpcPort);
    }

    public ResourceData loadDataFromStore(String resourceId, boolean resolve,
                                          Function<ResourceData, Integer> portFunc) {
        return loadDataFromStore(resourceId, resolve, recoverTimeout, portFunc);
    }

    private ResourceData loadDataFromStore(String resourceId, boolean resolve, int timeoutMs,
                                           Function<ResourceData, Integer> portFunc) {
        long currentTime = System.currentTimeMillis();
        long startTime = currentTime;
        long checkTime = currentTime;
        Throwable throwable = null;
        ResourceData resourceData;
        do {
            currentTime = System.currentTimeMillis();
            if (currentTime - checkTime > 2000) {
                long elapsedTime = currentTime - startTime;
                checkTime = currentTime;
                if (elapsedTime > timeoutMs) {
                    String reason = throwable != null ? throwable.getMessage() : null;
                    String msg = String.format("load resource %s timeout after %sms, reason:%s",
                        resourceId, elapsedTime, reason);
                    LOGGER.error(msg);
                    throw new GeaflowRuntimeException(msg);
                }
                SleepUtils.sleepMilliSecond(LOAD_INTERVAL_MS);
            }
            resourceData = getResourceData(resourceId);
            if (resourceData != null) {
                try {
                    if (resolve) {
                        int port = portFunc.apply(resourceData);
                        NetworkUtil.checkServiceAvailable(resourceData.getHost(), port,
                            connectTimeout);
                    }
                    break;
                } catch (IOException ex) {
                    throwable = ex;
                }
            }
        } while (true);
        return resourceData;
    }
}
