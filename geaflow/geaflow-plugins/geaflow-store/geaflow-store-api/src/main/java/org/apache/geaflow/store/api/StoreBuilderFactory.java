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

package org.apache.geaflow.store.api;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.store.IStoreBuilder;

public class StoreBuilderFactory {

    private static final Map<String, IStoreBuilder> CONCURRENT_TYPE_MAP = new ConcurrentHashMap<>();

    public static synchronized IStoreBuilder build(String storeType) {
        if (CONCURRENT_TYPE_MAP.containsKey(storeType)) {
            return CONCURRENT_TYPE_MAP.get(storeType);
        }

        ServiceLoader<IStoreBuilder> serviceLoader = ServiceLoader.load(IStoreBuilder.class);
        for (IStoreBuilder storeBuilder : serviceLoader) {
            if (storeBuilder.getStoreDesc().name().equalsIgnoreCase(storeType)) {
                CONCURRENT_TYPE_MAP.put(storeType, storeBuilder);
                return storeBuilder;
            }
        }
        throw new GeaflowRuntimeException(RuntimeErrors.INST.spiNotFoundError(storeType));
    }

}
