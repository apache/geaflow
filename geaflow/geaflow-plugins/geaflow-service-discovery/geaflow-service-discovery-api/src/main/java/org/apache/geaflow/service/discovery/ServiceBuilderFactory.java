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

package org.apache.geaflow.service.discovery;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;

public class ServiceBuilderFactory {

    private static final Map<String, ServiceBuilder> CONCURRENT_TYPE_MAP = new ConcurrentHashMap<>();

    public static synchronized ServiceBuilder build(String serviceType) {
        if (CONCURRENT_TYPE_MAP.containsKey(serviceType)) {
            return CONCURRENT_TYPE_MAP.get(serviceType);
        }

        ServiceLoader<ServiceBuilder> serviceLoader = ServiceLoader.load(ServiceBuilder.class);
        for (ServiceBuilder storeBuilder : serviceLoader) {
            if (storeBuilder.serviceType().equalsIgnoreCase(serviceType)) {
                CONCURRENT_TYPE_MAP.put(serviceType, storeBuilder);
                return storeBuilder;
            }
        }
        throw new GeaflowRuntimeException(RuntimeErrors.INST.spiNotFoundError(serviceType));
    }

    public static synchronized void clear() {
        CONCURRENT_TYPE_MAP.clear();
    }
}
