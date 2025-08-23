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

package org.apache.geaflow.metaserver.api;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.geaflow.common.mode.JobMode;
import org.apache.geaflow.metaserver.MetaServerContext;
import org.apache.geaflow.metaserver.local.DefaultServiceHandler;
import org.apache.geaflow.metaserver.service.NamespaceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceHandlerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceHandlerFactory.class);

    public static synchronized Map<NamespaceType, NamespaceServiceHandler> load(
        MetaServerContext context) {
        JobMode jobMode = JobMode.getJobMode(context.getConfiguration());

        Map<NamespaceType, NamespaceServiceHandler> map = Maps.newConcurrentMap();
        ServiceLoader<NamespaceServiceHandler> serviceLoader = ServiceLoader.load(
            NamespaceServiceHandler.class);
        for (NamespaceServiceHandler handler : serviceLoader) {
            if (jobMode.name().equals(handler.namespaceType().name())) {
                LOGGER.info("{} register service handler", handler.namespaceType());
                handler.init(context);
                map.put(handler.namespaceType(), handler);
            }
        }
        DefaultServiceHandler defaultServiceHandler = new DefaultServiceHandler();
        defaultServiceHandler.init(context);
        map.put(defaultServiceHandler.namespaceType(), defaultServiceHandler);
        return map;
    }
}
