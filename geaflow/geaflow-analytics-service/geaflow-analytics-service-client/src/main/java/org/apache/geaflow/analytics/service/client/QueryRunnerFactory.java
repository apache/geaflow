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

package org.apache.geaflow.analytics.service.client;

import java.util.Iterator;
import java.util.ServiceLoader;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.pipeline.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryRunnerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryRunnerFactory.class);

    public static IQueryRunner loadQueryRunner(QueryRunnerContext handlerContext) {
        Configuration configuration = handlerContext.getConfiguration();
        String type = configuration.getString(FrameworkConfigKeys.SERVICE_SERVER_TYPE);
        ServiceLoader<IQueryRunner> contextLoader = ServiceLoader.load(IQueryRunner.class);
        Iterator<IQueryRunner> contextIterable = contextLoader.iterator();
        while (contextIterable.hasNext()) {
            IQueryRunner clientHandler = contextIterable.next();
            if (clientHandler.getServiceType() == ServiceType.getEnum(type)) {
                LOGGER.info("loaded IClientHandler implementation {}", clientHandler);
                clientHandler.init(handlerContext);
                return clientHandler;
            }
        }
        LOGGER.error("NOT found IClientHandler implementation with type:{}", type);
        throw new GeaflowRuntimeException(RuntimeErrors.INST.spiNotFoundError(IQueryRunner.class.getSimpleName()));
    }
}
