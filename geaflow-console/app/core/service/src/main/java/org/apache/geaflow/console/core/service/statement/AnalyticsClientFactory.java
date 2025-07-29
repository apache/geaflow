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

package org.apache.geaflow.console.core.service.statement;

import lombok.extern.slf4j.Slf4j;
import org.apache.geaflow.console.common.service.integration.engine.analytics.AnalyticsClient;
import org.apache.geaflow.console.common.service.integration.engine.analytics.AnalyticsClientBuilder;
import org.apache.geaflow.console.common.service.integration.engine.analytics.Configuration;
import org.apache.geaflow.console.core.model.task.GeaflowTask;
import org.apache.geaflow.console.core.service.version.VersionClassLoader;
import org.apache.geaflow.console.core.service.version.VersionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class AnalyticsClientFactory {

    @Autowired
    private VersionFactory versionFactory;

    public AnalyticsClient buildClient(GeaflowTask task) {
        final VersionClassLoader classLoader = versionFactory.getClassLoader(task.getRelease().getVersion());
        final AnalyticsClientBuilder builder = classLoader.newInstance(AnalyticsClientBuilder.class);
        Configuration configuration = classLoader.newInstance(Configuration.class);
        final String redisParentNamespace = "/geaflow" + task.getId();
        configuration.putAll(task.getRelease().getJobConfig().toStringMap());
        configuration.put("brpc.connect.timeout.ms", String.valueOf(8000));
        configuration.put("geaflow.meta.server.retry.times", String.valueOf(2));
        configuration.put("geaflow.job.runtime.name", redisParentNamespace);
        return builder.withConfiguration(configuration)
            .withInitChannelPools(true)
            .build();
    }

}
