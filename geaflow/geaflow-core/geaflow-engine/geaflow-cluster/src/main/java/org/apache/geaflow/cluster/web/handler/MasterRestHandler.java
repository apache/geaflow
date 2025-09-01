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

package org.apache.geaflow.cluster.web.handler;

import java.io.Serializable;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.geaflow.cluster.common.ComponentInfo;
import org.apache.geaflow.cluster.web.api.ApiResponse;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.metric.ProcessMetrics;
import org.apache.geaflow.stats.collector.StatsCollectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/master")
public class MasterRestHandler implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MasterRestHandler.class);

    private final Configuration configuration;
    private final ComponentInfo componentInfo;

    public MasterRestHandler(ComponentInfo componentInfo, Configuration configuration) {
        this.configuration = configuration;
        this.componentInfo = componentInfo;
    }

    @GET
    @Path("/configuration")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse<Map<String, String>> queryConfiguration() {
        try {
            return ApiResponse.success(configuration.getConfigMap());
        } catch (Throwable t) {
            LOGGER.error("Query master configuration failed. {}", t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

    @GET
    @Path("/metrics")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse<ProcessMetrics> queryProcessMetrics() {
        try {
            return ApiResponse.success(StatsCollectorFactory.init(configuration).getProcessStatsCollector().collect());
        } catch (Throwable t) {
            LOGGER.error("Query master process metrics failed. {}", t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

    @GET
    @Path("/info")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse<ComponentInfo> queryMasterInfo() {
        try {
            return ApiResponse.success(componentInfo);
        } catch (Throwable t) {
            LOGGER.error("Query master process metrics failed. {}", t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

}
