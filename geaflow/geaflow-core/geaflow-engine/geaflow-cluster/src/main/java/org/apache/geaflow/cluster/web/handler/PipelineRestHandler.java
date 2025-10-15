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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.geaflow.cluster.web.api.ApiResponse;
import org.apache.geaflow.cluster.web.metrics.MetricFetcher;
import org.apache.geaflow.common.metric.CycleMetrics;
import org.apache.geaflow.common.metric.PipelineMetrics;
import org.apache.geaflow.stats.model.MetricCache;
import org.apache.geaflow.stats.model.MetricCache.PipelineMetricCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/pipelines")
public class PipelineRestHandler implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineRestHandler.class);

    private final MetricCache metricCache;
    private final MetricFetcher metricFetcher;

    public PipelineRestHandler(MetricCache metricCache, MetricFetcher metricFetcher) {
        this.metricCache = metricCache;
        this.metricFetcher = metricFetcher;
    }

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse<List<PipelineMetrics>> queryPipelineList() {
        try {
            metricFetcher.update();
            List<PipelineMetrics> list = new ArrayList<>();
            for (PipelineMetricCache cache : metricCache.getPipelineMetricCaches().values()) {
                if (cache.getPipelineMetrics() != null) {
                    list.add(cache.getPipelineMetrics());
                }
            }
            return ApiResponse.success(list);
        } catch (Throwable t) {
            LOGGER.error("Query pipeline list failed. {}", t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

    @GET
    @Path("/{pipelineName}/cycles")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse<Collection<CycleMetrics>> queryCycleList(@PathParam("pipelineName") String pipelineName) {
        try {
            metricFetcher.update();
            PipelineMetricCache cache = metricCache.getPipelineMetricCaches().get(pipelineName);
            if (cache == null) {
                return ApiResponse.success(Collections.EMPTY_LIST);
            }
            return ApiResponse.success(cache.getCycleMetricList().values());
        } catch (Throwable t) {
            LOGGER.error("Query cycle metric list of pipeline {} failed. {}", pipelineName,
                t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

}
