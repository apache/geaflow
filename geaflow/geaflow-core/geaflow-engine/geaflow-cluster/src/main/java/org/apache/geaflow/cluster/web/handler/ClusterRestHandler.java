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

import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.geaflow.cluster.clustermanager.AbstractClusterManager;
import org.apache.geaflow.cluster.clustermanager.IClusterManager;
import org.apache.geaflow.cluster.common.ComponentInfo;
import org.apache.geaflow.cluster.container.ContainerInfo;
import org.apache.geaflow.cluster.driver.DriverInfo;
import org.apache.geaflow.cluster.heartbeat.HeartbeatManager;
import org.apache.geaflow.cluster.resourcemanager.DefaultResourceManager;
import org.apache.geaflow.cluster.resourcemanager.IResourceManager;
import org.apache.geaflow.cluster.web.api.ApiResponse;
import org.apache.geaflow.cluster.web.metrics.MetricFetcher;
import org.apache.geaflow.cluster.web.metrics.ResourceMetrics;
import org.apache.geaflow.common.heartbeat.Heartbeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
public class ClusterRestHandler implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterRestHandler.class);

    private static final String TOTAL_CONTAINER_NUM = "totalContainers";
    private static final String TOTAL_DRIVER_NUM = "totalDrivers";
    private static final String ACTIVE_CONTAINER_NUM = "activeContainers";
    private static final String ACTIVE_DRIVER_NUM = "activeDrivers";
    private static final String TOTAL_WORKER_NUM = "totalWorkers";
    private static final String USED_WORKER_NUM = "usedWorkers";
    private static final String AVAILABLE_WORKER_NUM = "availableWorkers";
    private static final String PENDING_WORKER_NUM = "pendingWorkers";

    private static final String CONTAINER_ID_KEY = "id";
    private static final String CONTAINER_NAME_KEY = "name";
    private static final String CONTAINER_HOST_KEY = "host";
    private static final String PROCESS_ID_KEY = "pid";
    private static final String AGENT_PORT_KEY = "agentPort";
    private static final String PROCESS_METRICS_KEY = "metrics";
    private static final String LAST_UPDATE_TIME = "lastTimestamp";
    private static final String IS_ACTIVE = "isActive";

    private final AbstractClusterManager clusterManager;
    private final HeartbeatManager heartbeatManager;
    private final IResourceManager resourceManager;
    private final MetricFetcher metricFetcher;

    public ClusterRestHandler(IClusterManager clusterManager, HeartbeatManager heartbeatManager,
                              IResourceManager resourceManager, MetricFetcher metricFetcher) {
        this.clusterManager = (AbstractClusterManager) clusterManager;
        this.heartbeatManager = heartbeatManager;
        this.resourceManager = resourceManager;
        this.metricFetcher = metricFetcher;
    }

    @GET
    @Path("/overview")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse<Map<String, Object>> getOverview() throws IOException {
        try {
            metricFetcher.update();
            Set<Integer> heartbeatIds = heartbeatManager.getHeartBeatMap().keySet();
            Set<Integer> activeContainerIds = new HashSet<>(heartbeatIds);
            Set<Integer> activeDriverIds = new HashSet<>(heartbeatIds);
            activeContainerIds.retainAll(heartbeatManager.getActiveContainerIds());
            activeDriverIds.retainAll(heartbeatManager.getActiveDriverIds());
            int activeContainerNum = activeContainerIds.size();
            int activeDriverNum = activeDriverIds.size();
            int totalContainerNum = clusterManager.getTotalContainers();
            int totalDriverNum = clusterManager.getTotalDrivers();
            Map<String, Object> ret = new HashMap<>();
            ret.put(TOTAL_CONTAINER_NUM, totalContainerNum);
            ret.put(TOTAL_DRIVER_NUM, totalDriverNum);
            ret.put(ACTIVE_CONTAINER_NUM, activeContainerNum);
            ret.put(ACTIVE_DRIVER_NUM, activeDriverNum);
            if (resourceManager instanceof DefaultResourceManager) {
                ResourceMetrics metrics =
                    ((DefaultResourceManager) resourceManager).getResourceMetrics();
                ret.put(TOTAL_WORKER_NUM, metrics.getTotalWorkers());
                ret.put(AVAILABLE_WORKER_NUM, metrics.getAvailableWorkers());
                ret.put(PENDING_WORKER_NUM, metrics.getPendingWorkers());
                ret.put(USED_WORKER_NUM, metrics.getTotalWorkers() - metrics.getAvailableWorkers());
            }
            return ApiResponse.success(ret);
        } catch (Throwable t) {
            LOGGER.error("Query overview info failed. {}", t.getMessage(), t);
            return ApiResponse.error(t);
        }

    }

    @GET
    @Path("/containers")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse<List<JSONObject>> getContainers() throws IOException {
        try {
            metricFetcher.update();
            Map<Integer, ContainerInfo> containerMap = clusterManager.getContainerInfos();
            Map<Integer, Heartbeat> heartbeatMap = heartbeatManager.getHeartBeatMap();
            Set<Integer> activeContainerIds = heartbeatManager.getActiveContainerIds();
            return ApiResponse.success(buildDetailInfo(containerMap, heartbeatMap, activeContainerIds));
        } catch (Throwable t) {
            LOGGER.error("Query containers failed. {}", t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

    @GET
    @Path("/drivers")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse<List<JSONObject>> getDrivers() throws IOException {

        try {
            metricFetcher.update();
            Map<Integer, DriverInfo> driverMap = clusterManager.getDriverInfos();
            Map<Integer, Heartbeat> heartbeatMap = heartbeatManager.getHeartBeatMap();
            Set<Integer> activeDriverIds = heartbeatManager.getActiveDriverIds();
            return ApiResponse.success(buildDetailInfo(driverMap, heartbeatMap, activeDriverIds));
        } catch (Throwable t) {
            LOGGER.error("Query drivers failed. {}", t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

    private <T extends ComponentInfo> List<JSONObject> buildDetailInfo(Map<Integer, T> componentMap,
                                                                       Map<Integer, Heartbeat> heartbeatMap,
                                                                       Set<Integer> activeComponentIds) {
        List<JSONObject> result = new ArrayList<>();
        for (Map.Entry<Integer, T> entry : componentMap.entrySet()) {
            Integer componentId = entry.getKey();
            JSONObject containerObj = new JSONObject();
            containerObj.put(CONTAINER_ID_KEY, componentId);
            ComponentInfo info = entry.getValue();
            containerObj.put(CONTAINER_NAME_KEY, info.getName());
            containerObj.put(CONTAINER_HOST_KEY, info.getHost());
            containerObj.put(AGENT_PORT_KEY, info.getAgentPort());
            containerObj.put(PROCESS_ID_KEY, info.getPid());
            Heartbeat heartbeat = heartbeatMap.get(componentId);
            if (heartbeat != null) {
                containerObj.put(LAST_UPDATE_TIME, heartbeat.getTimestamp());
                containerObj.put(PROCESS_METRICS_KEY, heartbeat.getProcessMetrics());
            }
            containerObj.put(IS_ACTIVE, activeComponentIds.contains(componentId));
            result.add(containerObj);
        }
        return result;
    }
}
