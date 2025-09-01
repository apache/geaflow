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

package org.apache.geaflow.cluster.ray.client;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.JOB_WORK_PATH;

import io.ray.api.Ray;
import org.apache.geaflow.cluster.client.AbstractClusterClient;
import org.apache.geaflow.cluster.client.IPipelineClient;
import org.apache.geaflow.cluster.client.PipelineClientFactory;
import org.apache.geaflow.cluster.client.callback.ClusterStartedCallback.ClusterMeta;
import org.apache.geaflow.cluster.clustermanager.ClusterContext;
import org.apache.geaflow.cluster.clustermanager.ClusterInfo;
import org.apache.geaflow.cluster.ray.clustermanager.RayClient;
import org.apache.geaflow.cluster.ray.clustermanager.RayClusterId;
import org.apache.geaflow.cluster.ray.clustermanager.RayClusterManager;
import org.apache.geaflow.cluster.ray.utils.RaySystemFunc;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.env.ctx.IEnvironmentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RayClusterClient extends AbstractClusterClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RayClusterClient.class);
    private RayClusterManager rayClusterManager;
    private ClusterContext clusterContext;

    @Override
    public void init(IEnvironmentContext environmentContext) {
        super.init(environmentContext);
        clusterContext = new ClusterContext(config);
        rayClusterManager = new RayClusterManager();
        rayClusterManager.init(clusterContext);

        RaySystemFunc.initRayEnv(clusterContext.getClusterConfig());
        if (!config.contains(JOB_WORK_PATH)) {
            config.put(JOB_WORK_PATH, RaySystemFunc.getWorkPath());
        }
    }

    @Override
    public IPipelineClient startCluster() {
        try {
            RayClusterId clusterId = rayClusterManager.startMaster();
            ClusterInfo clusterInfo = RayClient.initMaster(clusterId.getHandler());
            ClusterMeta clusterMeta = new ClusterMeta(clusterInfo);
            callback.onSuccess(clusterMeta);
            LOGGER.info("cluster info: {}", clusterInfo);
            return PipelineClientFactory.createPipelineClient(
                clusterInfo.getDriverAddresses(), clusterContext.getConfig());
        } catch (Throwable e) {
            LOGGER.error("deploy cluster failed", e);
            callback.onFailure(e);
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        LOGGER.info("shutdown cluster");
        rayClusterManager.close();
        Ray.shutdown();
    }

}
