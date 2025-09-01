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

package org.apache.geaflow.cluster.local.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.geaflow.cluster.client.AbstractClusterClient;
import org.apache.geaflow.cluster.client.IPipelineClient;
import org.apache.geaflow.cluster.client.PipelineClientFactory;
import org.apache.geaflow.cluster.client.callback.ClusterStartedCallback.ClusterMeta;
import org.apache.geaflow.cluster.clustermanager.ClusterContext;
import org.apache.geaflow.cluster.clustermanager.ClusterInfo;
import org.apache.geaflow.cluster.local.clustermanager.LocalClient;
import org.apache.geaflow.cluster.local.clustermanager.LocalClusterId;
import org.apache.geaflow.cluster.local.clustermanager.LocalClusterManager;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.utils.ThreadUtil;
import org.apache.geaflow.env.ctx.IEnvironmentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalClusterClient extends AbstractClusterClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalClusterClient.class);
    private LocalClusterManager localClusterManager;
    private ClusterContext clusterContext;
    private final ExecutorService agentService = Executors.newSingleThreadExecutor(
        ThreadUtil.namedThreadFactory(true, "local-agent"));

    @Override
    public void init(IEnvironmentContext environmentContext) {
        super.init(environmentContext);
        clusterContext = new ClusterContext(config);
        localClusterManager = new LocalClusterManager();
        localClusterManager.init(clusterContext);
    }

    @Override
    public IPipelineClient startCluster() {
        try {
            LocalClusterId clusterId = localClusterManager.startMaster();
            ClusterInfo clusterInfo = LocalClient.initMaster(clusterId.getMaster());
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
        if (localClusterManager != null) {
            localClusterManager.close();
        }
    }

}
