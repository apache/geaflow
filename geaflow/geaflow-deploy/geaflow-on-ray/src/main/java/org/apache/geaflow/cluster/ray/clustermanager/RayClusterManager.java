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

package org.apache.geaflow.cluster.ray.clustermanager;

import static org.apache.geaflow.cluster.constants.ClusterConstants.AGENT_PROFILER_PATH;
import static org.apache.geaflow.cluster.ray.config.RayConfig.RAY_AGENT_PROFILER_PATH;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.PROCESS_AUTO_RESTART;

import com.google.common.base.Preconditions;
import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.cluster.clustermanager.ClusterContext;
import org.apache.geaflow.cluster.container.ContainerContext;
import org.apache.geaflow.cluster.driver.DriverContext;
import org.apache.geaflow.cluster.ray.context.RayContainerContext;
import org.apache.geaflow.cluster.ray.context.RayDriverContext;
import org.apache.geaflow.cluster.ray.entrypoint.RayContainerRunner;
import org.apache.geaflow.cluster.ray.entrypoint.RayDriverRunner;
import org.apache.geaflow.cluster.ray.entrypoint.RayMasterRunner;
import org.apache.geaflow.cluster.ray.entrypoint.RaySupervisorRunner;
import org.apache.geaflow.cluster.ray.utils.RaySystemFunc;
import org.apache.geaflow.cluster.runner.manager.GeaFlowClusterManager;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.env.IEnvironment.EnvType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RayClusterManager extends GeaFlowClusterManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(RayClusterManager.class);
    private static final int MASTER_ACTOR_ID = 0;
    private static Map<Integer, ActorHandle> actors = new HashMap<>();

    private String autoRestart;
    private String currentJobId;

    public RayClusterManager() {
        super(EnvType.RAY);
    }

    @Override
    public void init(ClusterContext clusterContext) {
        super.init(clusterContext);
        if (clusterContext.getHeartbeatManager() != null) {
            this.failFast = clusterConfig.getMaxRestarts() == 0;
            this.autoRestart = config.getString(PROCESS_AUTO_RESTART);
            this.currentJobId = Ray.getRuntimeContext().getCurrentJobId().toString();
        }
        String profilerPath = config.getString(RAY_AGENT_PROFILER_PATH);
        this.config.put(AGENT_PROFILER_PATH, profilerPath);
    }

    @Override
    public RayClusterId startMaster() {
        Preconditions.checkArgument(clusterConfig != null, "clusterConfig is not initialized");
        ActorHandle<RayMasterRunner> master = RayClient.createMaster(clusterConfig);
        clusterInfo = new RayClusterId(master);
        actors.put(MASTER_ACTOR_ID, master);
        return (RayClusterId) clusterInfo;
    }

    @Override
    public void createNewContainer(int containerId, boolean isRecover) {
        if (enableSupervisor) {
            String logFile = String.format("container-%s-%s.log", currentJobId, containerId);
            String command = getContainerShellCommand(containerId, isRecover, logFile);
            ActorHandle<RaySupervisorRunner> actor = createSupervisor(containerId, command,
                autoRestart);
            actors.put(containerId, actor);
        } else {
            ContainerContext containerContext = new RayContainerContext(containerId,
                clusterConfig.getConfig());
            ActorHandle<RayContainerRunner> container = RayClient.createContainer(clusterConfig, containerContext);
            actors.put(containerId, container);
        }

    }

    @Override
    public void createNewDriver(int driverId, int driverIndex) {
        LOGGER.info("create driver start, enable supervisor:{}", enableSupervisor);
        if (enableSupervisor) {
            String logFile = String.format("driver-%s-%s.log", currentJobId, driverId);
            String command = getDriverShellCommand(driverId, driverIndex, logFile);
            ActorHandle<RaySupervisorRunner> actor = createSupervisor(driverId, command,
                autoRestart);
            actors.put(driverId, actor);
        } else {
            DriverContext driverContext = new RayDriverContext(driverId, driverIndex, clusterConfig.getConfig());
            ActorHandle<RayDriverRunner> driver = RayClient.createDriver(clusterConfig,
                driverContext);
            actors.put(driverId, driver);
        }
        LOGGER.info("call driver start, id:{} index:{}", driverId, driverIndex);
    }

    @Override
    public void restartContainer(int containerId) {
        if (!actors.containsKey(containerId)) {
            throw new GeaflowRuntimeException(String.format("invalid container id %s", containerId));
        }
        actors.get(containerId).kill();
    }

    @Override
    public void restartDriver(int driverId) {
        if (!actors.containsKey(driverId)) {
            throw new GeaflowRuntimeException(String.format("invalid driver id %s", driverId));
        }
        actors.get(driverId).kill();
    }

    private ActorHandle<RaySupervisorRunner> createSupervisor(int containerId, String command, String autoStart) {
        Map<String, String> additionalEnvs = buildSupervisorEnvs(containerId, command, autoStart);
        return RayClient.createSupervisor(clusterConfig, additionalEnvs);
    }

    @Override
    public void close() {
        super.close();
        actors.clear();
        if (RaySystemFunc.isLocalMode()) {
            FileUtils.deleteQuietly(new File(RaySystemFunc.getWorkPath()));
        }
    }

}
