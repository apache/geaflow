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

package org.apache.geaflow.cluster.runner.manager;

import static org.apache.geaflow.cluster.constants.ClusterConstants.AUTO_RESTART;
import static org.apache.geaflow.cluster.constants.ClusterConstants.CONTAINER_ID;
import static org.apache.geaflow.cluster.constants.ClusterConstants.CONTAINER_INDEX;
import static org.apache.geaflow.cluster.constants.ClusterConstants.CONTAINER_START_COMMAND;
import static org.apache.geaflow.cluster.constants.ClusterConstants.IS_RECOVER;
import static org.apache.geaflow.cluster.constants.ClusterConstants.JOB_CONFIG;
import static org.apache.geaflow.cluster.constants.ClusterConstants.MASTER_ID;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.FO_STRATEGY;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.LOG_DIR;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SUPERVISOR_ENABLE;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import org.apache.geaflow.cluster.clustermanager.AbstractClusterManager;
import org.apache.geaflow.cluster.clustermanager.ClusterContext;
import org.apache.geaflow.cluster.failover.FailoverStrategyFactory;
import org.apache.geaflow.cluster.failover.IFailoverStrategy;
import org.apache.geaflow.cluster.rpc.RpcClient;
import org.apache.geaflow.cluster.runner.entrypoint.ContainerRunner;
import org.apache.geaflow.cluster.runner.entrypoint.DriverRunner;
import org.apache.geaflow.cluster.runner.failover.AbstractFailoverStrategy;
import org.apache.geaflow.cluster.runner.util.ClusterUtils;
import org.apache.geaflow.env.IEnvironment.EnvType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class GeaFlowClusterManager extends AbstractClusterManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeaFlowClusterManager.class);
    protected final String classpath;
    protected boolean enableSupervisor;
    protected EnvType envType;
    protected String configValue;
    protected boolean failFast;
    protected String logDir;

    public GeaFlowClusterManager(EnvType envType) {
        this.envType = envType;
        this.classpath = System.getProperty("java.class.path");
    }

    @Override
    public void init(ClusterContext clusterContext) {
        super.init(clusterContext);
        this.config = clusterContext.getConfig();
        this.configValue = ClusterUtils.convertConfigToString(clusterContext.getConfig());
        this.enableSupervisor = clusterContext.getConfig().getBoolean(SUPERVISOR_ENABLE);
        this.logDir = clusterContext.getConfig().getString(LOG_DIR);
    }

    @Override
    protected IFailoverStrategy buildFailoverStrategy() {
        IFailoverStrategy foStrategy = FailoverStrategyFactory.loadFailoverStrategy(envType,
            clusterConfig.getConfig().getString(FO_STRATEGY));
        foStrategy.init(clusterContext);
        if (foStrategy instanceof AbstractFailoverStrategy) {
            ((AbstractFailoverStrategy) foStrategy).setClusterManager(this);
        }
        return foStrategy;
    }

    @Override
    public void restartAllDrivers() {
        Map<Integer, String> driverIds = clusterContext.getDriverIds();
        LOGGER.info("Restart all drivers: {}", driverIds);
        if (enableSupervisor) {
            restartContainersBySupervisor(driverIds, true);
        } else {
            for (Map.Entry<Integer, String> entry : driverIds.entrySet()) {
                restartDriver(entry.getKey());
            }
        }
    }

    @Override
    public void restartAllContainers() {
        Map<Integer, String> containerIds = clusterContext.getContainerIds();
        LOGGER.info("Restart all containers: {}", containerIds);
        if (enableSupervisor) {
            restartContainersBySupervisor(containerIds, false);
        } else {
            for (Map.Entry<Integer, String> entry : containerIds.entrySet()) {
                restartContainer(entry.getKey());
            }
        }
    }

    protected void restartContainersBySupervisor(Map<Integer, String> containerIds,
                                                 boolean isDriver) {
        List<Future> futures = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : containerIds.entrySet()) {
            futures.add(
                RpcClient.getInstance().restartWorkerBySupervisor(entry.getValue(), failFast));
        }
        Iterator<Entry<Integer, String>> iterator = containerIds.entrySet().iterator();
        List<Integer> lostWorkers = new ArrayList<>();
        for (Future future : futures) {
            Entry<Integer, String> entry = iterator.next();
            try {
                future.get();
            } catch (Throwable e) {
                LOGGER.warn("catch exception from {}: {} {}", entry.getValue(),
                    e.getClass().getCanonicalName(), e.getMessage());
                lostWorkers.add(entry.getKey());
            }
        }
        if (isDriver) {
            LOGGER.info("Restart lost drivers: {}", lostWorkers);
            for (Integer id : lostWorkers) {
                restartDriver(id);
            }
        } else {
            LOGGER.info("Restart lost containers: {}", lostWorkers);
            for (Integer id : lostWorkers) {
                restartContainer(id);
            }
        }
    }

    public String getDriverShellCommand(int driverId, int driverIndex,
                                        String logFile) {
        Map<String, String> extraOptions = buildExtraOptions(driverId);
        extraOptions.put(CONTAINER_INDEX, String.valueOf(driverIndex));

        String logFilename = logDir + File.separator + logFile;
        return ClusterUtils.getStartCommand(clusterConfig.getDriverJvmOptions(), DriverRunner.class,
            logFilename, clusterConfig.getConfig(), extraOptions, classpath, false);
    }

    public String getContainerShellCommand(int containerId, boolean isRecover,
                                           String logFile) {
        Map<String, String> extraOptions = buildExtraOptions(containerId);
        extraOptions.put(IS_RECOVER, String.valueOf(isRecover));

        String logFilename = logDir + File.separator + logFile;
        return ClusterUtils.getStartCommand(clusterConfig.getContainerJvmOptions(),
            ContainerRunner.class, logFilename, clusterConfig.getConfig(), extraOptions,
            classpath, false);
    }

    protected Map<String, String> buildExtraOptions(int containerId) {
        Map<String, String> env = new HashMap<>();
        env.put(MASTER_ID, masterId);
        env.put(CONTAINER_ID, String.valueOf(containerId));
        env.put(JOB_CONFIG, configValue);
        return env;
    }

    protected Map<String, String> buildSupervisorEnvs(int containerId, String startCommand,
                                                      String autoRestart) {
        Map<String, String> env = new HashMap<>();
        env.put(AUTO_RESTART, autoRestart);
        env.put(CONTAINER_ID, String.valueOf(containerId));
        env.put(CONTAINER_START_COMMAND, startCommand);
        return env;
    }

}
