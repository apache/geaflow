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

package org.apache.geaflow.cluster.local.clustermanager;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.cluster.clustermanager.AbstractClusterManager;
import org.apache.geaflow.cluster.clustermanager.ClusterContext;
import org.apache.geaflow.cluster.container.ContainerContext;
import org.apache.geaflow.cluster.driver.DriverContext;
import org.apache.geaflow.cluster.failover.FailoverStrategyFactory;
import org.apache.geaflow.cluster.failover.FailoverStrategyType;
import org.apache.geaflow.cluster.failover.IFailoverStrategy;
import org.apache.geaflow.cluster.local.context.LocalContainerContext;
import org.apache.geaflow.cluster.local.context.LocalDriverContext;
import org.apache.geaflow.env.IEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalClusterManager extends AbstractClusterManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalClusterManager.class);
    private String appPath;

    @Override
    public void init(ClusterContext clusterContext) {
        super.init(clusterContext);
        this.appPath = getWorkPath();
    }

    @Override
    protected IFailoverStrategy buildFailoverStrategy() {
        return FailoverStrategyFactory.loadFailoverStrategy(IEnvironment.EnvType.LOCAL,
            FailoverStrategyType.disable_fo.name());
    }

    @Override
    public LocalClusterId startMaster() {
        Preconditions.checkArgument(clusterConfig != null, "clusterConfig is not initialized");
        clusterInfo = new LocalClusterId(LocalClient.createMaster(clusterConfig));
        return (LocalClusterId) clusterInfo;
    }

    @Override
    public void createNewContainer(int containerId, boolean isRecover) {
        ContainerContext containerContext = new LocalContainerContext(containerId,
            clusterConfig.getConfig());
        LocalClient.createContainer(clusterConfig, containerContext);
    }

    @Override
    public void createNewDriver(int driverId, int driverIndex) {
        DriverContext driverContext = new LocalDriverContext(driverId, driverIndex,
            clusterConfig.getConfig());
        LocalClient.createDriver(clusterConfig, driverContext);
        LOGGER.info("call driver start id:{} index:{}", driverId, driverIndex);
    }


    @Override
    public void restartDriver(int driverId) {
    }

    @Override
    public void restartContainer(int containerId) {
    }

    @Override
    protected void validateContainerNum(int containerNum) {
        Preconditions.checkArgument(containerNum == 1, "local mode containerNum must equal with 1");
    }

    @Override
    public void close() {
        super.close();
        if (appPath != null) {
            FileUtils.deleteQuietly(new File(appPath));
        }
    }

    private String getWorkPath() {
        String workPath = "/tmp/" + System.currentTimeMillis();
        try {
            FileUtils.forceMkdir(new File(workPath));
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return workPath;
    }

}
