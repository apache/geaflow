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

import java.io.Serializable;
import org.apache.geaflow.cluster.clustermanager.ClusterInfo;
import org.apache.geaflow.cluster.config.ClusterConfig;
import org.apache.geaflow.cluster.container.ContainerContext;
import org.apache.geaflow.cluster.driver.DriverContext;
import org.apache.geaflow.cluster.local.entrypoint.LocalContainerRunner;
import org.apache.geaflow.cluster.local.entrypoint.LocalDriverRunner;
import org.apache.geaflow.cluster.local.entrypoint.LocalMasterRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LocalClient implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalClient.class);

    public static LocalMasterRunner createMaster(ClusterConfig clusterConfig) {
        return new LocalMasterRunner(clusterConfig.getConfig());
    }

    public static ClusterInfo initMaster(LocalMasterRunner master) {
        LOGGER.info("init master");
        return master.init();
    }

    public static LocalDriverRunner createDriver(ClusterConfig clusterConfig,
                                                 DriverContext context) {
        return new LocalDriverRunner(context);
    }

    public static LocalContainerRunner createContainer(ClusterConfig clusterConfig,
                                                       ContainerContext containerContext) {
        return new LocalContainerRunner(containerContext);
    }

}
