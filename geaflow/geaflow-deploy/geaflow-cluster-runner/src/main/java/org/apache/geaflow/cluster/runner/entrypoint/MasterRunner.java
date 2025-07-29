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

package org.apache.geaflow.cluster.runner.entrypoint;

import org.apache.geaflow.cluster.clustermanager.ClusterInfo;
import org.apache.geaflow.cluster.clustermanager.IClusterManager;
import org.apache.geaflow.cluster.master.AbstractMaster;
import org.apache.geaflow.cluster.master.MasterContext;
import org.apache.geaflow.cluster.master.MasterFactory;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(MasterRunner.class);

    protected final Configuration config;
    protected final AbstractMaster master;

    public MasterRunner(Configuration config, IClusterManager clusterManager) {
        this.config = config;
        if (config.getBoolean(ExecutionConfigKeys.ENABLE_MASTER_LEADER_ELECTION)) {
            initLeaderElectionService();
        }

        MasterContext context = new MasterContext(config);
        context.setClusterManager(clusterManager);
        context.load();

        master = MasterFactory.create(config);
        master.init(context);
    }

    protected void initLeaderElectionService() {
    }

    public ClusterInfo init() {
        try {
            return master.startCluster();
        } catch (Throwable e) {
            LOGGER.error("init failed", e);
            throw e;
        }
    }

    protected void waitForTermination() {
        LOGGER.info("waiting for finishing...");
        master.waitTermination();
    }

}
