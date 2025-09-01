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

package org.apache.geaflow.cluster.master;

import static org.apache.geaflow.cluster.constants.ClusterConstants.DEFAULT_MASTER_ID;

import org.apache.geaflow.cluster.clustermanager.ClusterContext;
import org.apache.geaflow.cluster.clustermanager.IClusterManager;
import org.apache.geaflow.cluster.common.ReliableContainerContext;
import org.apache.geaflow.cluster.constants.ClusterConstants;
import org.apache.geaflow.common.config.Configuration;

public class MasterContext extends ReliableContainerContext {

    private Configuration configuration;
    private IClusterManager clusterManager;
    private ClusterContext clusterContext;

    public MasterContext(Configuration configuration) {
        super(DEFAULT_MASTER_ID, ClusterConstants.getMasterName(), configuration);
        this.configuration = configuration;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public IClusterManager getClusterManager() {
        return clusterManager;
    }

    public void setClusterManager(IClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public ClusterContext getClusterContext() {
        return clusterContext;
    }

    @Override
    public boolean isRecover() {
        return clusterContext.isRecover();
    }

    @Override
    public void load() {
        this.clusterContext = new ClusterContext(configuration);
        this.clusterContext.load();
    }

}
