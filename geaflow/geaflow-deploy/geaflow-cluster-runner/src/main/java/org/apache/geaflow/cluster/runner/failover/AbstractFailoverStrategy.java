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

package org.apache.geaflow.cluster.runner.failover;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SUPERVISOR_ENABLE;

import org.apache.geaflow.cluster.clustermanager.AbstractClusterManager;
import org.apache.geaflow.cluster.clustermanager.ClusterContext;
import org.apache.geaflow.cluster.clustermanager.IClusterManager;
import org.apache.geaflow.cluster.failover.IFailoverStrategy;
import org.apache.geaflow.env.IEnvironment.EnvType;
import org.apache.geaflow.stats.collector.StatsCollectorFactory;
import org.apache.geaflow.stats.model.EventLabel;
import org.apache.geaflow.stats.model.ExceptionLevel;

public abstract class AbstractFailoverStrategy implements IFailoverStrategy {

    protected EnvType envType;
    protected AbstractClusterManager clusterManager;
    protected boolean enableSupervisor;
    protected ClusterContext context;

    public AbstractFailoverStrategy(EnvType envType) {
        this.envType = envType;
    }

    @Override
    public void init(ClusterContext context) {
        this.context = context;
        this.enableSupervisor = context.getConfig().getBoolean(SUPERVISOR_ENABLE);
    }

    protected void reportFailoverEvent(ExceptionLevel level, EventLabel label, String message) {
        StatsCollectorFactory.init(context.getConfig()).getEventCollector()
            .reportEvent(level, label, message);
    }

    public void setClusterManager(IClusterManager clusterManager) {
        this.clusterManager = (AbstractClusterManager) clusterManager;
    }

    @Override
    public EnvType getEnv() {
        return envType;
    }

}
