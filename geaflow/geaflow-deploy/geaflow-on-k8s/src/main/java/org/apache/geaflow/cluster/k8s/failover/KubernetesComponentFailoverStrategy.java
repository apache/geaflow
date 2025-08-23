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

package org.apache.geaflow.cluster.k8s.failover;

import static org.apache.geaflow.cluster.constants.ClusterConstants.DEFAULT_MASTER_ID;

import org.apache.geaflow.cluster.clustermanager.ClusterContext;
import org.apache.geaflow.cluster.runner.failover.ComponentFailoverStrategy;
import org.apache.geaflow.common.exception.GeaflowHeartbeatException;
import org.apache.geaflow.env.IEnvironment.EnvType;
import org.apache.geaflow.stats.model.EventLabel;
import org.apache.geaflow.stats.model.ExceptionLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesComponentFailoverStrategy extends ComponentFailoverStrategy {
    private static final Logger LOGGER =
        LoggerFactory.getLogger(KubernetesComponentFailoverStrategy.class);

    protected ClusterContext clusterContext;

    public KubernetesComponentFailoverStrategy() {
        super(EnvType.K8S);
    }

    public void init(ClusterContext clusterContext) {
        super.init(clusterContext);
        this.clusterContext = clusterContext;
    }

    @Override
    public void doFailover(int componentId, Throwable cause) {
        if (componentId != DEFAULT_MASTER_ID) {
            if (cause instanceof GeaflowHeartbeatException) {
                String startMessage = String.format("Start component failover for component #%s "
                    + "cause by %s.", componentId, cause.getMessage());
                LOGGER.info(startMessage);
                reportFailoverEvent(ExceptionLevel.ERROR, EventLabel.FAILOVER_START, startMessage);

                long startTime = System.currentTimeMillis();
                if (clusterContext.getDriverIds().containsKey(componentId)) {
                    clusterManager.restartDriver(componentId);
                } else {
                    clusterManager.restartContainer(componentId);
                }

                String finishMessage = String.format("Completed component failover for component "
                    + "#%s in %s ms.", componentId, System.currentTimeMillis() - startTime);
                LOGGER.info(finishMessage);
                reportFailoverEvent(ExceptionLevel.INFO, EventLabel.FAILOVER_FINISH, finishMessage);
            } else {
                String reason = cause == null ? null : cause.getMessage();
                LOGGER.warn("{} throws exception: {}", componentId, reason);
            }
        }
    }

}
