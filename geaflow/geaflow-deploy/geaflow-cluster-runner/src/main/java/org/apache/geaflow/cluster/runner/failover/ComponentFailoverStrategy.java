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

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.PROCESS_AUTO_RESTART;

import org.apache.geaflow.cluster.clustermanager.ClusterContext;
import org.apache.geaflow.cluster.failover.FailoverStrategyType;
import org.apache.geaflow.env.IEnvironment.EnvType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This strategy is to restart the process by supervisor but not master.
 */
public class ComponentFailoverStrategy extends AbstractFailoverStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(ComponentFailoverStrategy.class);

    public ComponentFailoverStrategy(EnvType envType) {
        super(envType);
    }

    @Override
    public void init(ClusterContext context) {
        super.init(context);
        context.getConfig().put(PROCESS_AUTO_RESTART, AutoRestartPolicy.UNEXPECTED.getValue());
        LOGGER.info("init with foRestarts: {}", context.getClusterConfig().getMaxRestarts());
    }

    @Override
    public void doFailover(int componentId, Throwable cause) {
    }

    @Override
    public FailoverStrategyType getType() {
        return FailoverStrategyType.component_fo;
    }

}
