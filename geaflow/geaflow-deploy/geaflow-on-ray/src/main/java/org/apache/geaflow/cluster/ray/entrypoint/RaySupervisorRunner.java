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

package org.apache.geaflow.cluster.ray.entrypoint;

import static org.apache.geaflow.cluster.constants.ClusterConstants.AUTO_RESTART;
import static org.apache.geaflow.cluster.constants.ClusterConstants.CONTAINER_ID;
import static org.apache.geaflow.cluster.constants.ClusterConstants.CONTAINER_START_COMMAND;

import java.util.Map;
import org.apache.geaflow.cluster.runner.Supervisor;
import org.apache.geaflow.cluster.runner.util.ClusterUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.ProcessUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaySupervisorRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaySupervisorRunner.class);

    public RaySupervisorRunner(Configuration configuration, Map<String, String> env) {
        String id = ClusterUtils.getEnvValue(env, CONTAINER_ID);
        String autoRestartEnv = ClusterUtils.getEnvValue(env, AUTO_RESTART);
        LOGGER.info("Start supervisor with ID: {} pid: {} autoStart: {}", id,
            ProcessUtil.getProcessId(), autoRestartEnv);

        String startCommand = ClusterUtils.getEnvValue(env, CONTAINER_START_COMMAND);
        boolean autoRestart = !autoRestartEnv.equalsIgnoreCase(Boolean.FALSE.toString());
        Supervisor supervisor = new Supervisor(startCommand, configuration, autoRestart);
        supervisor.start();
    }

}
