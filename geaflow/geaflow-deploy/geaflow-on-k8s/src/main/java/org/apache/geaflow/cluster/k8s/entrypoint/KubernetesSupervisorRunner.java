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

package org.apache.geaflow.cluster.k8s.entrypoint;

import static org.apache.geaflow.cluster.constants.ClusterConstants.CONTAINER_START_COMMAND;

import java.util.Map;
import org.apache.geaflow.cluster.k8s.config.K8SConstants;
import org.apache.geaflow.cluster.k8s.utils.KubernetesUtils;
import org.apache.geaflow.cluster.runner.Supervisor;
import org.apache.geaflow.cluster.runner.util.ClusterUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.ProcessUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesSupervisorRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesSupervisorRunner.class);
    private static final Map<String, String> ENV = System.getenv();
    private final Supervisor supervisor;

    public KubernetesSupervisorRunner(Configuration configuration, String startCommand,
                                      boolean autoRestart) {
        this.supervisor = new Supervisor(startCommand, configuration, autoRestart);
    }

    public void run() {
        supervisor.start();
    }

    private void waitForTermination() {
        LOGGER.info("Waiting for supervisor exit.");
        supervisor.waitForTermination();
    }

    public static void main(String[] args) throws Exception {
        try {
            String id = ClusterUtils.getEnvValue(ENV, K8SConstants.ENV_CONTAINER_ID);
            String autoRestartEnv = ClusterUtils.getEnvValue(ENV, K8SConstants.ENV_AUTO_RESTART);
            LOGGER.info("Start supervisor with ID: {} pid: {} autoStart:{}", id,
                ProcessUtil.getProcessId(), autoRestartEnv);

            Configuration config = KubernetesUtils.loadConfiguration();
            String startCommand = ClusterUtils.getEnvValue(ENV, CONTAINER_START_COMMAND);
            boolean autoRestart = !autoRestartEnv.equalsIgnoreCase(Boolean.FALSE.toString());
            KubernetesSupervisorRunner workerRunner = new KubernetesSupervisorRunner(config,
                startCommand, autoRestart);
            workerRunner.run();
            workerRunner.waitForTermination();
            LOGGER.info("Exit worker process");
        } catch (Throwable e) {
            LOGGER.error("FATAL: process exits", e);
            throw e;
        }
    }

}
