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

import static org.apache.geaflow.cluster.constants.ClusterConstants.ENV_AGENT_PORT;
import static org.apache.geaflow.cluster.constants.ClusterConstants.ENV_SUPERVISOR_PORT;
import static org.apache.geaflow.cluster.constants.ClusterConstants.EXIT_CODE;
import static org.apache.geaflow.cluster.constants.ClusterConstants.MASTER_ID;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.AGENT_HTTP_PORT;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SUPERVISOR_RPC_PORT;

import org.apache.geaflow.cluster.common.AbstractContainer;
import org.apache.geaflow.cluster.constants.ClusterConstants;
import org.apache.geaflow.cluster.container.Container;
import org.apache.geaflow.cluster.container.ContainerContext;
import org.apache.geaflow.cluster.container.IContainer;
import org.apache.geaflow.cluster.runner.util.ClusterUtils;
import org.apache.geaflow.cluster.runner.util.RunnerRuntimeHook;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.ProcessUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ContainerRunner.class);
    private final ContainerContext containerContext;
    private IContainer container;

    public ContainerRunner(ContainerContext containerContext) {
        this.containerContext = containerContext;
    }

    public void run() {
        container = new Container();
        containerContext.load();
        container.init(containerContext);
    }

    private void waitForTermination() {
        LOGGER.info("wait for service terminating");
        ((AbstractContainer) container).waitTermination();
    }

    public void close() {
        if (container != null) {
            container.close();
        }
    }

    public static void main(String[] args) throws Exception {
        ContainerRunner containerRunner = null;
        try {
            final long startTime = System.currentTimeMillis();

            String id = ClusterUtils.getProperty(ClusterConstants.CONTAINER_ID);
            String masterId = ClusterUtils.getProperty(MASTER_ID);
            LOGGER.info("ResourceID assigned for this container:{} masterId:{}", id, masterId);

            Configuration config = ClusterUtils.loadConfiguration();
            config.setMasterId(masterId);
            String supervisorPort = ClusterUtils.getEnvValue(System.getenv(), ENV_SUPERVISOR_PORT);
            config.put(SUPERVISOR_RPC_PORT, supervisorPort);
            String agentPort = ClusterUtils.getEnvValue(System.getenv(), ENV_AGENT_PORT);
            config.put(AGENT_HTTP_PORT, agentPort);
            LOGGER.info("Supervisor rpc port: {} agentPort: {}", supervisorPort, agentPort);

            new RunnerRuntimeHook(ContainerRunner.class.getSimpleName(),
                Integer.parseInt(supervisorPort)).start();

            ContainerContext context = new ContainerContext(Integer.parseInt(id), config);
            containerRunner = new ContainerRunner(context);
            containerRunner.run();
            LOGGER.info("Completed container init in {}ms", System.currentTimeMillis() - startTime);
            containerRunner.waitForTermination();
        } catch (Throwable e) {
            LOGGER.error("FATAL: container {} exits", ProcessUtil.getProcessId(), e);
            if (containerRunner != null) {
                containerRunner.close();
            }
            System.exit(EXIT_CODE);
        }
    }

}
