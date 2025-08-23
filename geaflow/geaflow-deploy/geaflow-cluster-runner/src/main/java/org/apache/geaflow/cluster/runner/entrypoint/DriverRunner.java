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

import static org.apache.geaflow.cluster.constants.ClusterConstants.CONTAINER_ID;
import static org.apache.geaflow.cluster.constants.ClusterConstants.CONTAINER_INDEX;
import static org.apache.geaflow.cluster.constants.ClusterConstants.ENV_AGENT_PORT;
import static org.apache.geaflow.cluster.constants.ClusterConstants.ENV_SUPERVISOR_PORT;
import static org.apache.geaflow.cluster.constants.ClusterConstants.EXIT_CODE;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.AGENT_HTTP_PORT;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.DRIVER_RPC_PORT;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SUPERVISOR_RPC_PORT;

import org.apache.geaflow.cluster.constants.ClusterConstants;
import org.apache.geaflow.cluster.driver.Driver;
import org.apache.geaflow.cluster.driver.DriverContext;
import org.apache.geaflow.cluster.runner.util.ClusterUtils;
import org.apache.geaflow.cluster.runner.util.RunnerRuntimeHook;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.ProcessUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(DriverRunner.class);

    private final DriverContext driverContext;
    private Driver driver;

    public DriverRunner(DriverContext driverContext) {
        this.driverContext = driverContext;
    }

    public void run() {
        int rpcPort = driverContext.getConfig().getInteger(DRIVER_RPC_PORT);
        driver = new Driver(rpcPort);
        driverContext.load();
        driver.init(driverContext);
    }

    private void waitForTermination() {
        LOGGER.info("wait for service terminating");
        driver.waitTermination();
    }

    public void close() {
        if (driver != null) {
            driver.close();
        }
    }

    public static void main(String[] args) throws Exception {
        DriverRunner driverRunner = null;
        try {
            final long startTime = System.currentTimeMillis();

            String id = ClusterUtils.getProperty(CONTAINER_ID);
            String index = ClusterUtils.getProperty(CONTAINER_INDEX);
            String masterId = ClusterUtils.getProperty(ClusterConstants.MASTER_ID);
            LOGGER.info("ResourceID assigned for this driver id:{} index:{} masterId:{}", id, index, masterId);

            Configuration config = ClusterUtils.loadConfiguration();
            config.setMasterId(masterId);
            String supervisorPort = ClusterUtils.getEnvValue(System.getenv(), ENV_SUPERVISOR_PORT);
            config.put(SUPERVISOR_RPC_PORT, supervisorPort);
            String agentPort = ClusterUtils.getEnvValue(System.getenv(), ENV_AGENT_PORT);
            config.put(AGENT_HTTP_PORT, agentPort);
            LOGGER.info("Supervisor rpc port: {} agentPort: {}", supervisorPort, agentPort);

            new RunnerRuntimeHook(DriverRunner.class.getSimpleName(),
                Integer.parseInt(supervisorPort)).start();

            DriverContext context = new DriverContext(Integer.parseInt(id),
                Integer.parseInt(index), config);
            driverRunner = new DriverRunner(context);
            driverRunner.run();
            LOGGER.info("Completed driver init in {} ms", System.currentTimeMillis() - startTime);
            driverRunner.waitForTermination();
        } catch (Throwable e) {
            LOGGER.error("FATAL: driver {} exits", ProcessUtil.getProcessId(), e);
            if (driverRunner != null) {
                driverRunner.close();
            }
            System.exit(EXIT_CODE);
        }
    }

}
