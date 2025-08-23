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

import static org.apache.geaflow.cluster.constants.ClusterConstants.EXIT_CODE;

import java.util.Map;
import org.apache.geaflow.cluster.driver.Driver;
import org.apache.geaflow.cluster.driver.DriverContext;
import org.apache.geaflow.cluster.k8s.config.K8SConstants;
import org.apache.geaflow.cluster.k8s.config.KubernetesDriverParam;
import org.apache.geaflow.cluster.k8s.utils.KubernetesUtils;
import org.apache.geaflow.cluster.runner.util.ClusterUtils;
import org.apache.geaflow.common.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is an adaptation of Flink's org.apache.flink.kubernetes.taskmanager.KubernetesTaskExecutorRunner.
 */
public class KubernetesDriverRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesDriverRunner.class);

    /**
     * The process environment variables.
     */
    private static final Map<String, String> ENV = System.getenv();

    private final int driverId;
    private final int driverIndex;
    private final Configuration config;
    private Driver driver;

    public KubernetesDriverRunner(int driverId, int driverIndex, Configuration config) {
        this.driverId = driverId;
        this.driverIndex = driverIndex;
        this.config = config;
    }

    public void run() {
        KubernetesDriverParam driverParam = new KubernetesDriverParam(config);
        DriverContext driverContext = new DriverContext(driverId, driverIndex, config);
        driver = new Driver(driverParam.getRpcPort());
        driverContext.load();
        driver.init(driverContext);
    }

    private void waitForTermination() {
        LOGGER.info("wait for service terminating");
        driver.waitTermination();
    }

    public static void main(String[] args) throws Exception {
        try {
            final long startTime = System.currentTimeMillis();
            String id = ClusterUtils.getEnvValue(ENV, K8SConstants.ENV_CONTAINER_ID);
            String index = ClusterUtils.getEnvValue(ENV, K8SConstants.ENV_CONTAINER_INDEX);
            String masterId = ClusterUtils.getEnvValue(ENV, K8SConstants.ENV_MASTER_ID);
            LOGGER.info("ResourceID assigned for this driver id:{} index:{} masterId:{}", id,
                index, masterId);

            Configuration config = KubernetesUtils.loadConfiguration();
            config.setMasterId(masterId);
            KubernetesDriverRunner kubernetesDriverRunner =
                new KubernetesDriverRunner(Integer.parseInt(id), Integer.parseInt(index), config);
            kubernetesDriverRunner.run();
            LOGGER.info("Completed driver init in {} ms", System.currentTimeMillis() - startTime);
            kubernetesDriverRunner.waitForTermination();
        } catch (Throwable e) {
            LOGGER.error("FATAL: process exits", e);
            System.exit(EXIT_CODE);
        }
    }

}
