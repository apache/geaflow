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

import static org.apache.geaflow.cluster.constants.ClusterConstants.AGENT_PROFILER_PATH;
import static org.apache.geaflow.cluster.constants.ClusterConstants.DEFAULT_MASTER_ID;
import static org.apache.geaflow.cluster.constants.ClusterConstants.EXIT_CODE;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CLUSTER_ID;

import io.fabric8.kubernetes.api.model.ConfigMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.cluster.clustermanager.ClusterInfo;
import org.apache.geaflow.cluster.k8s.clustermanager.GeaflowKubeClient;
import org.apache.geaflow.cluster.k8s.clustermanager.KubernetesClusterManager;
import org.apache.geaflow.cluster.k8s.clustermanager.KubernetesResourceBuilder;
import org.apache.geaflow.cluster.k8s.config.K8SConstants;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfig;
import org.apache.geaflow.cluster.k8s.config.KubernetesMasterParam;
import org.apache.geaflow.cluster.k8s.utils.KubernetesUtils;
import org.apache.geaflow.cluster.k8s.watcher.KubernetesPodWatcher;
import org.apache.geaflow.cluster.rpc.ConnectAddress;
import org.apache.geaflow.cluster.runner.entrypoint.MasterRunner;
import org.apache.geaflow.cluster.runner.util.ClusterUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.ha.leaderelection.ILeaderContender;
import org.apache.geaflow.ha.leaderelection.LeaderContenderType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* This file is based on source code from the Flink Project (http://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is an adaptation of Flink's
 * org.apache.flink.kubernetes.taskmanager.KubernetesTaskExecutorRunner.
 */
public class KubernetesMasterRunner extends MasterRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesMasterRunner.class);
    private static final Map<String, String> ENV = System.getenv();

    public KubernetesMasterRunner(Configuration config) {
        super(config, new KubernetesClusterManager());
    }

    @Override
    public ClusterInfo init() {
        startClusterWatcher();

        ClusterInfo clusterInfo = super.init();
        updateConfigMap(clusterInfo);
        return clusterInfo;
    }

    protected void startClusterWatcher() {
        KubernetesPodWatcher watcher = new KubernetesPodWatcher(config);
        watcher.start();
    }

    @Override
    protected void initLeaderElectionService() {
        master.initLeaderElectionService(new KubernetesMasterLeaderContender(), config,
            DEFAULT_MASTER_ID);
        try {
            master.waitForLeaderElection();
        } catch (InterruptedException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    private class KubernetesMasterLeaderContender implements ILeaderContender {

        @Override
        public void handleLeadershipGranted() {
            LOGGER.info("Leadership granted, init master now.");
            master.notifyLeaderElection();
        }

        @Override
        public void handleLeadershipLost() {
            LOGGER.info("Leadership lost, exit the process now.");
            System.exit(EXIT_CODE);
        }

        @Override
        public LeaderContenderType getType() {
            return LeaderContenderType.master;
        }
    }

    private void updateConfigMap(ClusterInfo clusterInfo) {
        Map<String, String> updatedConfig = new HashMap<>();
        ConnectAddress masterAddress = clusterInfo.getMasterAddress();
        updatedConfig.put(KubernetesConfig.MASTER_EXPOSED_ADDRESS, masterAddress.toString());

        Map<String, ConnectAddress> driverAddresses = clusterInfo.getDriverAddresses();
        updatedConfig.put(KubernetesConfig.DRIVER_EXPOSED_ADDRESS,
            KubernetesUtils.encodeRpcAddressMap(driverAddresses));

        GeaflowKubeClient client = new GeaflowKubeClient(config);
        String clusterId = config.getString(CLUSTER_ID);

        KubernetesMasterParam masterParam = new KubernetesMasterParam(config);
        ConfigMap configMap = client.getConfigMap(masterParam.getConfigMapName(clusterId));
        ConfigMap updatedConfigMap = KubernetesResourceBuilder.updateConfigMap(configMap,
            updatedConfig);
        client.createOrReplaceConfigMap(updatedConfigMap);
        LOGGER.info("updated master configmap: {}", config);
    }

    public static void main(String[] args) throws Exception {
        try {
            final long startTime = System.currentTimeMillis();
            Configuration config = KubernetesUtils.loadConfiguration();
            String masterId = ClusterUtils.getEnvValue(ENV, K8SConstants.ENV_MASTER_ID);
            config.setMasterId(masterId);
            String profilerPath = ClusterUtils.getEnvValue(ENV, K8SConstants.ENV_PROFILER_PATH);
            config.put(AGENT_PROFILER_PATH, profilerPath);

            KubernetesMasterRunner masterRunner = new KubernetesMasterRunner(config);
            masterRunner.init();
            LOGGER.info("Completed master init in {} ms", System.currentTimeMillis() - startTime);
            masterRunner.waitForTermination();
        } catch (Throwable e) {
            LOGGER.error("FATAL: process exits", e);
            System.exit(EXIT_CODE);
        }
    }

}
