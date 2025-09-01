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

package org.apache.geaflow.cluster.heartbeat;

import java.io.Serializable;
import org.apache.geaflow.cluster.common.ComponentInfo;
import org.apache.geaflow.cluster.rpc.RpcClient;
import org.apache.geaflow.cluster.rpc.RpcEndpointRef.RpcCallback;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.heartbeat.Heartbeat;
import org.apache.geaflow.rpc.proto.Master.RegisterResponse;
import org.apache.geaflow.stats.collector.ProcessStatsCollector;
import org.apache.geaflow.stats.collector.StatsCollectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatClient<T extends ComponentInfo> implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatClient.class);

    private final int containerId;
    private final String containerName;
    private final Configuration config;
    private HeartbeatSender heartbeatSender;
    private final ProcessStatsCollector statsCollector;
    private T info;
    private String masterId;

    public HeartbeatClient(int containerId, String containerName, Configuration config) {
        this.containerId = containerId;
        this.containerName = containerName;
        this.config = config;
        this.statsCollector = StatsCollectorFactory.getInstance().getProcessStatsCollector();
    }

    public void init(String masterId, T info) {
        this.masterId = masterId;
        this.info = info;
        registerToMaster();
        startHeartBeat(masterId);
    }

    public void registerToMaster() {
        LOGGER.info("register: {}", info);
        RpcClient.init(config);
        doRegister(masterId, info);
    }

    private void doRegister(String masterId, T info) {
        RpcClient.getInstance().registerContainer(masterId, info, new RpcCallback<RegisterResponse>() {

            @Override
            public void onSuccess(RegisterResponse event) {
                LOGGER.info("{} registered success:{}", containerName, event.getSuccess());
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.error("register info failed", t);
            }
        });
    }

    public void startHeartBeat(String masterId) {
        LOGGER.info("start {} heartbeat", containerName);
        this.heartbeatSender = new HeartbeatSender(masterId, () -> {
            Heartbeat heartbeat = null;
            if (containerName != null) {
                heartbeat = new Heartbeat(containerId);
                heartbeat.setContainerName(containerName);
                heartbeat.setProcessMetrics(statsCollector.collect());
            }
            return heartbeat;
        }, config, this);

        this.heartbeatSender.start();
    }

    public void close() {
        if (heartbeatSender != null) {
            heartbeatSender.close();
        }
    }

}
