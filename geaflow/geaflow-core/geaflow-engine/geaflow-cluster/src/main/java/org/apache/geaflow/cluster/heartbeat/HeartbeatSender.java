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

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.HEARTBEAT_INITIAL_DELAY_MS;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.HEARTBEAT_INTERVAL_MS;

import java.io.Serializable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.geaflow.cluster.rpc.RpcClient;
import org.apache.geaflow.cluster.rpc.RpcEndpointRef.RpcCallback;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.heartbeat.Heartbeat;
import org.apache.geaflow.common.utils.ExecutorUtil;
import org.apache.geaflow.common.utils.ThreadUtil;
import org.apache.geaflow.rpc.proto.Master.HeartbeatResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatSender implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatSender.class);

    private final String masterId;
    private final ScheduledExecutorService scheduledService;
    private final Supplier<Heartbeat> heartbeatTrigger;
    private final HeartbeatClient heartbeatClient;
    private final long initialDelayMs;
    private final long intervalMs;
    private ScheduledFuture scheduledFuture;

    public HeartbeatSender(String masterId, Supplier<Heartbeat> heartbeatTrigger,
                           Configuration config, HeartbeatClient heartbeatClient) {
        this.masterId = masterId;
        this.heartbeatTrigger = heartbeatTrigger;
        this.scheduledService = new ScheduledThreadPoolExecutor(1,
            ThreadUtil.namedThreadFactory(true, "heartbeat-sender"));
        this.heartbeatClient = heartbeatClient;
        this.initialDelayMs = config.getInteger(HEARTBEAT_INITIAL_DELAY_MS);
        this.intervalMs = config.getInteger(HEARTBEAT_INTERVAL_MS);
    }

    public void start() {
        scheduledFuture = scheduledService.scheduleWithFixedDelay(() -> {
            Heartbeat message = null;
            try {
                message = heartbeatTrigger.get();
                if (message != null) {
                    RpcClient.getInstance().sendHeartBeat(masterId, message, new RpcCallback<HeartbeatResponse>() {

                        @Override
                        public void onSuccess(HeartbeatResponse event) {
                            if (!event.getRegistered()) {
                                LOGGER.warn("Heartbeat is not registered.");
                                heartbeatClient.registerToMaster();
                            }
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            LOGGER.error("Send heartbeat failed.", t);
                        }
                    });
                }
            } catch (Throwable e) {
                LOGGER.error("send heartbeat {} failed", message, e);
            }
        }, initialDelayMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void close() {
        LOGGER.info("Close heartbeat sender");
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
        if (scheduledService != null) {
            ExecutorUtil.shutdown(scheduledService);
        }
    }

}
