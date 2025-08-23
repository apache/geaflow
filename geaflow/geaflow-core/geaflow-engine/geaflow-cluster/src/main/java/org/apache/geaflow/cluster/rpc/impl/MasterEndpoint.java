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

package org.apache.geaflow.cluster.rpc.impl;

import com.google.protobuf.Empty;
import org.apache.geaflow.cluster.clustermanager.AbstractClusterManager;
import org.apache.geaflow.cluster.clustermanager.IClusterManager;
import org.apache.geaflow.cluster.container.ContainerInfo;
import org.apache.geaflow.cluster.driver.DriverInfo;
import org.apache.geaflow.cluster.heartbeat.HeartbeatManager;
import org.apache.geaflow.cluster.master.IMaster;
import org.apache.geaflow.cluster.rpc.IMasterEndpoint;
import org.apache.geaflow.common.encoder.RpcMessageEncoder;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.heartbeat.Heartbeat;
import org.apache.geaflow.rpc.proto.Master.HeartbeatRequest;
import org.apache.geaflow.rpc.proto.Master.HeartbeatResponse;
import org.apache.geaflow.rpc.proto.Master.RegisterRequest;
import org.apache.geaflow.rpc.proto.Master.RegisterResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterEndpoint implements IMasterEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(MasterEndpoint.class);

    private final IMaster master;
    private final IClusterManager clusterManager;

    public MasterEndpoint(IMaster master, IClusterManager clusterManager) {
        this.master = master;
        this.clusterManager = clusterManager;
    }

    @Override
    public RegisterResponse registerContainer(RegisterRequest request) {
        try {
            ContainerInfo containerInfo = RpcMessageEncoder.decode(request.getPayload());
            return ((AbstractClusterManager) clusterManager).registerContainer(containerInfo);
        } catch (Throwable t) {
            LOGGER.error("register container failed: {}", t.getMessage(), t);
            throw new GeaflowRuntimeException(String.format("register container failed: %s",
                t.getMessage()), t);
        }
    }

    @Override
    public RegisterResponse registerDriver(RegisterRequest request) {
        try {
            DriverInfo driverInfo = RpcMessageEncoder.decode(request.getPayload());
            return ((AbstractClusterManager) clusterManager).registerDriver(driverInfo);
        } catch (Throwable t) {
            LOGGER.error("register driver failed: {}", t.getMessage(), t);
            throw new GeaflowRuntimeException(String.format("register driver failed: %s",
                t.getMessage()), t);
        }
    }

    @Override
    public HeartbeatResponse receiveHeartbeat(HeartbeatRequest request) {
        try {
            Heartbeat heartbeat = new Heartbeat(request.getId());
            heartbeat.setTimestamp(request.getTimestamp());
            heartbeat.setContainerName(RpcMessageEncoder.decode(request.getName()));
            heartbeat.setProcessMetrics(RpcMessageEncoder.decode(request.getPayload()));
            HeartbeatManager heartbeatManager =
                ((AbstractClusterManager) clusterManager).getClusterContext().getHeartbeatManager();
            return heartbeatManager.receivedHeartbeat(heartbeat);
        } catch (Throwable t) {
            LOGGER.error("process {} heartbeat failed: {}", request.getId(), t.getMessage(), t);
            throw new GeaflowRuntimeException(String.format("process %s heartbeat failed: %s",
                request.getId(), t.getMessage()), t);
        }
    }

    @Override
    public Empty receiveException(HeartbeatRequest request) {
        try {
            int containerId = request.getId();
            String containerName = RpcMessageEncoder.decode(request.getName());
            String errMessage = RpcMessageEncoder.decode(request.getPayload());
            LOGGER.info("received exception from {}: {}", containerName, errMessage);
            clusterManager.doFailover(containerId, new RuntimeException(errMessage));
            return Empty.newBuilder().build();
        } catch (Throwable t) {
            LOGGER.error("process {} heartbeat failed: {}", request.getId(), t.getMessage(), t);
            throw new GeaflowRuntimeException(String.format("process %s heartbeat failed: %s",
                request.getId(), t.getMessage()), t);
        }
    }

    @Override
    public Empty close(Empty request) {
        try {
            master.close();
            return Empty.newBuilder().build();
        } catch (Throwable t) {
            LOGGER.error("close failed: {}", t.getMessage(), t);
            throw new GeaflowRuntimeException(String.format("close failed: %s", t.getMessage()), t);
        }
    }
}
