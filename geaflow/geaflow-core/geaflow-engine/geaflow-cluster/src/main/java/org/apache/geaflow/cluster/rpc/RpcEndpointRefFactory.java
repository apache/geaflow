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

package org.apache.geaflow.cluster.rpc;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.cluster.rpc.impl.ContainerEndpointRef;
import org.apache.geaflow.cluster.rpc.impl.DriverEndpointRef;
import org.apache.geaflow.cluster.rpc.impl.MasterEndpointRef;
import org.apache.geaflow.cluster.rpc.impl.MetricEndpointRef;
import org.apache.geaflow.cluster.rpc.impl.PipelineMasterEndpointRef;
import org.apache.geaflow.cluster.rpc.impl.ResourceManagerEndpointRef;
import org.apache.geaflow.cluster.rpc.impl.SupervisorEndpointRef;
import org.apache.geaflow.common.config.Configuration;

public class RpcEndpointRefFactory implements Serializable {
    private final Map<EndpointRefID, RpcEndpointRef> endpointRefMap;
    private final Configuration configuration;

    private static RpcEndpointRefFactory INSTANCE;

    private RpcEndpointRefFactory(Configuration config) {
        this.endpointRefMap = new ConcurrentHashMap<>();
        this.configuration = config;
    }

    public static synchronized RpcEndpointRefFactory getInstance(Configuration config) {
        if (INSTANCE == null) {
            INSTANCE = new RpcEndpointRefFactory(config);
        }
        return INSTANCE;
    }

    public static synchronized RpcEndpointRefFactory getInstance() {
        return INSTANCE;
    }

    public MasterEndpointRef connectMaster(String host, int port) {
        EndpointRefID refID = new EndpointRefID(host, port, EndpointType.MASTER);
        try {
            return (MasterEndpointRef) endpointRefMap
                .computeIfAbsent(refID,
                    key -> new MasterEndpointRef(host, port, configuration));
        } catch (Throwable t) {
            invalidateRef(refID);
            throw new RuntimeException("connect master error, host " + host + " port " + port, t);
        }
    }

    public ResourceManagerEndpointRef connectResourceManager(String host, int port) {
        EndpointRefID refID = new EndpointRefID(host, port, EndpointType.RESOURCE_MANAGER);
        try {
            return (ResourceManagerEndpointRef) endpointRefMap
                .computeIfAbsent(refID,
                    key -> new ResourceManagerEndpointRef(host, port, configuration));
        } catch (Throwable t) {
            invalidateRef(refID);
            throw new RuntimeException("connect rm error, host " + host + " port " + port, t);
        }
    }

    public DriverEndpointRef connectDriver(String host, int port) {
        EndpointRefID refID = new EndpointRefID(host, port, EndpointType.DRIVER);
        try {
            return (DriverEndpointRef) endpointRefMap
                .computeIfAbsent(refID,
                    key -> new DriverEndpointRef(host, port, configuration));
        } catch (Throwable t) {
            invalidateRef(refID);
            throw new RuntimeException("connect driver error, host " + host + " port " + port, t);
        }
    }

    public PipelineMasterEndpointRef connectPipelineManager(String host, int port) {
        EndpointRefID refID = new EndpointRefID(host, port, EndpointType.PIPELINE_MANAGER);
        try {
            return (PipelineMasterEndpointRef) endpointRefMap
                .computeIfAbsent(refID,
                    key -> new PipelineMasterEndpointRef(host, port, configuration));
        } catch (Throwable t) {
            invalidateRef(refID);
            throw new RuntimeException("connect pipeline master error, host " + host + " port " + port, t);
        }
    }

    public ContainerEndpointRef connectContainer(String host, int port) {
        EndpointRefID refID = new EndpointRefID(host, port, EndpointType.CONTAINER);
        try {
            return (ContainerEndpointRef) endpointRefMap
                .computeIfAbsent(refID,
                    key -> new ContainerEndpointRef(host, port, configuration));
        } catch (Throwable t) {
            invalidateRef(refID);
            throw new RuntimeException("connect container error, host " + host + " port " + port, t);
        }
    }

    public SupervisorEndpointRef connectSupervisor(String host, int port) {
        EndpointRefID refID = new EndpointRefID(host, port, EndpointType.SUPERVISOR);
        try {
            return (SupervisorEndpointRef) endpointRefMap
                .computeIfAbsent(refID, key -> new SupervisorEndpointRef(host, port, configuration));
        } catch (Throwable t) {
            invalidateRef(refID);
            throw new RuntimeException("connect container error, host " + host + " port " + port, t);
        }
    }

    public MetricEndpointRef connectMetricServer(String host, int port) {
        EndpointRefID refID = new EndpointRefID(host, port, EndpointType.METRIC);
        try {
            return (MetricEndpointRef) endpointRefMap
                .computeIfAbsent(refID, key -> new MetricEndpointRef(host, port, configuration));
        } catch (Throwable t) {
            invalidateRef(refID);
            throw new RuntimeException("connect container error, host " + host + " port " + port, t);
        }
    }

    public void invalidateEndpointCache(String host, int port, EndpointType endpointType) {
        invalidateRef(new EndpointRefID(host, port, endpointType));
    }

    public void invalidateRef(EndpointRefID refId) {
        endpointRefMap.remove(refId);
    }

    public enum EndpointType {
        /**
         * Master endpoint.
         */
        MASTER,
        /**
         * ResourceManager endpoint.
         */
        RESOURCE_MANAGER,
        /**
         * Driver endpoint.
         */
        DRIVER,
        /**
         * Pipeline endpoint.
         */
        PIPELINE_MANAGER,
        /**
         * Container endpoint.
         */
        CONTAINER,
        /**
         * Worker endpoint.
         */
        SUPERVISOR,
        /**
         * Metric query endpoint.
         */
        METRIC
    }

    public static class EndpointRefID {

        private String host;
        private int port;
        private EndpointType endpointType;

        public EndpointRefID(String host, int port, EndpointType endpointType) {
            this.host = host;
            this.port = port;
            this.endpointType = endpointType;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public EndpointType getEndpointType() {
            return endpointType;
        }

        public void setEndpointType(EndpointType endpointType) {
            this.endpointType = endpointType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            EndpointRefID that = (EndpointRefID) o;
            return port == that.port && host.equals(that.host) && endpointType == that.endpointType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(host, port, endpointType);
        }

        @Override
        public String toString() {
            return "EndpointRefID{" + "host='" + host + '\'' + ", port=" + port + ", endpointType="
                + endpointType + '}';
        }
    }

}
