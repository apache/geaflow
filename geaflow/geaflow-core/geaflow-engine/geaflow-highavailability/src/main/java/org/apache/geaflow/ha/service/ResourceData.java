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

package org.apache.geaflow.ha.service;

import java.io.Serializable;

public class ResourceData implements Serializable {
    private String host;
    private int processId;
    /**
     * rpc service port.
     */

    private int rpcPort;
    /**
     * shuffle service port.
     */
    private int shufflePort;
    /**
     * shuffle service port.
     */
    private int metricPort;
    /**
     * worker rpc porker.
     */
    private int supervisorPort;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getProcessId() {
        return processId;
    }

    public void setProcessId(int processId) {
        this.processId = processId;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    public void setRpcPort(int rpcPort) {
        this.rpcPort = rpcPort;
    }

    public int getShufflePort() {
        return shufflePort;
    }

    public void setShufflePort(int shufflePort) {
        this.shufflePort = shufflePort;
    }

    public int getMetricPort() {
        return metricPort;
    }

    public void setMetricPort(int metricPort) {
        this.metricPort = metricPort;
    }

    public int getSupervisorPort() {
        return supervisorPort;
    }

    public void setSupervisorPort(int supervisorPort) {
        this.supervisorPort = supervisorPort;
    }

    @Override
    public String toString() {
        return "ResourceData{" + "host='" + host + '\'' + ", processId=" + processId + ", rpcPort="
            + rpcPort + ", shufflePort=" + shufflePort + ", metricPort=" + metricPort
            + ", supervisorPort=" + supervisorPort + '}';
    }
}
