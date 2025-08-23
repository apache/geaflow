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

package org.apache.geaflow.cluster.common;

import java.io.Serializable;

public class ComponentInfo implements Serializable {

    /**
     * component id.
     */
    protected int id;
    /**
     * component name.
     */
    protected String name;
    /**
     * host ip.
     */
    protected String host;
    /**
     * process id.
     */
    protected int pid;
    /**
     * rpc service port.
     */
    protected int rpcPort;
    /**
     * metric query port.
     */
    protected int metricPort;
    /**
     * agent service port.
     */
    protected int agentPort;

    public ComponentInfo() {
    }

    public ComponentInfo(int id, String name, String host, int pid, int rpcPort) {
        this.id = id;
        this.host = host;
        this.pid = pid;
        this.rpcPort = rpcPort;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    public void setRpcPort(int rpcPort) {
        this.rpcPort = rpcPort;
    }

    public int getMetricPort() {
        return metricPort;
    }

    public void setMetricPort(int metricPort) {
        this.metricPort = metricPort;
    }

    public int getAgentPort() {
        return agentPort;
    }

    public void setAgentPort(int agentPort) {
        this.agentPort = agentPort;
    }
}
