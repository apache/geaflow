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

package org.apache.geaflow.cluster.clustermanager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.cluster.container.ContainerInfo;

public class ContainerExecutorInfo implements Serializable {

    /**
     * container id.
     */
    private int containerId;
    /**
     * container name.
     */
    private String containerName;
    /**
     * host ip.
     */
    private String host;
    /**
     * process id.
     */
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
     * executor index list.
     */
    private List<Integer> executorIds;

    public ContainerExecutorInfo(ContainerInfo containerInfo, int firstWorkerIndex,
                                 int workerNum) {
        this.containerId = containerInfo.getId();
        this.containerName = containerInfo.getName();
        this.host = containerInfo.getHost();
        this.rpcPort = containerInfo.getRpcPort();
        this.shufflePort = containerInfo.getShufflePort();
        this.processId = containerInfo.getPid();
        this.executorIds = new ArrayList<>(workerNum);
        for (int i = 0; i < workerNum; i++) {
            this.executorIds.add(firstWorkerIndex + i);
        }
    }

    public int getContainerId() {
        return containerId;
    }

    public String getContainerName() {
        return containerName;
    }

    public String getHost() {
        return host;
    }

    public int getProcessId() {
        return processId;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    public int getShufflePort() {
        return shufflePort;
    }

    public List<Integer> getExecutorIds() {
        return executorIds;
    }

}
