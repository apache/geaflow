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

package org.apache.geaflow.common.heartbeat;

import java.io.Serializable;
import java.util.List;
import org.apache.geaflow.common.metric.ProcessMetrics;

public class HeartbeatInfo implements Serializable {

    private int totalNum;
    private int activeNum;
    private List<ContainerHeartbeatInfo> containers;
    private long expiredTimeMs;

    public static class ContainerHeartbeatInfo {

        private Integer id;

        private String name;

        private String host;

        private int pid;

        private Long lastTimestamp;

        private ProcessMetrics metrics;

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
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

        public Long getLastTimestamp() {
            return lastTimestamp;
        }

        public void setLastTimestamp(Long lastTimestamp) {
            this.lastTimestamp = lastTimestamp;
        }

        public ProcessMetrics getMetrics() {
            return metrics;
        }

        public void setMetrics(ProcessMetrics metrics) {
            this.metrics = metrics;
        }

        @Override
        public String toString() {
            return "ContainerHeartbeatInfo{" + "id=" + id + ", name='" + name + '\'' + ", host='"
                + host + '\'' + ", pid=" + pid + ", lastTimestamp=" + lastTimestamp + ", metrics="
                + metrics + '}';
        }
    }

    public int getTotalNum() {
        return totalNum;
    }

    public void setTotalNum(int totalNum) {
        this.totalNum = totalNum;
    }

    public int getActiveNum() {
        return activeNum;
    }

    public void setActiveNum(int activeNum) {
        this.activeNum = activeNum;
    }

    public List<ContainerHeartbeatInfo> getContainers() {
        return containers;
    }

    public void setContainers(List<ContainerHeartbeatInfo> containers) {
        this.containers = containers;
    }

    public long getExpiredTimeMs() {
        return expiredTimeMs;
    }

    public void setExpiredTimeMs(long expiredTimeMs) {
        this.expiredTimeMs = expiredTimeMs;
    }

    @Override
    public String toString() {
        return "HeartbeatInfo{" + "totalNum=" + totalNum + ", activeNum=" + activeNum
            + ", containers=" + containers + ", expiredTime=" + expiredTimeMs + '}';
    }
}
