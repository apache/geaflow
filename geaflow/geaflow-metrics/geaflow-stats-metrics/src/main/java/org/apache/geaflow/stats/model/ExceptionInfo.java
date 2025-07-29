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

package org.apache.geaflow.stats.model;

import java.io.Serializable;

public class ExceptionInfo implements Serializable {

    protected String hostname;
    protected String ip;
    protected int processId;
    protected String severity;
    protected String message;
    protected long timestamp;

    public ExceptionInfo(String hostname, String ip, int processId, String message,
                         String severity) {
        this.hostname = hostname;
        this.ip = ip;
        this.processId = processId;
        this.message = message;
        this.timestamp = System.currentTimeMillis();
        this.severity = severity;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getProcessId() {
        return processId;
    }

    public void setProcessId(int processId) {
        this.processId = processId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    @Override
    public String toString() {
        return "ExceptionInfo{" + "hostname='" + hostname + '\'' + ", ip='" + ip + '\''
            + ", processId=" + processId + ", severity='" + severity + '\'' + ", message='"
            + message + '\'' + ", timestamp=" + timestamp + '}';
    }
}


