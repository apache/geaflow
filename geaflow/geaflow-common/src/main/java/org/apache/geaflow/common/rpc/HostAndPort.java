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

package org.apache.geaflow.common.rpc;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Objects;

public class HostAndPort implements Serializable {

    private static final String DELIMITER = "_";
    private final String host;

    private final int port;

    public HostAndPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public static HostAndPort of(String hostAndPort) {
        String[] strs = hostAndPort.split(DELIMITER);
        Preconditions.checkState(strs.length == 2);
        return new HostAndPort(strs[0], Integer.parseInt(strs[1]));
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HostAndPort that = (HostAndPort) o;
        return port == that.port && Objects.equals(host, that.host);
    }

    @Override
    public String toString() {
        return host + DELIMITER + port;
    }
}
