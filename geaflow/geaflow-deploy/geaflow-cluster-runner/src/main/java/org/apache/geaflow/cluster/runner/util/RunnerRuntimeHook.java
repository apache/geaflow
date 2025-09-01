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

package org.apache.geaflow.cluster.runner.util;

import java.net.Socket;
import org.apache.geaflow.common.utils.ProcessUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunnerRuntimeHook extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(RunnerRuntimeHook.class);

    private final String name;
    private final int port;

    public RunnerRuntimeHook(String name, int port) {
        this.name = name;
        this.port = port;
    }

    @Override
    public void run() {
        checkLiveness();
    }

    private void checkLiveness() {
        String host = ProcessUtil.getHostIp();
        try (Socket socket = new Socket(host, port)) {
            LOGGER.info("Created socket to address: {}/{}", host, port);
            int c;
            while ((c = socket.getInputStream().read()) != -1) {
                LOGGER.info("Read message from remote: {}", c);
            }
        } catch (Throwable e) {
            LOGGER.error("Read from supervisor failed", e);
        }
        int pid = ProcessUtil.getProcessId();
        LOGGER.error("Kill {}(pid:{}) because parent process died", name, pid);
        ProcessUtil.killProcess(pid);
    }
}
