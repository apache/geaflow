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

package org.apache.geaflow.common.utils;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

public class PortUtil {

    private static final int MAX_NUM = 200;
    private static final int DEFAULT_MIN_PORT = 50000;
    private static final int DEFAULT_MAX_PORT = 60000;

    public static int getPort(int minPort, int maxPort) {

        int num = 0;
        int port;
        while (num < MAX_NUM) {
            try {
                port = getAvailablePort(minPort, maxPort);
                if (port > 0) {
                    return port;
                }
            } catch (Exception e) {
                num++;
            }
        }
        throw new RuntimeException(String.format("no available port in [%d,%d]", minPort, maxPort));
    }

    public static int getPort(int port) {
        return port != 0 ? port : getPort(DEFAULT_MIN_PORT, DEFAULT_MAX_PORT);
    }

    private static int getAvailablePort(int minPort, int maxPort) throws IOException {
        Random random = new Random();
        int port = 0;
        while (true) {
            int tempPort = random.nextInt(maxPort) % (maxPort - minPort + 1) + minPort;
            ServerSocket serverSocket = new ServerSocket(tempPort);
            port = serverSocket.getLocalPort();
            serverSocket.close();
            break;
        }
        return port;
    }

}
