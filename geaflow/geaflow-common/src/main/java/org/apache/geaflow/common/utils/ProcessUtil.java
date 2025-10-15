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
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;
import java.net.InetAddress;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessUtil.class);
    private static final String HOSTNAME;
    private static final String HOST_IP;
    private static final String HOSTNAME_AND_IP;
    private static final String HOST_AND_PID;
    private static final int PROCESS_ID;
    public static final String LOCAL_ADDRESS;

    static {
        try {
            InetAddress addr = InetAddress.getLocalHost();
            HOSTNAME = addr.getHostName();
            HOST_IP = addr.getHostAddress();
            LOCAL_ADDRESS = addr.getHostAddress();
            HOSTNAME_AND_IP = HOSTNAME + "/" + HOST_IP;

            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            String name = runtime.getName(); // format: "pid@hostname"
            PROCESS_ID = Integer.parseInt(name.substring(0, name.indexOf('@')));
            HOST_AND_PID = HOSTNAME + ":" + PROCESS_ID;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public static String getHostname() {
        return HOSTNAME;
    }

    public static String getHostIp() {
        return HOST_IP;
    }

    public static int getProcessId() {
        return PROCESS_ID;
    }

    public static String getHostAndPid() {
        return HOST_AND_PID;
    }

    public static String getHostAndIp() {
        return HOSTNAME_AND_IP;
    }

    /**
     * -Xmx.
     */
    public static long getMaxMemory() {
        Runtime runtime = Runtime.getRuntime();
        return (runtime.maxMemory()) / FileUtils.ONE_MB;
    }

    /**
     * start is -Xms, Process Current Get Memory from Os.
     */
    public static long getTotalMemory() {
        Runtime runtime = Runtime.getRuntime();
        return (runtime.totalMemory()) / FileUtils.ONE_MB;
    }

    /**
     * Get Memory from os and not use.
     */
    public static long getFreeMemory() {
        Runtime runtime = Runtime.getRuntime();
        return (runtime.freeMemory()) / FileUtils.ONE_MB;
    }

    public static synchronized int getProcessPid(Process p) {
        int pid = -1;
        try {
            if (ReflectionUtil.JAVA_VERSION >= 9 || p.getClass().getName().equals("java.lang.UNIXProcess")) {
                Field f = p.getClass().getDeclaredField("pid");
                f.setAccessible(true);
                pid = f.getInt(p);
                f.setAccessible(false);
            }
        } catch (Exception e) {
            LOGGER.warn("fail to get pid from {}", p.getClass().getCanonicalName());
            pid = -1;
        }
        return pid;
    }

    public static void killProcess(int pid) {
        execute("kill -9 " + pid);
    }

    public static void execute(String cmd) {
        LOGGER.info(cmd);
        try {
            Process process = Runtime.getRuntime().exec(cmd);
            process.waitFor();
        } catch (InterruptedException | IOException e) {
            LOGGER.error(" {} failed: {}", cmd, e);
            throw new GeaflowRuntimeException(e.getMessage(), e);
        }
    }

}
