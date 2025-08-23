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

package org.apache.geaflow.cluster.ray.utils;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.JOB_WORK_PATH;

import io.ray.api.Ray;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.cluster.config.ClusterConfig;
import org.apache.geaflow.cluster.ray.config.RayConfig;
import org.apache.geaflow.common.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaySystemFunc implements Serializable {

    private static final long serialVersionUID = -3708025618479190982L;

    private static final Logger LOGGER = LoggerFactory.getLogger(RaySystemFunc.class);

    private static final Object LOCK = new Object();
    private static String appPath;

    public static boolean isRestarted() {
        return Ray.getRuntimeContext().wasCurrentActorRestarted();
    }

    public static boolean isLocalMode() {
        return Ray.getRuntimeContext().isLocalMode();
    }

    public static String getWorkPath() {
        if (appPath != null) {
            return appPath;
        }
        synchronized (LOCK) {
            if (Ray.getRuntimeContext().isLocalMode()) {
                appPath = "/tmp/" + System.currentTimeMillis();
                try {
                    FileUtils.forceMkdir(new File(appPath));
                } catch (IOException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            } else {
                appPath = Ray.getRuntimeContext().getCurrentRuntimeEnv()
                    .get(RayConfig.RAY_JOB_WORKING_DIR, String.class);
            }
        }
        return appPath;
    }

    public static void initRayEnv(ClusterConfig clusterConfig) {
        LOGGER.info("clusterConfig:{}", clusterConfig);
        Configuration config = clusterConfig.getConfig();

        // Set working dir.
        System.setProperty(
            String.format("%s.%s", RayConfig.RAY_JOB_RUNTIME_ENV, RayConfig.RAY_JOB_WORKING_DIR),
            config.getString(JOB_WORK_PATH));
        Ray.init();
    }
}
