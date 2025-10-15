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

package org.apache.geaflow.example.util;

import static org.apache.geaflow.cluster.constants.ClusterConstants.CLUSTER_TYPE;

import java.util.Locale;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.env.EnvironmentFactory;
import org.apache.geaflow.env.IEnvironment.EnvType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvironmentUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(EnvironmentUtil.class);

    public static Environment loadEnvironment(String[] args) {
        EnvType clusterType = getClusterType();
        switch (clusterType) {
            case K8S:
                return EnvironmentFactory.onK8SEnvironment(args);
            case RAY:
                return EnvironmentFactory.onRayEnvironment(args);
            default:
                return EnvironmentFactory.onLocalEnvironment(args);
        }
    }

    private static EnvType getClusterType() {
        String clusterType = System.getProperty(CLUSTER_TYPE);
        if (StringUtils.isBlank(clusterType)) {
            LOGGER.warn("use local as default cluster");
            return EnvType.LOCAL;
        }
        return (EnvType.valueOf(clusterType.toUpperCase(Locale.ROOT)));
    }

}
