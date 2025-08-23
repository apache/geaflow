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

package org.apache.geaflow.console.core.service.runtime;

import org.apache.geaflow.console.core.model.data.GeaflowInstance;
import org.apache.geaflow.console.core.model.job.config.ClusterArgsClass;
import org.apache.geaflow.console.core.model.job.config.ClusterConfigClass;
import org.apache.geaflow.console.core.model.job.config.RayClientArgsClass;
import org.apache.geaflow.console.core.model.job.config.RayClusterArgsClass;
import org.apache.geaflow.console.core.model.plugin.config.RayPluginConfigClass;
import org.apache.geaflow.console.core.model.task.GeaflowTask;
import org.springframework.stereotype.Component;

@Component
public class RayTaskParams extends TaskParams {

    public RayClientArgsClass buildClientArgs(GeaflowInstance instance, GeaflowTask task) {
        return new RayClientArgsClass(buildGeaflowArgs(instance, task), task.getMainClass());
    }

    @Override
    protected ClusterArgsClass buildClusterArgs(GeaflowTask task) {

        RayClusterArgsClass clusterArgs = new RayClusterArgsClass();
        ClusterConfigClass clusterConfig = task.getRelease().getClusterConfig().parse(ClusterConfigClass.class);

        clusterArgs.setTaskClusterConfig(clusterConfig);
        clusterArgs.setRayConfig(buildRayClusterConfig(task));
        return clusterArgs;
    }

    RayPluginConfigClass buildRayClusterConfig(GeaflowTask task) {
        return task.getRelease().getCluster().getConfig().parse(RayPluginConfigClass.class);
    }

}
