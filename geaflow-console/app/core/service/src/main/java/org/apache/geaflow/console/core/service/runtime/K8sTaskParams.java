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

import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;
import org.apache.geaflow.console.core.model.data.GeaflowInstance;
import org.apache.geaflow.console.core.model.job.config.ClusterArgsClass;
import org.apache.geaflow.console.core.model.job.config.ClusterConfigClass;
import org.apache.geaflow.console.core.model.job.config.K8SClusterArgsClass;
import org.apache.geaflow.console.core.model.job.config.K8sClientArgsClass;
import org.apache.geaflow.console.core.model.job.config.K8sClientStopArgsClass;
import org.apache.geaflow.console.core.model.plugin.config.K8sPluginConfigClass;
import org.apache.geaflow.console.core.model.release.GeaflowRelease;
import org.apache.geaflow.console.core.model.task.GeaflowTask;
import org.apache.geaflow.console.core.model.task.GeaflowTaskHandle;
import org.springframework.stereotype.Component;

@Component
public class K8sTaskParams extends TaskParams {

    public K8sClientArgsClass buildClientArgs(GeaflowInstance instance, GeaflowTask task) {
        return new K8sClientArgsClass(buildGeaflowArgs(instance, task), task.getMainClass());
    }

    public K8sClientStopArgsClass buildClientStopArgs(GeaflowTask task) {
        GeaflowTaskHandle handle = task.getHandle();
        Preconditions.checkNotNull(handle, "Task %s handle can't be empty", task.getId());

        return new K8sClientStopArgsClass(handle.getAppId(), ((K8SClusterArgsClass) buildClusterArgs(task)));
    }

    @Override
    protected ClusterArgsClass buildClusterArgs(GeaflowTask task) {
        GeaflowRelease release = task.getRelease();

        K8SClusterArgsClass clusterArgs = new K8SClusterArgsClass();
        ClusterConfigClass clusterConfig = release.getClusterConfig().parse(ClusterConfigClass.class);
        K8sPluginConfigClass k8sPluginConfig = release.getCluster().getConfig().parse(K8sPluginConfigClass.class);

        clusterArgs.setTaskClusterConfig(clusterConfig);
        clusterArgs.setClusterConfig(k8sPluginConfig);
        clusterArgs.setEngineJarUrls(JSON.toJSONString(task.getVersionFiles(deployConfig.getGatewayUrl())));
        clusterArgs.setTaskJarUrls(JSON.toJSONString(task.getUserFiles(deployConfig.getGatewayUrl())));
        return clusterArgs;
    }

}
