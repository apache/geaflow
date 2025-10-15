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

package org.apache.geaflow.kubernetes.operator.core.job;

import static org.apache.geaflow.kubernetes.operator.core.model.constants.GeaflowConstants.CLUSTER_KEY;
import static org.apache.geaflow.kubernetes.operator.core.model.constants.GeaflowConstants.JOB_KEY;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.cluster.k8s.client.KubernetesJobClient;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.GeaflowJob;
import org.apache.geaflow.kubernetes.operator.core.util.GeaflowJobUtil;
import org.apache.geaflow.kubernetes.operator.core.util.KubernetesUtil;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class GeaflowJobManager implements JobManager<GeaflowJob> {

    private static final String DSL_MAIN_CLASS = "org.apache.geaflow.dsl.runtime.engine"
        + ".GeaFlowGqlClient";

    private static final int DEFAULT_KUBERNETES_CONNECTION_RETRY_TIMES = 10;

    @Override
    public void deployJob(GeaflowJob geaflowJob, Context<GeaflowJob> context) {
        KubernetesJobClient jobClient = generateJobClient(geaflowJob);
        jobClient.submitJob();
    }

    @Override
    public void destroyJob(GeaflowJob geaflowJob, Context<GeaflowJob> context) {
        KubernetesUtil.deleteClientConfigMap(geaflowJob.appId());
        KubernetesUtil.deleteMasterService(geaflowJob.appId());
    }

    private KubernetesJobClient generateJobClient(GeaflowJob geaflowJob) {
        Map<String, String> k8sClientConfig = generateClientArgs(geaflowJob);
        String masterUrl = KubernetesUtil.getKubernetesClient().getMasterUrl().toString();
        return new KubernetesJobClient(k8sClientConfig, masterUrl);
    }

    private Map<String, String> generateClientArgs(GeaflowJob geaflowJob) {
        String clusterId = geaflowJob.appId();
        String classArgsStr = GeaflowJobUtil.buildClassArgs(geaflowJob);
        JSONObject classArgs = JSON.parseObject(classArgsStr);
        JSONObject clientClusterArgs = classArgs.getJSONObject(CLUSTER_KEY);

        // Add k8s client config to clientArgs.
        clientClusterArgs.putAll(KubernetesUtil.generateKubernetesConfig().getConfigMap());

        // Add the app id into clientArgs.
        clientClusterArgs.put(ExecutionConfigKeys.CLUSTER_ID.getKey(), clusterId);

        Configuration k8sClientConfig = new Configuration();
        k8sClientConfig.putAll(clientClusterArgs.toJavaObject(new TypeReference<>() {
        }));
        JSONObject jobArgs = classArgs.getJSONObject(JOB_KEY);
        k8sClientConfig.putAll(jobArgs.toJavaObject(new TypeReference<>() {
        }));
        String entryClass = geaflowJob.getSpec().getEntryClass();
        if (StringUtils.isEmpty(entryClass)) {
            entryClass = DSL_MAIN_CLASS;
        }
        k8sClientConfig.put(KubernetesConfigKeys.USER_CLASS_ARGS, classArgs.toJSONString());
        k8sClientConfig.put(KubernetesConfigKeys.USER_MAIN_CLASS, entryClass);
        k8sClientConfig.put(KubernetesConfigKeys.CONNECTION_RETRY_TIMES,
            String.valueOf(DEFAULT_KUBERNETES_CONNECTION_RETRY_TIMES));
        return k8sClientConfig.getConfigMap();
    }
}
