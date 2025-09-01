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

package org.apache.geaflow.kubernetes.operator.core.util;

import com.alibaba.fastjson.JSON;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.kubernetes.operator.core.model.constants.GeaflowConstants;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.AbstractGeaflowResource;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.AbstractJobSpec;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.ClientSpec;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.ContainerSpec;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.DriverSpec;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.MasterSpec;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.UserSpec;
import org.apache.geaflow.kubernetes.operator.core.model.exception.GeaflowRuntimeException;
import org.apache.geaflow.kubernetes.operator.core.model.job.RemoteFile;

public class GeaflowJobUtil {

    private static final SimpleDateFormat FORMATTER = new SimpleDateFormat("yyMMddhhmmssSSS");

    public static String buildClassArgs(AbstractGeaflowResource<?, ?> geaflowJob) {
        AbstractJobSpec spec = geaflowJob.getSpec();
        Map<String, Object> clusterArgs = new HashMap<>();

        // Add client spec to clusterArgs.
        fillInClientSpec(clusterArgs, spec);
        // Add master spec to clusterArgs.
        fillInMasterSpec(clusterArgs, spec);
        // Add driver spec to clusterArgs.
        fillInDriverSpec(clusterArgs, spec);
        // Add worker spec to clusterArgs.
        fillInContainerSpec(clusterArgs, spec);
        // Add engine jars and udf jars to clusterArgs.
        fillInEngineAndUdfJars(clusterArgs, spec);

        // Add image-pull-policy and service-account to clusterArgs.
        clusterArgs.put(KubernetesConfigKeys.CONTAINER_IMAGE.getKey(), spec.getImage());
        clusterArgs.put(KubernetesConfigKeys.CONTAINER_IMAGE_PULL_POLICY.getKey(), spec.getImagePullPolicy());
        clusterArgs.put(KubernetesConfigKeys.SERVICE_ACCOUNT.getKey(), spec.getServiceAccount());


        Map<String, Object> jobArgs = new HashMap<>();
        fillInJobArgs(jobArgs, spec);

        Map<String, Object> systemArgs = new HashMap<>();
        Long jobUid = geaflowJob.getStatus().getJobUid();
        fillInSystemArgs(systemArgs, spec, jobUid);

        Map<String, Object> classArgs = new HashMap<>();
        classArgs.put(GeaflowConstants.CLUSTER_KEY, clusterArgs);
        classArgs.put(GeaflowConstants.JOB_KEY, jobArgs);
        classArgs.put(GeaflowConstants.SYSTEM_KEY, systemArgs);
        return JSON.toJSONString(classArgs);
    }

    private static void fillInEngineAndUdfJars(Map<String, Object> clusterArgs, AbstractJobSpec spec) {
        clusterArgs.put(KubernetesConfigKeys.ENGINE_JAR_FILES.getKey(), spec.getEngineJars());
        List<RemoteFile> remoteFiles = new ArrayList<>();
        remoteFiles.addAll(Optional.ofNullable(spec.getUdfJars()).orElse(new ArrayList<>()));
        if (StringUtils.isEmpty(spec.getEntryClass())) {
            // Empty entry class, needs gql file.
            if (spec.getGqlFile() == null) {
                throw new GeaflowRuntimeException("Gql file must be set when entryClass is empty!");
            }
            String name = spec.getGqlFile().getName();
            if (StringUtils.isEmpty(name)) {
                throw new GeaflowRuntimeException("Gql file name cannot be empty!");
            }
            remoteFiles.add(spec.getGqlFile());
            clusterArgs.put(DSLConfigKeys.GEAFLOW_DSL_QUERY_PATH.getKey(), name);

            // Gql conf file is optional.
            if (spec.getGqlConfFile() != null) {
                String gqlConfFileName = spec.getGqlConfFile().getName();
                if (StringUtils.isEmpty(gqlConfFileName)) {
                    throw new GeaflowRuntimeException("Gql conf file name cannot be empty!");
                }
                remoteFiles.add(spec.getGqlConfFile());
                clusterArgs.put(DSLConfigKeys.GEAFLOW_DSL_PARALLELISM_CONFIG_PATH.getKey(),
                    spec.getGqlConfFile().getName());
            }
        }
        clusterArgs.put(KubernetesConfigKeys.USER_JAR_FILES.getKey(), remoteFiles);
    }

    private static void fillInSystemArgs(Map<String, Object> systemArgs, AbstractJobSpec spec,
                                         Long jobUid) {
        systemArgs.put(ExecutionConfigKeys.JOB_UNIQUE_ID.getKey(), jobUid);
        systemArgs.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(),
            GeaflowConstants.GEAFLOW_KEY + jobUid);
        UserSpec userSpec = spec.getUserSpec();
        if (userSpec != null) {
            Map<String, String> stateConfig = userSpec.getStateConfig();
            if (stateConfig != null) {
                systemArgs.put(GeaflowConstants.STATE_CONFIG_KEY, new HashMap<>(stateConfig));
            }
            Map<String, String> metricConfig = userSpec.getMetricConfig();
            if (metricConfig != null) {
                systemArgs.put(GeaflowConstants.METRIC_CONFIG_KEY, new HashMap<>(metricConfig));
            }
        }
    }

    private static void fillInJobArgs(Map<String, Object> jobArgs, AbstractJobSpec spec) {
        if (spec.getUserSpec() != null && spec.getUserSpec().getAdditionalArgs() != null) {
            jobArgs.putAll(spec.getUserSpec().getAdditionalArgs());
        }
    }

    private static void fillInClientSpec(Map<String, Object> clusterArgs, AbstractJobSpec spec) {
        ClientSpec clientSpec = Optional.ofNullable(spec.getClientSpec()).orElse(new ClientSpec());
        clusterArgs.put(ExecutionConfigKeys.CLIENT_MEMORY_MB.getKey(),
            clientSpec.getResource().getMemoryMb());
        clusterArgs.put(ExecutionConfigKeys.CLIENT_VCORES.getKey(),
            clientSpec.getResource().getCpuCores());
        clusterArgs.put(ExecutionConfigKeys.CLIENT_JVM_OPTIONS.getKey(),
            clientSpec.getResource().getJvmOptions());
    }

    private static void fillInMasterSpec(Map<String, Object> clusterArgs, AbstractJobSpec spec) {
        MasterSpec masterSpec = Optional.ofNullable(spec.getMasterSpec()).orElse(new MasterSpec());
        clusterArgs.put(ExecutionConfigKeys.MASTER_MEMORY_MB.getKey(),
            masterSpec.getResource().getMemoryMb());
        clusterArgs.put(ExecutionConfigKeys.MASTER_VCORES.getKey(),
            masterSpec.getResource().getCpuCores());
        clusterArgs.put(ExecutionConfigKeys.MASTER_JVM_OPTIONS.getKey(),
            masterSpec.getResource().getJvmOptions());
    }

    private static void fillInDriverSpec(Map<String, Object> clusterArgs, AbstractJobSpec spec) {
        DriverSpec driverSpec = Optional.ofNullable(spec.getDriverSpec()).orElse(new DriverSpec());
        clusterArgs.put(ExecutionConfigKeys.DRIVER_MEMORY_MB.getKey(),
            driverSpec.getResource().getMemoryMb());
        clusterArgs.put(ExecutionConfigKeys.DRIVER_VCORES.getKey(),
            driverSpec.getResource().getCpuCores());
        clusterArgs.put(ExecutionConfigKeys.DRIVER_JVM_OPTION.getKey(),
            driverSpec.getResource().getJvmOptions());
        clusterArgs.put(ExecutionConfigKeys.DRIVER_NUM.getKey(),
            driverSpec.getDriverNum());
    }

    private static void fillInContainerSpec(Map<String, Object> clusterArgs, AbstractJobSpec spec) {
        ContainerSpec containerSpec = Optional.ofNullable(spec.getContainerSpec())
            .orElse(new ContainerSpec());
        clusterArgs.put(ExecutionConfigKeys.CONTAINER_MEMORY_MB.getKey(),
            containerSpec.getResource().getMemoryMb());
        clusterArgs.put(ExecutionConfigKeys.CONTAINER_VCORES.getKey(),
            containerSpec.getResource().getCpuCores());
        clusterArgs.put(ExecutionConfigKeys.CONTAINER_JVM_OPTION.getKey(),
            containerSpec.getResource().getJvmOptions());
        clusterArgs.put(ExecutionConfigKeys.CONTAINER_NUM.getKey(), containerSpec.getContainerNum());
        clusterArgs.put(ExecutionConfigKeys.CONTAINER_WORKER_NUM.getKey(),
            containerSpec.getWorkerNumPerContainer());
    }

    public static synchronized Long generateJobUid() {
        StringBuilder date = new StringBuilder(FORMATTER.format(new Date()));
        Random random = new Random();
        for (int i = 0; i < 3; i++) {
            date.append(random.nextInt(10));
        }
        return Long.valueOf(date.toString());
    }

}
