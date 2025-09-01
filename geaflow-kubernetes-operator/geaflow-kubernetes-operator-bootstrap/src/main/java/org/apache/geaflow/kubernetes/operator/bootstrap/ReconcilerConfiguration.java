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

package org.apache.geaflow.kubernetes.operator.bootstrap;

import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.*;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import java.util.List;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.geaflow.cluster.k8s.clustermanager.KubernetesClientFactory;
import org.apache.geaflow.kubernetes.operator.core.job.GeaflowJobManager;
import org.apache.geaflow.kubernetes.operator.core.job.JobCRCache;
import org.apache.geaflow.kubernetes.operator.core.reconciler.GeaflowJobReconciler;
import org.apache.geaflow.kubernetes.operator.core.util.KubernetesUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class ReconcilerConfiguration {

    @Value("${is_local_mode}")
    private Boolean isLocalMode;

    @Value("${k8s_master_url}")
    private String masterUrl;

    @Value("${k8s_namespace}")
    private String namespace;

    @Value("${k8s_client_cert_data}")
    private String clientCertData;

    @Value("${k8s_client_key_data}")
    private String clientKeyData;

    @Value("${k8s_ca_cert_data}")
    private String caCertData;

    @Autowired
    private GeaflowJobManager geaflowJobManager;

    @Autowired
    private JobCRCache jobCRCache;

    @PostConstruct
    private void postConstruct() {
        KubernetesClient kubernetesClient;
        if (isLocalMode != null && isLocalMode) {
            org.apache.geaflow.common.config.Configuration configuration =
                new org.apache.geaflow.common.config.Configuration();
            configuration.put(MASTER_URL, masterUrl);
            configuration.put(NAME_SPACE, namespace);
            configuration.put(CERT_KEY, clientKeyData);
            configuration.put(CERT_DATA, clientCertData);
            configuration.put(CA_DATA, caCertData);
            log.info("Operator is running in local host. Use custom k8s configuration: {}.", configuration);
            kubernetesClient = KubernetesClientFactory.create(configuration);
        } else {
            log.info(
                "Operator is deployed in kubernetes, use configuration of current k8s cluster.");
            kubernetesClient = new KubernetesClientBuilder().build();
        }
        KubernetesUtil.setKubernetesClient(kubernetesClient);
    }

    @Bean
    public GeaflowJobReconciler geaflowJobReconciler() {
        KubernetesClient kubernetesClient = KubernetesUtil.getKubernetesClient();
        return new GeaflowJobReconciler(kubernetesClient, geaflowJobManager, jobCRCache);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    @SuppressWarnings("rawtypes")
    public Operator operator(List<Reconciler> controllers) {
        Operator operator = new Operator();
        controllers.forEach(operator::register);
        return operator;
    }
}
