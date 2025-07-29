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

package org.apache.geaflow.cluster.k8s.clustermanager;

import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.CA_DATA;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.CERT_DATA;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.CERT_KEY;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.CLIENT_KEY_ALGO;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.MASTER_URL;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.NAME_SPACE;
import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.PING_INTERVAL_MS;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.common.config.Configuration;

/**
 * Builder for Kubernetes clients.
 */
public class KubernetesClientFactory {

    public static KubernetesClient create(Configuration config) {
        String masterUrl = config.getString(MASTER_URL);
        return create(config, masterUrl);
    }

    public static KubernetesClient create(Configuration config, String masterUrl) {
        String namespace = config.getString(NAME_SPACE);
        long pingInterval = config.getLong(PING_INTERVAL_MS);

        ConfigBuilder clientConfig = new ConfigBuilder()
            .withApiVersion("v1")
            .withMasterUrl(masterUrl)
            .withWebsocketPingInterval(pingInterval)
            .withNamespace(namespace);

        clientConfig.withTrustCerts(true);
        String certKey = config.getString(CERT_KEY);
        if (!StringUtils.isBlank(certKey)) {
            clientConfig.withClientKeyData(certKey);
        }

        String certData = config.getString(CERT_DATA);
        if (!StringUtils.isBlank(certData)) {
            clientConfig.withClientCertData(certData);
        }

        String caData = config.getString(CA_DATA);
        if (!StringUtils.isBlank(caData)) {
            clientConfig.withCaCertData(certData);
        }

        String certKeyAlgo = config.getString(CLIENT_KEY_ALGO);
        if (!StringUtils.isBlank(certKeyAlgo)) {
            clientConfig.withClientKeyAlgo(certKeyAlgo);
        }

        return new KubernetesClientBuilder()
            .withConfig(clientConfig.build())
            .build();
    }

}

