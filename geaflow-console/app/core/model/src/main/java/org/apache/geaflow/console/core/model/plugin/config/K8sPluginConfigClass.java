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

package org.apache.geaflow.console.core.model.plugin.config;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.geaflow.console.common.util.NetworkUtil;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.core.model.config.GeaflowConfigKey;
import org.apache.geaflow.console.core.model.config.GeaflowConfigValue;

@Slf4j
@Getter
@Setter
public class K8sPluginConfigClass extends PluginConfigClass {

    @GeaflowConfigKey(value = "kubernetes.master.url", comment = "i18n.key.k8s.server.url")
    @GeaflowConfigValue(required = true, defaultValue = "https://0.0.0.0:6443")
    private String masterUrl;

    @GeaflowConfigKey(value = "kubernetes.container.image", comment = "i18n.key.geaflow.registry.address")
    @GeaflowConfigValue(required = true, defaultValue = "tugraph/geaflow:0.1")
    private String imageUrl;

    @GeaflowConfigKey(value = "kubernetes.service.account", comment = "i18n.key.api.service.username")
    @GeaflowConfigValue(defaultValue = "geaflow")
    private String serviceAccount;

    @GeaflowConfigKey(value = "kubernetes.service.exposed.type", comment = "i18n.key.api.service.type")
    @GeaflowConfigValue(defaultValue = "NODE_PORT")
    private String serviceType;

    @GeaflowConfigKey(value = "kubernetes.namespace", comment = "i18n.key.namespace")
    @GeaflowConfigValue(defaultValue = "default")
    private String namespace;

    @GeaflowConfigKey(value = "kubernetes.cert.data", comment = "i18n.key.client.cert.data")
    private String certData;

    @GeaflowConfigKey(value = "kubernetes.cert.key", comment = "i18n.key.client.cert.key")
    private String certKey;

    @GeaflowConfigKey(value = "kubernetes.ca.data", comment = "i18n.key.cluster.ca.data")
    private String caData;

    @GeaflowConfigKey(value = "kubernetes.connection.retry.times", comment = "i18n.key.retry.times")
    @GeaflowConfigValue(defaultValue = "100")
    private Integer retryTimes;

    @GeaflowConfigKey(value = "kubernetes.cluster.name", comment = "i18n.key.cluster.name")
    private String clusterName;

    @GeaflowConfigKey(value = "kubernetes.pod.user.labels", comment = "i18n.key.pod.user.labels")
    private String podUserLabels;

    @GeaflowConfigKey(value = "kubernetes.service.suffix", comment = "i18n.key.api.service.suffix")
    private String serviceSuffix;

    @GeaflowConfigKey(value = "kubernetes.resource.storage.limit.size", comment = "i18n.key.storage.limit")
    @GeaflowConfigValue(defaultValue = "10Gi")
    private String storageLimit;

    @GeaflowConfigKey(value = "kubernetes.geaflow.cluster.timeout.ms", comment = "i18n.key.client.timeout")
    @GeaflowConfigValue(defaultValue = "300000")
    private Integer clientTimeout;

    @GeaflowConfigKey(value = "kubernetes.container.image.pullPolicy", comment = "i18n.key.image.pull.policy")
    @GeaflowConfigValue(defaultValue = "Always")
    private String pullPolicy;

    @GeaflowConfigKey(value = "kubernetes.certs.client.key.algo", comment = "i18n.key.client.cert.key.algo")
    private String certKeyAlgo;

    @GeaflowConfigKey(value = "kubernetes.engine.jar.pull.always", comment = "i18n.key.engine.jar.pull.always")
    @GeaflowConfigValue(defaultValue = "true")
    private String alwaysPullEngineJar;

    public K8sPluginConfigClass() {
        super(GeaflowPluginType.K8S);
    }

    @Override
    public void testConnection() {
        NetworkUtil.testUrl(masterUrl);
    }
}
