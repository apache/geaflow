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

package org.apache.geaflow.cluster.k8s.config;

import org.apache.geaflow.cluster.k8s.config.KubernetesConfig.ServiceExposedType;
import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

/* This file is based on source code from the Flink Project (http://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is an adaptation of Flink's org.apache.flink.kubernetes.configuration.KubernetesConfigOptions.
 */
public class KubernetesConfigKeys {

    public static final ConfigKey CERT_DATA = ConfigKeys.key("kubernetes.cert.data")
        .defaultValue("")
        .description("kubernetes client cert data");

    public static final ConfigKey CERT_KEY = ConfigKeys.key("kubernetes.cert.key")
        .defaultValue("")
        .description("kubernetes client cert key data");

    public static final ConfigKey CA_DATA = ConfigKeys.key("kubernetes.ca.data")
        .defaultValue("")
        .description("kubernetes cluster ca data");

    public static final ConfigKey NAME_SPACE = ConfigKeys.key("kubernetes.namespace")
        .defaultValue("default")
        .description("kubernetes namespace");

    public static final ConfigKey CLUSTER_NAME = ConfigKeys.key("kubernetes.cluster.name")
        .defaultValue("")
        .description("kubernetes cluster name");

    public static final ConfigKey CLUSTER_FAULT_INJECTION_ENABLE = ConfigKeys.key("kubernetes"
            + ".cluster.fault-injection.enable")
        .defaultValue(false)
        .description("kubernetes cluster fo enable");

    public static final ConfigKey MASTER_URL = ConfigKeys.key("kubernetes.master.url")
        .defaultValue("https://kubernetes.default.svc")
        .description("kubernetes cluster master url");

    public static final ConfigKey SERVICE_SUFFIX = ConfigKeys.key("kubernetes.service.suffix")
        .defaultValue("")
        .description("suffix to append to the service name");

    public static final ConfigKey SERVICE_ACCOUNT = ConfigKeys.key("kubernetes.service.account")
        .defaultValue("geaflow")
        .description("kubernetes service account to request resources from api server");

    public static final ConfigKey SERVICE_EXPOSED_TYPE = ConfigKeys
        .key("kubernetes.service.exposed.type")
        .defaultValue(ServiceExposedType.NODE_PORT.name())
        .description("kubernetes service exposed service type");

    public static final ConfigKey SERVICE_DNS_ENV = ConfigKeys
        .key("kubernetes.service.dns.env")
        .defaultValue(null)
        .description("kubernetes service dns env");

    public static final ConfigKey SERVICE_USER_LABELS = ConfigKeys.key("kubernetes.service.user.labels")
        .defaultValue("")
        .description("The labels to be set for services. Specified as key:value pairs separated by "
            + "commas. such as version:alphav1,deploy:test.");

    public static final ConfigKey SERVICE_USER_ANNOTATIONS = ConfigKeys.key("kubernetes.service.user.annotations")
        .defaultValue("")
        .description("The annotations to be set for services. Specified as key:value pairs separated by "
            + "commas. such as version:alphav1,deploy:test.");

    public static final ConfigKey DNS_SEARCH_DOMAINS = ConfigKeys
        .key("kubernetes.pods.dns.search.domains")
        .defaultValue("")
        .description("dns search domain config");

    public static final ConfigKey CONNECTION_RETRY_TIMES = ConfigKeys
        .key("kubernetes.connection.retry.times")
        .defaultValue(100)
        .description("max retry to connect to api server");

    public static final ConfigKey CONNECTION_RETRY_INTERVAL_MS = ConfigKeys
        .key("kubernetes.connection.retry.interval.ms")
        .defaultValue(1000L)
        .description("max connect retry interval in ms");

    public static final ConfigKey PING_INTERVAL_MS = ConfigKeys
        .key("kubernetes.websocketPingInterval.ms")
        .defaultValue(10000L)
        .description("client ping interval in ms");

    public static final ConfigKey POD_USER_LABELS = ConfigKeys.key("kubernetes.pod.user.labels")
        .defaultValue("")
        .description("The labels to be set for pods. Specified as key:value pairs separated by "
            + "commas. such as version:alphav1,deploy:test.");

    public static final ConfigKey CONTAINER_IMAGE = ConfigKeys.key("kubernetes.container.image")
        .defaultValue("geaflow-k8s:latest")
        .description("container image name");

    public static final ConfigKey CONTAINER_IMAGE_PULL_POLICY = ConfigKeys
        .key("kubernetes.container.image.pullPolicy")
        .defaultValue("IfNotPresent")
        .description("container image pull policy");

    public static final ConfigKey CONTAINER_CONF_FILES = ConfigKeys
        .key("kubernetes.container.conf.files")
        .defaultValue("/opt/geaflow/conf/log4j.properties")
        .description("files to be used within containers");

    public static final ConfigKey ENABLE_RESOURCE_MEMORY_LIMIT = ConfigKeys
        .key("kubernetes.enable.resource.memory.limit")
        .defaultValue(true)
        .description("enable container memory limit");

    public static final ConfigKey ENABLE_RESOURCE_CPU_LIMIT = ConfigKeys
        .key("kubernetes.enable.resource.cpu.limit")
        .defaultValue(true)
        .description("enable container cpu limit");

    public static final ConfigKey ENABLE_RESOURCE_EPHEMERAL_STORAGE_LIMIT = ConfigKeys
        .key("kubernetes.enable.resource.storage.limit")
        .defaultValue(true)
        .description("enable container disk storage limit");

    public static final ConfigKey DEFAULT_RESOURCE_EPHEMERAL_STORAGE_SIZE = ConfigKeys
        .key("kubernetes.resource.storage.limit.size")
        .defaultValue("15Gi")
        .description("default container storage size");

    public static final ConfigKey DOCKER_NETWORK_TYPE = ConfigKeys
        .key("kubernetes.docker.network.type")
        .defaultValue("BRIDGE")
        .description("It could be BRIDGE/HOST.");

    public static final ConfigKey USE_IP_IN_HOST_NETWORK = ConfigKeys
        .key("kubernetes.use-ip-in-host-network")
        .defaultValue(true)
        .description("whether to use ip in host network");

    public static final ConfigKey ENABLE_LOG_DISK_LESS = ConfigKeys
        .key("kubernetes.log.diskless.enable")
        .defaultValue(true)
        .description("whether to enable log diskless");

    public static final ConfigKey TOLERATION_LIST = ConfigKeys
        .key("kubernetes.toleration.list")
        .noDefaultValue()
        .description("Multiple tolerations will be separated by commas. Each toleration contains "
            + "five parts, key:operator:value:effect:tolerationSeconds. Use - instead if the part "
            + "is null. For example, key1:Equal:value1:NoSchedule:-,key2:Exists:-:-:-,"
            + "key3:Equal:value3:NoExecute:3600");

    public static final ConfigKey MATCH_EXPRESSION_LIST = ConfigKeys
        .key("kubernetes.match-expression.list")
        .noDefaultValue()
        .description("Multiple match-expressions will be separated by commas. Each "
            + "match-expression contains "
            + "five parts, key:operator:value:effect:tolerationSeconds. Use - instead if the part "
            + "is null. For example, key1:Equal:value1:NoSchedule:-,key2:Exists:-:-:-,"
            + "key3:Equal:value3:NoExecute:3600");

    public static final ConfigKey EVICTED_POD_LABELS = ConfigKeys
        .key("kubernetes.pods.evict.labels")
        .defaultValue("pod.sigma.ali/eviction:true")
        .description("The labels of pod to be evicted");

    public static final ConfigKey CONF_DIR = ConfigKeys.key("kubernetes.geaflow.conf.dir")
        .defaultValue("/etc/geaflow/conf")
        .description("geaflow conf directory");

    public static final ConfigKey LOG_DIR = ConfigKeys.key("kubernetes.geaflow.log.dir")
        .defaultValue("/home/admin/logs/geaflow")
        .description("geaflow job log directory");

    public static final ConfigKey WATCHER_CHECK_INTERVAL = ConfigKeys
        .key("kubernetes.watcher.check.interval.seconds")
        .defaultValue(60)
        .description("time interval to check watcher liveness in seconds");

    public static final ConfigKey DRIVER_NODE_PORT = ConfigKeys.key("kubernetes.driver.node.port")
        .defaultValue(0)
        .description("driver node port");

    public static final ConfigKey WORK_DIR = ConfigKeys.key("kubernetes.geaflow.work.dir")
        .defaultValue("/home/admin/geaflow/tmp")
        .description("job work dir");

    public static final ConfigKey ENGINE_JAR_FILES = ConfigKeys.key("kubernetes.engine.jar.files")
        .defaultValue("")
        .description("engine jar files, separated by comma");

    public static final ConfigKey USER_JAR_FILES = ConfigKeys.key("kubernetes.user.jar.files")
        .defaultValue("")
        .description("user udf jar files, separated by comma");

    public static final ConfigKey USER_MAIN_CLASS = ConfigKeys.key("kubernetes.user.main.class")
        .noDefaultValue()
        .description("the main class of user program");

    public static final ConfigKey USER_CLASS_ARGS = ConfigKeys.key("kubernetes.user.class.args")
        .noDefaultValue()
        .description("the args of user mainClass");

    public static final ConfigKey PROCESS_AUTO_RESTART = ConfigKeys.key("kubernetes.cluster.process.auto-restart")
        .defaultValue("unexpected")
        .description("whether to restart process automatically");

    public static final ConfigKey CLIENT_KEY_ALGO = ConfigKeys.key("kubernetes.certs.client.key.algo")
        .defaultValue("")
        .description("client key algo");

    public static final ConfigKey LEADER_ELECTION_LEASE_DURATION = ConfigKeys.key("kubernetes.leader-election.lease-duration")
        .defaultValue(15)
        .description("The duration seconds of once leader-election in kubernetes. Contenders can "
            + "try to contend for a new leader after the previous leader invalid");

    public static final ConfigKey LEADER_ELECTION_RENEW_DEADLINE = ConfigKeys.key("kubernetes.leader-election.renew-deadline")
        .defaultValue(15)
        .description("The deadline seconds of once leader-election in kubernetes. The current "
            + "leader must renew the leadership within the deadline, or the leadership will be "
            + "invalid after lease duration");

    public static final ConfigKey LEADER_ELECTION_RETRY_PERIOD = ConfigKeys.key("kubernetes.leader-election.retry-period")
        .defaultValue(5)
        .description("The interval seconds of each contenders to try to contend for a new leader,"
            + " also is the interval seconds of current leader to renew for its leadership lease");

    public static final ConfigKey ALWAYS_PULL_ENGINE_JAR = ConfigKeys
        .key("kubernetes.engine.jar.pull.always")
        .defaultValue(false)
        .description("whether to always pull the remote engine jar to replace local ones");
}
