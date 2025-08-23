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

package org.apache.geaflow.console.biz.shared.impl;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.biz.shared.InstallManager;
import org.apache.geaflow.console.biz.shared.VersionManager;
import org.apache.geaflow.console.biz.shared.convert.InstallViewConverter;
import org.apache.geaflow.console.biz.shared.demo.DemoJob;
import org.apache.geaflow.console.biz.shared.view.InstallView;
import org.apache.geaflow.console.common.util.Fmt;
import org.apache.geaflow.console.common.util.I18nUtil;
import org.apache.geaflow.console.common.util.NetworkUtil;
import org.apache.geaflow.console.common.util.ProcessUtil;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.context.GeaflowContext;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.type.GeaflowPluginCategory;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.core.model.cluster.GeaflowCluster;
import org.apache.geaflow.console.core.model.config.GeaflowSystemConfig;
import org.apache.geaflow.console.core.model.config.SystemConfigKeys;
import org.apache.geaflow.console.core.model.data.GeaflowInstance;
import org.apache.geaflow.console.core.model.install.GeaflowInstall;
import org.apache.geaflow.console.core.model.job.GeaflowJob;
import org.apache.geaflow.console.core.model.plugin.config.ContainerPluginConfigClass;
import org.apache.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import org.apache.geaflow.console.core.model.plugin.config.InfluxdbPluginConfigClass;
import org.apache.geaflow.console.core.model.plugin.config.JdbcPluginConfigClass;
import org.apache.geaflow.console.core.model.plugin.config.K8sPluginConfigClass;
import org.apache.geaflow.console.core.model.plugin.config.LocalPluginConfigClass;
import org.apache.geaflow.console.core.model.plugin.config.PluginConfigClass;
import org.apache.geaflow.console.core.model.plugin.config.RedisPluginConfigClass;
import org.apache.geaflow.console.core.model.security.GeaflowTenant;
import org.apache.geaflow.console.core.model.security.GeaflowUser;
import org.apache.geaflow.console.core.service.ClusterService;
import org.apache.geaflow.console.core.service.DatasourceService;
import org.apache.geaflow.console.core.service.InstanceService;
import org.apache.geaflow.console.core.service.JobService;
import org.apache.geaflow.console.core.service.PluginConfigService;
import org.apache.geaflow.console.core.service.SystemConfigService;
import org.apache.geaflow.console.core.service.TenantService;
import org.apache.geaflow.console.core.service.UserService;
import org.apache.geaflow.console.core.service.config.DatasourceConfig;
import org.apache.geaflow.console.core.service.config.DeployConfig;
import org.apache.geaflow.console.core.service.security.TokenGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
public class InstallManagerImpl implements InstallManager {

    @Autowired
    private SystemConfigService systemConfigService;

    @Autowired
    private DeployConfig deployConfig;

    @Autowired
    private InstallViewConverter installViewConverter;

    @Autowired
    private DatasourceConfig datasourceConfig;

    @Autowired
    private DatasourceService datasourceService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private PluginConfigService pluginConfigService;

    @Autowired
    private VersionManager versionManager;

    @Autowired
    private TokenGenerator tokenGenerator;

    @Autowired
    private JobService jobService;

    @Autowired
    private InstanceService instanceService;

    @Autowired
    private UserService userService;

    @Autowired
    private TenantService tenantService;

    private final List<DemoJob> demoJobs;

    @Autowired
    public InstallManagerImpl(List<DemoJob> demoJobs) {
        this.demoJobs = demoJobs;
    }


    private interface ConfigBuilder {

        PluginConfigClass configRuntimeCluster();

        PluginConfigClass configRuntimeMeta();

        PluginConfigClass configHaMeta();

        PluginConfigClass configMetric();

        PluginConfigClass configRemoteFile();

        PluginConfigClass configData();

    }

    @Override
    public InstallView get() {
        GeaflowInstall install = new GeaflowInstall();

        // load default config
        install.setRuntimeClusterConfig(
            pluginConfigService.getDefaultPluginConfig(GeaflowPluginCategory.RUNTIME_CLUSTER));
        install.setRuntimeMetaConfig(pluginConfigService.getDefaultPluginConfig(GeaflowPluginCategory.RUNTIME_META));
        install.setHaMetaConfig(pluginConfigService.getDefaultPluginConfig(GeaflowPluginCategory.HA_META));
        install.setMetricConfig(pluginConfigService.getDefaultPluginConfig(GeaflowPluginCategory.METRIC));
        install.setRemoteFileConfig(pluginConfigService.getDefaultPluginConfig(GeaflowPluginCategory.REMOTE_FILE));
        install.setDataConfig(pluginConfigService.getDefaultPluginConfig(GeaflowPluginCategory.DATA));

        // default configs
        if (deployConfig.isLocalMode()) {
            ConfigBuilder builder = new DefaultConfigBuilder();
            if (install.getRuntimeClusterConfig() == null) {
                GeaflowPluginConfig runtimeClusterConfig = new GeaflowPluginConfig(
                    GeaflowPluginCategory.RUNTIME_CLUSTER, builder.configRuntimeCluster());
                runtimeClusterConfig.setName("cluster-default");
                runtimeClusterConfig.setComment(I18nUtil.getMessage("i18n.key.default.cluster"));
                install.setRuntimeClusterConfig(runtimeClusterConfig);
            }

            if (install.getRuntimeMetaConfig() == null) {
                GeaflowPluginConfig runtimeMetaConfig = new GeaflowPluginConfig(GeaflowPluginCategory.RUNTIME_META,
                    builder.configRuntimeMeta());
                runtimeMetaConfig.setName("runtime-meta-store-default");
                runtimeMetaConfig.setComment(I18nUtil.getMessage("i18n.key.default.runtime.meta.store"));
                install.setRuntimeMetaConfig(runtimeMetaConfig);
            }

            if (install.getHaMetaConfig() == null) {
                GeaflowPluginConfig haMetaConfig = new GeaflowPluginConfig(GeaflowPluginCategory.HA_META,
                    builder.configHaMeta());
                haMetaConfig.setName("ha-meta-store-default");
                haMetaConfig.setComment(I18nUtil.getMessage("i18n.key.default.ha.meta.store"));
                install.setHaMetaConfig(haMetaConfig);
            }

            if (install.getMetricConfig() == null) {
                GeaflowPluginConfig metricConfig = new GeaflowPluginConfig(GeaflowPluginCategory.METRIC,
                    builder.configMetric());
                metricConfig.setName("metric-store-default");
                metricConfig.setComment(I18nUtil.getMessage("i18n.key.default.metric.store"));
                install.setMetricConfig(metricConfig);
            }

            if (install.getRemoteFileConfig() == null) {
                GeaflowPluginConfig remoteFileConfig = new GeaflowPluginConfig(GeaflowPluginCategory.REMOTE_FILE,
                    builder.configRemoteFile());
                remoteFileConfig.setName("file-store-default");
                remoteFileConfig.setComment(I18nUtil.getMessage("i18n.key.default.file.store"));
                install.setRemoteFileConfig(remoteFileConfig);
            }

            if (install.getDataConfig() == null) {
                GeaflowPluginConfig dataConfig = new GeaflowPluginConfig(GeaflowPluginCategory.DATA,
                    builder.configData());
                dataConfig.setName("data-store-default");
                dataConfig.setComment(I18nUtil.getMessage("i18n.key.default.data.store"));
                install.setDataConfig(dataConfig);
            }
        }

        // get deploy mode
        InstallView installView = installViewConverter.convert(install);
        installView.setDeployMode(deployConfig.getMode());

        return installView;
    }

    @Transactional
    @Override
    public boolean install(InstallView installView) {
        if (systemConfigService.getBoolean(SystemConfigKeys.GEAFLOW_INITIALIZED)) {
            throw new GeaflowException("Geaflow has been initialized");
        }

        if (!systemConfigService.exist(null, SystemConfigKeys.GEAFLOW_INITIALIZED)) {
            GeaflowSystemConfig config = new GeaflowSystemConfig();
            config.setName(SystemConfigKeys.GEAFLOW_INITIALIZED);
            config.setComment(I18nUtil.getMessage("i18n.key.geaflow.system.inited.flag"));
            config.setValue("false");
            systemConfigService.create(config);
        }

        // check local deploy mode
        if (!deployConfig.isLocalMode() && NetworkUtil.isLocal(datasourceConfig.getUrl())) {
            throw new GeaflowException("Datasource '{}' can't be used in 'CLUSTER' deploy mode",
                StringUtils.substringBeforeLast(datasourceConfig.getUrl(), "?"));
        }

        // prepare install
        GeaflowInstall install = installViewConverter.convert(installView);

        // init plugin and config
        List<GeaflowPluginConfig> pluginConfigs = new ArrayList<>();
        pluginConfigs.add(install.getRuntimeClusterConfig());
        pluginConfigs.add(install.getRuntimeMetaConfig());
        pluginConfigs.add(install.getHaMetaConfig());
        pluginConfigs.add(install.getMetricConfig());
        pluginConfigs.add(install.getRemoteFileConfig());
        pluginConfigs.add(install.getDataConfig());
        pluginConfigs.forEach(pluginConfig -> {
            pluginConfigService.testConnection(pluginConfig);
            pluginConfigService.createDefaultPluginConfig(pluginConfig);
        });

        // init meta table
        GeaflowPluginConfig runtimeMetaConfig = install.getRuntimeMetaConfig();
        if (GeaflowPluginType.JDBC.name().equals(runtimeMetaConfig.getType())) {
            JdbcPluginConfigClass jdbcConfig = runtimeMetaConfig.getConfig().parse(JdbcPluginConfigClass.class);
            datasourceService.executeResource(jdbcConfig, "runtimemeta.init.sql");
        }

        // setup influxdb
        if (deployConfig.isLocalMode()) {
            GeaflowPluginConfig metricConfig = install.getMetricConfig();
            if (GeaflowPluginType.INFLUXDB.name().equals(metricConfig.getType())) {
                InfluxdbPluginConfigClass influxdbConfig = metricConfig.getConfig()
                    .parse(InfluxdbPluginConfigClass.class);
                if (influxdbConfig.getUrl().contains(deployConfig.getHost())) {
                    try {
                        String org = influxdbConfig.getOrg();
                        String bucket = influxdbConfig.getBucket();
                        String token = influxdbConfig.getToken();
                        String setupCommand = Fmt.as("/usr/local/bin/influx setup --org '{}' --bucket '{}' "
                            + "--username geaflow --password geaflow123456 --token '{}' --force", org, bucket, token);

                        log.info("Setup influxdb with command {}", setupCommand);
                        ProcessUtil.execute(setupCommand);
                    } catch (Exception e) {
                        log.error("Set up influx db failed", e);
                    }

                }
            }
        }

        // init cluster
        clusterService.create(new GeaflowCluster(install.getRuntimeClusterConfig()));

        // init version
        versionManager.createDefaultVersion();

        createDemoJobs();

        // set install status
        systemConfigService.setValue(SystemConfigKeys.GEAFLOW_INITIALIZED, true);
        return true;
    }

    private void createDemoJobs() {
        GeaflowContext context = ContextHolder.get();
        try {
            GeaflowUser user = userService.get(context.getUserId());
            GeaflowTenant tenant = tenantService.getByName(tenantService.getDefaultTenantName(user.getName()));
            GeaflowInstance instance = instanceService.getByName(
                instanceService.getDefaultInstanceName(user.getName()));
            context.setTenantId(tenant.getId());
            context.setSystemSession(false);

            List<GeaflowJob> jobs = new ArrayList<>();
            for (DemoJob demoJob : demoJobs) {
                GeaflowJob job = demoJob.build();
                job.setInstanceId(instance.getId());
                jobs.add(job);
            }

            jobService.create(jobs);
            log.info("create demo jobs success");
        } catch (Exception e) {
            log.error("create demo job failed", e);
            throw e;
        } finally {
            context.setTenantId(null);
            context.setSystemSession(true);
        }

    }


    private class DefaultConfigBuilder implements ConfigBuilder {

        @Override
        public PluginConfigClass configRuntimeCluster() {
            if (deployConfig.isLocalMode()) {
                return new ContainerPluginConfigClass();
            }

            K8sPluginConfigClass k8sConfig = new K8sPluginConfigClass();
            k8sConfig.setMasterUrl(Fmt.as("http://{}:8000", deployConfig.getHost()));
            k8sConfig.setImageUrl("tugraph/geaflow:0.1");
            k8sConfig.setServiceType("CLUSTER_IP");
            k8sConfig.setStorageLimit("10Gi");
            k8sConfig.setClientTimeout(600000);
            return k8sConfig;
        }

        @Override
        public PluginConfigClass configRuntimeMeta() {
            return datasourceConfig.buildPluginConfigClass();
        }

        @Override
        public PluginConfigClass configHaMeta() {
            RedisPluginConfigClass redisConfig = new RedisPluginConfigClass();
            redisConfig.setHost(deployConfig.getHost());
            redisConfig.setPort(6379);
            return redisConfig;
        }

        @Override
        public PluginConfigClass configMetric() {
            InfluxdbPluginConfigClass influxdbConfig = new InfluxdbPluginConfigClass();
            influxdbConfig.setUrl(Fmt.as("http://{}:8086", deployConfig.getHost()));
            influxdbConfig.setToken(tokenGenerator.nextToken());
            influxdbConfig.setOrg("geaflow");
            influxdbConfig.setBucket("geaflow");
            return influxdbConfig;
        }

        @Override
        public PluginConfigClass configRemoteFile() {
            LocalPluginConfigClass localConfig = new LocalPluginConfigClass();
            localConfig.setRoot("/tmp");
            return localConfig;
        }

        @Override
        public PluginConfigClass configData() {
            LocalPluginConfigClass localConfig = new LocalPluginConfigClass();
            localConfig.setRoot("/tmp/geaflow/chk");
            return localConfig;
        }
    }
}






