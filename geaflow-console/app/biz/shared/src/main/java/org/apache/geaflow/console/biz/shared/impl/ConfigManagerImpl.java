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

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.console.biz.shared.ConfigManager;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.common.util.type.GeaflowPluginCategory;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.core.model.config.ConfigDescFactory;
import org.apache.geaflow.console.core.model.config.ConfigDescItem;
import org.apache.geaflow.console.core.model.job.config.ClusterConfigClass;
import org.apache.geaflow.console.core.model.job.config.JobConfigClass;
import org.apache.geaflow.console.core.model.plugin.GeaflowPlugin;
import org.apache.geaflow.console.core.service.PluginService;
import org.apache.geaflow.console.core.service.config.DeployConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ConfigManagerImpl implements ConfigManager {

    @Autowired
    private DeployConfig deployConfig;

    @Autowired
    private PluginService pluginService;

    @Override
    public List<ConfigDescItem> getClusterConfig() {
        return ConfigDescFactory.getOrRegister(ClusterConfigClass.class).getItems();
    }

    @Override
    public List<ConfigDescItem> getJobConfig() {
        return ConfigDescFactory.getOrRegister(JobConfigClass.class).getItems();
    }

    @Override
    public List<GeaflowPluginCategory> getPluginCategories() {
        return Lists.newArrayList(GeaflowPluginCategory.values());
    }

    @Override
    public List<String> getPluginCategoryTypes(GeaflowPluginCategory category) {
        List<String> types = new ArrayList<>();
        switch (category) {
            case TABLE:
                types.add(GeaflowPluginType.FILE.name());
                types.add(GeaflowPluginType.KAFKA.name());
                types.add(GeaflowPluginType.HIVE.name());
                types.add(GeaflowPluginType.SOCKET.name());
                List<GeaflowPlugin> plugins = pluginService.getPlugins(category);
                types.addAll(ListUtil.convert(plugins, GeaflowPlugin::getType));
                break;
            case GRAPH:
                types.add(GeaflowPluginType.MEMORY.name());
                types.add(GeaflowPluginType.ROCKSDB.name());
                break;
            case RUNTIME_CLUSTER:
                if (deployConfig.isLocalMode()) {
                    types.add(GeaflowPluginType.CONTAINER.name());
                }
                types.add(GeaflowPluginType.K8S.name());
                types.add(GeaflowPluginType.RAY.name());
                break;
            case RUNTIME_META:
                types.add(GeaflowPluginType.JDBC.name());
                break;
            case HA_META:
                types.add(GeaflowPluginType.REDIS.name());
                break;
            case METRIC:
                types.add(GeaflowPluginType.INFLUXDB.name());
                break;
            case REMOTE_FILE:
            case DATA:
                if (deployConfig.isLocalMode()) {
                    types.add(GeaflowPluginType.LOCAL.name());
                }
                types.add(GeaflowPluginType.DFS.name());
                types.add(GeaflowPluginType.OSS.name());
                break;
            default:
                throw new GeaflowIllegalException("Unknown category {}", category);
        }
        return types.stream().distinct().collect(Collectors.toList());
    }

    @Override
    public List<ConfigDescItem> getPluginConfig(GeaflowPluginCategory category, String type) {
        if (!getPluginCategoryTypes(category).contains(type)) {
            throw new GeaflowIllegalException("Plugin type {} not supported by category {}", type, category);
        }

        GeaflowPluginType geaflowPluginType = GeaflowPluginType.of(type);
        return geaflowPluginType == GeaflowPluginType.None ? new ArrayList<>() :
            ConfigDescFactory.get(geaflowPluginType).getItems();
    }
}
