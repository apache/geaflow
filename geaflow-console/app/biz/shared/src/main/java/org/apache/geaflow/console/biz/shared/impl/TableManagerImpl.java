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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.console.biz.shared.PluginConfigManager;
import org.apache.geaflow.console.biz.shared.TableManager;
import org.apache.geaflow.console.biz.shared.convert.DataViewConverter;
import org.apache.geaflow.console.biz.shared.convert.FieldViewConverter;
import org.apache.geaflow.console.biz.shared.convert.PluginConfigViewConverter;
import org.apache.geaflow.console.biz.shared.convert.TableViewConverter;
import org.apache.geaflow.console.biz.shared.view.PluginConfigView;
import org.apache.geaflow.console.biz.shared.view.TableView;
import org.apache.geaflow.console.common.dal.entity.TableEntity;
import org.apache.geaflow.console.common.dal.model.TableSearch;
import org.apache.geaflow.console.common.util.Fmt;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.type.GeaflowPluginCategory;
import org.apache.geaflow.console.core.model.data.GeaflowField;
import org.apache.geaflow.console.core.model.data.GeaflowTable;
import org.apache.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import org.apache.geaflow.console.core.service.DataService;
import org.apache.geaflow.console.core.service.TableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TableManagerImpl extends DataManagerImpl<GeaflowTable, TableView, TableSearch> implements TableManager {

    @Autowired
    private TableService tableService;

    @Autowired
    private FieldViewConverter fieldViewConverter;

    @Autowired
    private PluginConfigViewConverter pluginConfigViewConverter;

    @Autowired
    private PluginConfigManager pluginConfigManager;

    @Autowired
    private TableViewConverter tableViewConverter;

    @Override
    public DataViewConverter<GeaflowTable, TableView> getConverter() {
        return tableViewConverter;
    }

    @Override
    public DataService<GeaflowTable, TableEntity, TableSearch> getService() {
        return tableService;
    }

    @Override
    public List<GeaflowTable> parse(List<TableView> views) {
        return views.stream().map(v -> {
            List<GeaflowField> fields = ListUtil.convert(v.getFields(), e -> fieldViewConverter.convert(e));
            GeaflowPluginConfig pluginConfig = pluginConfigViewConverter.convert(v.getPluginConfig());
            return tableViewConverter.convert(v, fields, pluginConfig);
        }).collect(Collectors.toList());
    }

    @Override
    public List<String> create(String instanceName, List<TableView> views) {
        for (TableView view : views) {
            PluginConfigView pluginConfigView = Preconditions.checkNotNull(view.getPluginConfig(),
                "Table pluginConfig is required");
            pluginConfigView.setCategory(GeaflowPluginCategory.TABLE);
            pluginConfigView.setName(Fmt.as("{}-{}-table-config", instanceName, view.getName()));
            pluginConfigManager.create(pluginConfigView);
        }
        return super.create(instanceName, views);
    }
}
