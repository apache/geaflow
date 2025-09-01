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

package org.apache.geaflow.console.biz.shared.convert;

import java.util.List;
import java.util.Optional;
import org.apache.geaflow.console.biz.shared.view.TableView;
import org.apache.geaflow.console.common.util.type.GeaflowPluginCategory;
import org.apache.geaflow.console.common.util.type.GeaflowStructType;
import org.apache.geaflow.console.core.model.data.GeaflowField;
import org.apache.geaflow.console.core.model.data.GeaflowTable;
import org.apache.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TableViewConverter extends StructViewConverter<GeaflowTable, TableView> {

    @Autowired
    private PluginConfigViewConverter pluginConfigViewConverter;


    @Override
    public void merge(TableView view, TableView updateView) {
        super.merge(view, updateView);
        Optional.ofNullable(updateView.getFields()).ifPresent(view::setFields);
        Optional.ofNullable(updateView.getPluginConfig()).ifPresent(e -> {
            // update pluginConfig info
            e.setId(view.getPluginConfig().getId());
            e.setCategory(GeaflowPluginCategory.TABLE);
            view.setPluginConfig(e);
        });
    }

    @Override
    protected TableView modelToView(GeaflowTable model) {
        TableView tableView = super.modelToView(model);
        tableView.setType(GeaflowStructType.TABLE);
        tableView.setPluginConfig(pluginConfigViewConverter.convert(model.getPluginConfig()));
        return tableView;
    }

    @Override
    protected GeaflowTable viewToModel(TableView view) {
        GeaflowTable table = super.viewToModel(view);
        table.setType(GeaflowStructType.TABLE);
        return table;
    }


    public GeaflowTable convert(TableView view, List<GeaflowField> fields, GeaflowPluginConfig pluginConfig) {
        GeaflowTable table = super.convert(view, fields);
        table.setPluginConfig(pluginConfig);
        return table;

    }
}
