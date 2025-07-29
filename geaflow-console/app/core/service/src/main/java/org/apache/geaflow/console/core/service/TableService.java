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

package org.apache.geaflow.console.core.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.geaflow.console.common.dal.dao.DataDao;
import org.apache.geaflow.console.common.dal.dao.TableDao;
import org.apache.geaflow.console.common.dal.entity.IdEntity;
import org.apache.geaflow.console.common.dal.entity.TableEntity;
import org.apache.geaflow.console.common.dal.model.TableSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.type.GeaflowResourceType;
import org.apache.geaflow.console.core.model.GeaflowId;
import org.apache.geaflow.console.core.model.data.GeaflowField;
import org.apache.geaflow.console.core.model.data.GeaflowTable;
import org.apache.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import org.apache.geaflow.console.core.service.converter.DataConverter;
import org.apache.geaflow.console.core.service.converter.TableConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TableService extends DataService<GeaflowTable, TableEntity, TableSearch> {

    @Autowired
    private TableDao tableDao;

    @Autowired
    private FieldService fieldService;

    @Autowired
    private TableConverter tableConverter;


    @Autowired
    private PluginConfigService pluginConfigService;

    private GeaflowResourceType resourceType = GeaflowResourceType.TABLE;

    @Override
    protected DataDao<TableEntity, TableSearch> getDao() {
        return tableDao;
    }

    @Override
    protected DataConverter<GeaflowTable, TableEntity> getConverter() {
        return tableConverter;
    }

    @Override
    public List<String> create(List<GeaflowTable> models) {
        List<String> tableIds = super.create(models);

        for (GeaflowTable model : models) {
            fieldService.createByResource(new ArrayList<>(model.getFields().values()), model.getId(), resourceType);
        }

        return tableIds;
    }

    @Override
    protected List<GeaflowTable> parse(List<TableEntity> tableEntities) {
        List<String> tableIds = ListUtil.convert(tableEntities, IdEntity::getId);
        Map<String, List<GeaflowField>> fieldsMap = fieldService.getByResources(tableIds, resourceType);

        return tableEntities.stream().map(e -> {
            List<GeaflowField> fields = fieldsMap.get(e.getId());
            GeaflowPluginConfig pluginConfig = pluginConfigService.get(e.getPluginConfigId());
            return tableConverter.convert(e, fields, pluginConfig);
        }).collect(Collectors.toList());
    }

    @Override
    public boolean update(List<GeaflowTable> tables) {
        List<String> tableIds = ListUtil.convert(tables, GeaflowId::getId);

        fieldService.removeByResources(tableIds, resourceType);
        for (GeaflowTable newTable : tables) {
            List<GeaflowField> newFields = new ArrayList<>(newTable.getFields().values());
            fieldService.createByResource(newFields, newTable.getId(), resourceType);

            GeaflowPluginConfig pluginConfig = newTable.getPluginConfig();
            pluginConfigService.update(pluginConfig);
        }
        return super.update(tables);
    }

    @Override
    public boolean drop(List<String> ids) {
        List<TableEntity> entities = tableDao.get(ids);

        fieldService.removeByResources(ids, resourceType);
        pluginConfigService.drop(ListUtil.convert(entities, TableEntity::getPluginConfigId));
        return super.drop(ids);
    }

}
