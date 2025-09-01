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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.geaflow.console.common.dal.dao.FieldDao;
import org.apache.geaflow.console.common.dal.dao.NameDao;
import org.apache.geaflow.console.common.dal.entity.FieldEntity;
import org.apache.geaflow.console.common.dal.model.FieldSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.type.GeaflowResourceType;
import org.apache.geaflow.console.core.model.GeaflowId;
import org.apache.geaflow.console.core.model.GeaflowName;
import org.apache.geaflow.console.core.model.data.GeaflowField;
import org.apache.geaflow.console.core.model.data.GeaflowStruct;
import org.apache.geaflow.console.core.service.converter.FieldConverter;
import org.apache.geaflow.console.core.service.converter.NameConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class FieldService extends NameService<GeaflowField, FieldEntity, FieldSearch> {

    @Autowired
    private FieldDao fieldDao;

    @Autowired
    private FieldConverter fieldConverter;

    @Override
    protected NameDao<FieldEntity, FieldSearch> getDao() {
        return fieldDao;
    }

    @Override
    protected NameConverter<GeaflowField, FieldEntity> getConverter() {
        return fieldConverter;
    }

    @Override
    protected List<GeaflowField> parse(List<FieldEntity> entities) {
        return ListUtil.convert(entities, e -> fieldConverter.convert(e));
    }

    @Override
    public List<String> create(List<GeaflowField> models) {
        throw new UnsupportedOperationException("Field can only be saved by vertex/edge/table");
    }

    public List<String> createByResource(List<GeaflowField> models, String resourceId,
                                         GeaflowResourceType resourceType) {
        if (CollectionUtils.isEmpty(models)) {
            return new ArrayList<>();
        }

        List<FieldEntity> entities = new ArrayList<>();
        for (int i = 0; i < models.size(); i++) {
            entities.add(fieldConverter.convert(models.get(i), resourceId, resourceType, i));
        }
        return fieldDao.create(entities);
    }

    public Map<String, List<GeaflowField>> getByResources(List<String> resourceIds, GeaflowResourceType resourceType) {
        if (CollectionUtils.isEmpty(resourceIds)) {
            return new HashMap<>();
        }
        List<FieldEntity> fieldEntityList = fieldDao.getByResources(resourceIds, resourceType);

        // init map to avoid null fields
        Map<String, List<GeaflowField>> modelMap = new HashMap<>();
        resourceIds.forEach(id -> {
            modelMap.put(id, new ArrayList<>());
        });
        // convert to map according to guid.
        fieldEntityList.forEach(e -> {
            GeaflowField field = fieldConverter.convert(e);
            String id = e.getResourceId();
            modelMap.get(id).add(field);
        });

        return modelMap;
    }


    public void removeByResources(List<String> resourceIds, GeaflowResourceType resourceType) {
        fieldDao.removeByResources(resourceIds, resourceType);
    }

    public void updateResourceFields(List<GeaflowField> oldFields, List<GeaflowField> newFields, GeaflowStruct struct) {

        GeaflowResourceType resourceType = GeaflowResourceType.valueOf(struct.getType().name());
        // Save old fields
        Map<String, GeaflowField> oldFieldMap = ListUtil.toMap(oldFields, GeaflowId::getId);
        // add fields if id is null
        List<GeaflowField> addFields = newFields.stream().filter(e -> e.getId() == null).collect(Collectors.toList());
        this.createByResource(addFields, struct.getId(), resourceType);

        // update if id is null and different from the old field
        List<GeaflowField> updateFields = newFields.stream()
            .filter(e -> e.getId() != null && !oldFieldMap.get(e.getId()).equals(e)).collect(Collectors.toList());
        this.update(updateFields);

        // get the remove fields by subset
        List<String> oldIds = ListUtil.convert(oldFields, GeaflowId::getId);
        List<String> newIds = newFields.stream().map(GeaflowName::getId).filter(Objects::nonNull)
            .collect(Collectors.toList());
        List<String> removeIds = ListUtil.diff(oldIds, newIds);
        this.drop(removeIds);
    }

}
