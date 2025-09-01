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

package org.apache.geaflow.console.common.dal.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.console.common.dal.entity.FieldEntity;
import org.apache.geaflow.console.common.dal.mapper.FieldMapper;
import org.apache.geaflow.console.common.dal.model.FieldSearch;
import org.apache.geaflow.console.common.util.type.GeaflowResourceType;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

@Repository
public class FieldDao extends TenantLevelDao<FieldMapper, FieldEntity> implements NameDao<FieldEntity, FieldSearch> {


    public boolean removeByResources(List<String> resourceIds, GeaflowResourceType resourceType) {
        if (CollectionUtils.isEmpty(resourceIds)) {
            return true;
        }
        return lambdaUpdate().in(FieldEntity::getResourceId, resourceIds).eq(FieldEntity::getResourceType, resourceType).remove();
    }


    public List<FieldEntity> getByResources(List<String> resourceIds, GeaflowResourceType resourceType) {
        if (CollectionUtils.isEmpty(resourceIds)) {
            return new ArrayList<>();
        }

        return lambdaQuery().in(FieldEntity::getResourceId, resourceIds)
            .eq(FieldEntity::getResourceType, resourceType.name())
            .orderByAsc(Arrays.asList(FieldEntity::getResourceId, FieldEntity::getSortKey))
            .list();
    }


    public List<FieldEntity> getByResource(String resourceId, GeaflowResourceType resourceType) {
        return getByResources(Collections.singletonList(resourceId), resourceType);
    }
}
