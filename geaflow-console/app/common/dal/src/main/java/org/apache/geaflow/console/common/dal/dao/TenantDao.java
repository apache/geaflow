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

import com.github.yulichang.wrapper.MPJLambdaWrapper;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.geaflow.console.common.dal.entity.IdEntity;
import org.apache.geaflow.console.common.dal.entity.NameEntity;
import org.apache.geaflow.console.common.dal.entity.TenantEntity;
import org.apache.geaflow.console.common.dal.entity.TenantUserMappingEntity;
import org.apache.geaflow.console.common.dal.mapper.TenantMapper;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.dal.model.TenantSearch;
import org.springframework.stereotype.Repository;

@Repository
public class TenantDao extends SystemLevelDao<TenantMapper, TenantEntity> implements
    NameDao<TenantEntity, TenantSearch> {

    public PageList<TenantEntity> search(String userId, TenantSearch search) {
        MPJLambdaWrapper<TenantEntity> wrapper = new MPJLambdaWrapper<TenantEntity>().selectAll(TenantEntity.class)
            .innerJoin(TenantUserMappingEntity.class, TenantUserMappingEntity::getTenantId, TenantEntity::getId)
            .eq(TenantUserMappingEntity::getUserId, userId);

        return search(wrapper, search);
    }

    public Map<String, String> getTenantNames(Collection<String> tenantIds) {
        List<TenantEntity> entities = lambdaQuery().select(TenantEntity::getId, TenantEntity::getName)
            .in(TenantEntity::getId, tenantIds).list();
        return entities.stream().collect(Collectors.toMap(IdEntity::getId, NameEntity::getName));
    }
}
