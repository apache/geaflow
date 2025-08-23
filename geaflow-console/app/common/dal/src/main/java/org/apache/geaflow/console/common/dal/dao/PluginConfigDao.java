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

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.dal.entity.PluginConfigEntity;
import org.apache.geaflow.console.common.dal.mapper.PluginConfigMapper;
import org.apache.geaflow.console.common.dal.model.PluginConfigSearch;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.type.GeaflowPluginCategory;
import org.springframework.stereotype.Repository;

@Repository
public class PluginConfigDao extends UserLevelDao<PluginConfigMapper, PluginConfigEntity> implements
    NameDao<PluginConfigEntity, PluginConfigSearch> {

    public List<PluginConfigEntity> getPluginConfigs(GeaflowPluginCategory category, String type) {
        return lambdaQuery().eq(category != null, PluginConfigEntity::getCategory, category)
            .eq(StringUtils.isNotEmpty(type), PluginConfigEntity::getType, type).list();
    }

    @Override
    public void configSearch(LambdaQueryWrapper<PluginConfigEntity> wrapper, PluginConfigSearch search) {
        boolean systemSession = ContextHolder.get().isSystemSession();
        wrapper.eq(search.getType() != null, PluginConfigEntity::getType, search.getType())
            .eq(search.getCategory() != null, PluginConfigEntity::getCategory, search.getCategory())
            .eq(PluginConfigEntity::isSystem, systemSession);
    }
}
