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
import org.apache.geaflow.console.common.dal.entity.IdEntity;
import org.apache.geaflow.console.common.dal.entity.VersionEntity;
import org.apache.geaflow.console.common.dal.mapper.VersionMapper;
import org.apache.geaflow.console.common.dal.model.VersionSearch;
import org.springframework.stereotype.Repository;

@Repository
public class VersionDao extends SystemLevelDao<VersionMapper, VersionEntity> implements
    NameDao<VersionEntity, VersionSearch> {

    @Override
    public void configSearch(LambdaQueryWrapper<VersionEntity> wrapper, VersionSearch search) {
        wrapper.eq(search.getPublish() != null, VersionEntity::isPublish, search.getPublish());
    }

    public VersionEntity getDefaultVersion() {
        return lambdaQuery().eq(VersionEntity::isPublish, true).orderByDesc(IdEntity::getGmtModified).last("limit 1")
            .one();
    }

    public VersionEntity getPublishVersionByName(String name) {
        return lambdaQuery().eq(VersionEntity::isPublish, true).eq(VersionEntity::getName, name).last("limit 1").one();
    }

    public long getFileRefCount(String fileId, String excludeFunctionId) {
        return lambdaQuery()
            .eq(VersionEntity::getEngineJarId, fileId)
            .or().eq(VersionEntity::getLangJarId, fileId)
            .ne(excludeFunctionId != null, IdEntity::getId, excludeFunctionId)
            .count();
    }
}
