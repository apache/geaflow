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

import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.geaflow.console.common.dal.entity.InstanceEntity;
import org.apache.geaflow.console.common.dal.entity.ResourceCount;
import org.apache.geaflow.console.common.dal.mapper.InstanceMapper;
import org.apache.geaflow.console.common.dal.model.InstanceSearch;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.springframework.stereotype.Repository;

@Repository
public class InstanceDao extends TenantLevelDao<InstanceMapper, InstanceEntity> implements NameDao<InstanceEntity, InstanceSearch> {

    public List<InstanceEntity> search() {
        return lambdaQuery().list();
    }


    public List<ResourceCount> getResourceCount(String instanceId, List<String> names) {
        if (instanceId == null || CollectionUtils.isEmpty(names)) {
            throw new GeaflowException("Empty instance or names");
        }

        return getBaseMapper().getResourceCount(instanceId, names);
    }
}
