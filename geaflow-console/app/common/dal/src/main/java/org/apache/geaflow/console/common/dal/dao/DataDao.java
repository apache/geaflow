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

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.yulichang.wrapper.MPJLambdaWrapper;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.console.common.dal.entity.DataEntity;
import org.apache.geaflow.console.common.dal.entity.IdEntity;
import org.apache.geaflow.console.common.dal.entity.NameEntity;
import org.apache.geaflow.console.common.dal.model.DataSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.springframework.util.CollectionUtils;

public interface DataDao<E extends DataEntity, S extends DataSearch> extends NameDao<E, S> {

    String INSTANCE_ID_FIELD_NAME = "instance_id";

    @Override
    default List<E> getByNames(List<String> names) {
        throw new GeaflowException("Use getByNames(instanceId, names) instead");
    }

    @Override
    default Map<String, String> getIdsByNames(List<String> names) {
        throw new GeaflowException("Use getIdsByNames(instanceId, names) instead");
    }

    default E getByName(String instanceId, String name) {
        if (name == null) {
            return null;
        }
        List<E> entities = getByNames(instanceId, Collections.singletonList(name));
        return entities.isEmpty() ? null : entities.get(0);
    }

    default Map<String, String> getIdByName(String instanceId, String name) {
        if (name == null) {
            return new HashMap<>();
        }
        return getIdsByNames(instanceId, Collections.singletonList(name));
    }


    default List<E> getByNames(String instanceId, List<String> names) {
        Preconditions.checkNotNull(instanceId, "Invalid instanceId");

        if (CollectionUtils.isEmpty(names)) {
            return new ArrayList<>();
        }

        return lambdaQuery().in(E::getName, names).eq(E::getInstanceId, instanceId).list();
    }

    default Map<String, String> getIdsByNames(String instanceId, List<String> names) {
        if (CollectionUtils.isEmpty(names)) {
            return new HashMap<>();
        }

        List<E> entities = lambdaQuery().select(E::getId, E::getName).eq(E::getInstanceId, instanceId)
            .in(E::getName, names).list();
        return ListUtil.toMap(entities, NameEntity::getName, IdEntity::getId);
    }

    default List<E> getByInstanceId(String instanceId) {
        Preconditions.checkNotNull(instanceId);
        return lambdaQuery().eq(E::getInstanceId, instanceId).list();
    }

    @Override
    default void configBaseSearch(QueryWrapper<E> wrapper, S search) {
        NameDao.super.configBaseSearch(wrapper, search);

        String instanceId = search.getInstanceId();
        wrapper.eq(instanceId != null, INSTANCE_ID_FIELD_NAME, instanceId);
    }

    @Override
    default void configBaseJoinSearch(MPJLambdaWrapper<E> wrapper, S search) {
        NameDao.super.configBaseJoinSearch(wrapper, search);

        String instanceId = search.getInstanceId();
        wrapper.eq(instanceId != null, E::getInstanceId, instanceId);
    }
}
