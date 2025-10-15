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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.dal.entity.IdEntity;
import org.apache.geaflow.console.common.dal.entity.NameEntity;
import org.apache.geaflow.console.common.dal.model.NameSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.springframework.util.CollectionUtils;

public interface NameDao<E extends NameEntity, S extends NameSearch> extends IdDao<E, S> {

    String NAME_FILE_NAME = "name";

    String COMMENT_FILE_NAME = "comment";

    default boolean existName(String name) {
        if (name == null) {
            return false;
        }

        return lambdaQuery().eq(E::getName, name).exists();
    }

    default E getByName(String name) {
        if (name == null) {
            return null;
        }
        List<E> entities = getByNames(Collections.singletonList(name));
        return entities.isEmpty() ? null : entities.get(0);
    }

    default String getIdByName(String name) {
        if (name == null) {
            return null;
        }
        return getIdsByNames(Collections.singletonList(name)).get(name);
    }

    default List<E> getByNames(List<String> names) {
        if (CollectionUtils.isEmpty(names)) {
            return new ArrayList<>();
        }

        return lambdaQuery().in(E::getName, names).list();
    }

    default Map<String, String> getIdsByNames(List<String> names) {
        if (CollectionUtils.isEmpty(names)) {
            return new HashMap<>();
        }

        List<E> entities = lambdaQuery().select(E::getId, E::getName).in(E::getName, names).list();
        return ListUtil.toMap(entities, NameEntity::getName, IdEntity::getId);

    }

    @Override
    default void configBaseSearch(QueryWrapper<E> wrapper, S search) {
        IdDao.super.configBaseSearch(wrapper, search);

        String name = search.getName();
        String comment = search.getComment();

        wrapper.like(StringUtils.isNotBlank(name), NAME_FILE_NAME, name);
        wrapper.like(StringUtils.isNotBlank(comment), COMMENT_FILE_NAME, comment);
    }

    @Override
    default void configBaseJoinSearch(MPJLambdaWrapper<E> wrapper, S search) {
        IdDao.super.configBaseJoinSearch(wrapper, search);

        String name = search.getName();
        String comment = search.getComment();

        wrapper.like(StringUtils.isNotBlank(name), E::getName, name);
        wrapper.like(StringUtils.isNotBlank(comment), E::getComment, comment);
    }
}
