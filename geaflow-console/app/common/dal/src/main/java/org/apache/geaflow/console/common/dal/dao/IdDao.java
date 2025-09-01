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
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.OrderItem;
import com.baomidou.mybatisplus.extension.conditions.query.LambdaQueryChainWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.IService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.github.yulichang.base.mapper.MPJJoinMapper;
import com.github.yulichang.wrapper.MPJLambdaWrapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.dal.IdGenerator;
import org.apache.geaflow.console.common.dal.entity.IdEntity;
import org.apache.geaflow.console.common.dal.entity.UserLevelEntity;
import org.apache.geaflow.console.common.dal.model.IdSearch;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.dal.model.PageSearch.SortOrder;
import org.apache.geaflow.console.common.dal.wrapper.GeaflowLambdaQueryChainWrapper;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.springframework.util.CollectionUtils;

public interface IdDao<E extends IdEntity, S extends IdSearch> extends IService<E> {

    String TENANT_ID_FIELD_NAME = "tenant_id";

    String CREATE_TIME_FIELD_NAME = "gmt_create";

    String MODIFY_TIME_FIELD_NAME = "gmt_modified";

    String CREATOR_FIELD_NAME = "creator_id";

    String MODIFIER_FIELD_NAME = "modifier_id";

    default boolean exist(String id) {
        if (id == null) {
            return false;
        }

        return lambdaQuery().eq(E::getId, id).exists();
    }

    default E get(String id) {
        if (id == null) {
            return null;
        }
        List<E> entities = get(Collections.singletonList(id));
        return entities.isEmpty() ? null : entities.get(0);
    }

    default String create(E entity) {
        if (entity == null) {
            return null;
        }
        List<String> ids = create(Collections.singletonList(entity));
        return ids.isEmpty() ? null : ids.get(0);
    }

    default boolean update(E entity) {
        if (entity == null) {
            return false;
        }
        return update(Collections.singletonList(entity));
    }

    default boolean drop(String id) {
        if (id == null) {
            return false;
        }
        return drop(Collections.singletonList(id));
    }

    default List<E> get(List<String> ids) {
        if (CollectionUtils.isEmpty(ids)) {
            return new ArrayList<>();
        }

        return listByIds(ids);
    }

    default List<String> create(List<E> entities) {
        String tenantId = ContextHolder.get().getTenantId();
        String userId = ContextHolder.get().getUserId();
        Date date = new Date();

        boolean systemSession = ContextHolder.get().isSystemSession();
        boolean userLevel = this instanceof UserLevelDao;

        entities.forEach(e -> {
            e.setTenantId(Optional.ofNullable(e.getTenantId()).orElse(tenantId));
            e.setId(IdGenerator.nextId());
            e.setGmtCreate(Optional.ofNullable(e.getGmtCreate()).orElse(date));
            e.setGmtModified(Optional.ofNullable(e.getGmtModified()).orElse(date));
            e.setCreatorId(Optional.ofNullable(e.getCreatorId()).orElse(userId));
            e.setModifierId(Optional.ofNullable(e.getModifierId()).orElse(userId));
            if (userLevel) {
                ((UserLevelEntity) e).setSystem(systemSession);
            }
        });
        saveBatch(entities);
        return entities.stream().map(IdEntity::getId).collect(Collectors.toList());
    }

    default boolean update(List<E> entities) {
        if (CollectionUtils.isEmpty(entities)) {
            return false;
        }

        String userId = ContextHolder.get().getUserId();
        Date date = new Date();
        for (E e : entities) {
            e.setGmtModified(Optional.ofNullable(e.getGmtModified()).orElse(date));
            e.setModifierId(Optional.ofNullable(e.getModifierId()).orElse(userId));
        }

        return updateBatchById(entities);
    }

    default boolean drop(List<String> ids) {
        if (CollectionUtils.isEmpty(ids)) {
            return true;
        }

        return lambdaUpdate().in(E::getId, ids).remove();
    }

    default PageList<E> search(S search) {
        QueryWrapper<E> queryWrapper = new QueryWrapper<>();

        // config general query condition
        configQueryWrapper(queryWrapper);

        // config general search condition
        configBaseSearch(queryWrapper, search);

        // config concrete search condition
        LambdaQueryWrapper<E> lambdaQueryWrapper = queryWrapper.lambda();
        configSearch(lambdaQueryWrapper, search);

        LambdaQueryChainWrapper<E> wrapper = new GeaflowLambdaQueryChainWrapper<>(getBaseMapper(), lambdaQueryWrapper);
        Page<E> page = buildPage(search);
        if (page != null) {
            return new PageList<>(wrapper.page(page));

        } else {
            return new PageList<>(wrapper.list());
        }
    }

    default PageList<E> search(MPJLambdaWrapper<E> wrapper, S search) {
        MPJJoinMapper<E> joinMapper = ((MPJJoinMapper) getBaseMapper());
        configBaseJoinSearch(wrapper, search);
        configJoinSearch(wrapper, search);

        Class<E> entityClass = ((ServiceImpl) this).getEntityClass();
        Page<E> page = buildPage(search);
        if (page != null) {
            return new PageList<>(joinMapper.selectJoinPage(page, entityClass, wrapper));

        } else {
            return new PageList<>(joinMapper.selectJoinList(entityClass, wrapper));
        }
    }

    static <E extends IdEntity, S extends IdSearch> Page<E> buildPage(S search) {
        Page<E> page = null;
        if (search.getSize() != null) {
            page = new Page<>(Optional.ofNullable(search.getPage()).orElse(1), search.getSize());

            String sort = search.getSort();
            if (StringUtils.isNotBlank(sort)) {
                page.addOrder(SortOrder.ASC.equals(search.getOrder()) ? OrderItem.asc(sort) : OrderItem.desc(sort));
            } else {
                page.addOrder(OrderItem.desc(MODIFY_TIME_FIELD_NAME));
            }
        }
        return page;
    }

    default void configBaseSearch(QueryWrapper<E> wrapper, S search) {
        Date startCreateTime = search.getStartCreateTime();
        Date endCreateTime = search.getEndCreateTime();
        Date startModifyTime = search.getStartModifyTime();
        Date endModifyTime = search.getEndModifyTime();
        String creatorId = search.getCreatorId();
        String modifierId = search.getModifierId();

        wrapper.ge(startCreateTime != null, CREATE_TIME_FIELD_NAME, startCreateTime);
        wrapper.le(endCreateTime != null, CREATE_TIME_FIELD_NAME, endCreateTime);
        wrapper.ge(startModifyTime != null, MODIFY_TIME_FIELD_NAME, startModifyTime);
        wrapper.le(endModifyTime != null, MODIFY_TIME_FIELD_NAME, endModifyTime);
        wrapper.eq(StringUtils.isNotBlank(creatorId), CREATOR_FIELD_NAME, creatorId);
        wrapper.eq(StringUtils.isNotBlank(modifierId), MODIFIER_FIELD_NAME, modifierId);
        wrapper.orderBy(search.getOrder() != null && StringUtils.isNotBlank(search.getSort()),
            search.getOrder() == SortOrder.ASC, search.getSort());

    }

    default void configSearch(LambdaQueryWrapper<E> wrapper, S search) {

    }

    void configQueryWrapper(QueryWrapper<E> wrapper);

    void configUpdateWrapper(UpdateWrapper<E> wrapper);

    default void configBaseJoinSearch(MPJLambdaWrapper<E> wrapper, S search) {
        Date startCreateTime = search.getStartCreateTime();
        Date endCreateTime = search.getEndCreateTime();
        Date startModifyTime = search.getStartModifyTime();
        Date endModifyTime = search.getEndModifyTime();
        String creatorId = search.getCreatorId();
        String modifierId = search.getModifierId();

        wrapper.ge(startCreateTime != null, E::getGmtCreate, startCreateTime);
        wrapper.le(endCreateTime != null, E::getGmtCreate, endCreateTime);
        wrapper.ge(startModifyTime != null, E::getGmtModified, startModifyTime);
        wrapper.le(endModifyTime != null, E::getGmtModified, endModifyTime);
        wrapper.eq(StringUtils.isNotBlank(creatorId), E::getCreatorId, creatorId);
        wrapper.eq(StringUtils.isNotBlank(modifierId), E::getModifierId, modifierId);
    }

    default void configJoinSearch(MPJLambdaWrapper<E> wrapper, S search) {

    }
}
