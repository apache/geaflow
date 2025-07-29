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

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.dal.dao.AuthorizationDao;
import org.apache.geaflow.console.common.dal.dao.InstanceDao;
import org.apache.geaflow.console.common.dal.dao.NameDao;
import org.apache.geaflow.console.common.dal.entity.AuthorizationEntity;
import org.apache.geaflow.console.common.dal.entity.InstanceEntity;
import org.apache.geaflow.console.common.dal.entity.ResourceCount;
import org.apache.geaflow.console.common.dal.model.InstanceSearch;
import org.apache.geaflow.console.common.util.Fmt;
import org.apache.geaflow.console.common.util.I18nUtil;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.type.GeaflowAuthorityType;
import org.apache.geaflow.console.common.util.type.GeaflowResourceType;
import org.apache.geaflow.console.core.model.data.GeaflowInstance;
import org.apache.geaflow.console.core.model.security.GeaflowUser;
import org.apache.geaflow.console.core.service.converter.InstanceConverter;
import org.apache.geaflow.console.core.service.converter.NameConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InstanceService extends NameService<GeaflowInstance, InstanceEntity, InstanceSearch> {

    @Autowired
    private InstanceDao instanceDao;

    @Autowired
    private AuthorizationDao authorizationDao;

    @Autowired
    private InstanceConverter instanceConverter;

    @Autowired
    private AuthorizationService authorizationService;

    @Override
    protected NameDao<InstanceEntity, InstanceSearch> getDao() {
        return instanceDao;
    }

    @Override
    protected NameConverter<GeaflowInstance, InstanceEntity> getConverter() {
        return instanceConverter;
    }

    @Override
    protected List<GeaflowInstance> parse(List<InstanceEntity> entities) {
        return ListUtil.convert(entities, e -> instanceConverter.convert(e));
    }

    @Override
    public List<String> create(List<GeaflowInstance> models) {
        List<String> ids = super.create(models);
        authorizationService.addAuthorization(ids, ContextHolder.get().getUserId(), GeaflowAuthorityType.ALL,
            GeaflowResourceType.INSTANCE);
        return ids;
    }

    @Override
    public boolean drop(List<String> ids) {
        authorizationService.dropByResources(ids, GeaflowResourceType.INSTANCE);
        return super.drop(ids);
    }

    public List<GeaflowInstance> search() {
        return parse(instanceDao.search());
    }

    public List<ResourceCount> getResourceCount(String instanceId, List<String> names) {
        return instanceDao.getResourceCount(instanceId, names);
    }

    public String getDefaultInstanceName(String userName) {
        return "instance_" + userName;
    }

    @Transactional
    public String createDefaultInstance(String tenantId, GeaflowUser user) {
        String userName = user.getName();
        String userComment = user.getComment();
        String instanceName = getDefaultInstanceName(userName);
        String userDisplayName = StringUtils.isBlank(userComment) ? userName : userComment;
        String instanceComment = Fmt.as(I18nUtil.getMessage("i18n.key.default.instance.comment.format"),
            userDisplayName);

        // Need to set tenantId, using dao directly
        InstanceEntity entity = new InstanceEntity();
        entity.setTenantId(tenantId);
        entity.setName(instanceName);
        entity.setComment(instanceComment);
        String instanceId = instanceDao.create(entity);

        AuthorizationEntity authorizationEntity = new AuthorizationEntity(user.getId(), GeaflowAuthorityType.ALL,
            GeaflowResourceType.INSTANCE, instanceId);
        authorizationEntity.setTenantId(tenantId);
        authorizationDao.create(authorizationEntity);
        return instanceId;
    }
}
