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

import static org.apache.geaflow.console.common.dal.dao.IdDao.TENANT_ID_FIELD_NAME;

import com.baomidou.mybatisplus.core.conditions.AbstractWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.extension.conditions.query.LambdaQueryChainWrapper;
import com.baomidou.mybatisplus.extension.conditions.update.LambdaUpdateChainWrapper;
import org.apache.geaflow.console.common.dal.entity.IdEntity;
import org.apache.geaflow.console.common.dal.mapper.GeaflowBaseMapper;
import org.apache.geaflow.console.common.dal.wrapper.GeaflowLambdaQueryChainWrapper;
import org.apache.geaflow.console.common.dal.wrapper.GeaflowLambdaUpdateChainWrapper;

public abstract class TenantLevelExtDao<M extends GeaflowBaseMapper<E>, E extends IdEntity> extends
    TenantLevelDao<M, E> {

    public static final String IGNORE_TENANT_SIGNATURE = "###GEAFLOW_IGNORE_TENANT_INTERCEPTOR###";


    public LambdaQueryChainWrapper<E> lambdaQuery(String tenantId) {
        if (tenantId != null) {
            return lambdaQuery();
        }

        QueryWrapper<E> wrapper = new QueryWrapper<>();
        configSystemWrapper(wrapper);
        return new GeaflowLambdaQueryChainWrapper<>(getBaseMapper(), wrapper.lambda());
    }

    public LambdaUpdateChainWrapper<E> lambdaUpdate(String tenantId) {
        if (tenantId != null) {
            return lambdaUpdate();
        }

        UpdateWrapper<E> wrapper = new UpdateWrapper<>();
        configSystemWrapper(wrapper);
        return new GeaflowLambdaUpdateChainWrapper<>(getBaseMapper(), wrapper.lambda());
    }

    private void configSystemWrapper(AbstractWrapper<E, String, ?> wrapper) {
        wrapper.isNull(TENANT_ID_FIELD_NAME).comment(IGNORE_TENANT_SIGNATURE);
    }

}
