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

import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.plugins.InterceptorIgnoreHelper;
import com.baomidou.mybatisplus.extension.conditions.query.LambdaQueryChainWrapper;
import com.baomidou.mybatisplus.extension.conditions.update.LambdaUpdateChainWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.geaflow.console.common.dal.entity.IdEntity;
import org.apache.geaflow.console.common.dal.mapper.GeaflowBaseMapper;
import org.apache.geaflow.console.common.dal.wrapper.GeaflowLambdaQueryChainWrapper;
import org.apache.geaflow.console.common.dal.wrapper.GeaflowLambdaUpdateChainWrapper;
import org.springframework.beans.factory.InitializingBean;

public abstract class GeaflowBaseDao<M extends GeaflowBaseMapper<E>, E extends IdEntity> extends
    ServiceImpl<M, E> implements InitializingBean {

    protected boolean ignoreTenant;

    protected String tableName;

    @Override
    public void afterPropertiesSet() throws Exception {
        this.ignoreTenant = InterceptorIgnoreHelper.willIgnoreTenantLine(mapperClass.getName() + ".*");
        this.tableName = entityClass.getAnnotation(TableName.class).value();
    }

    @Override
    public LambdaQueryChainWrapper<E> lambdaQuery() {
        QueryWrapper<E> wrapper = new QueryWrapper<>();
        configQueryWrapper(wrapper);
        return new GeaflowLambdaQueryChainWrapper<>(getBaseMapper(), wrapper.lambda());
    }

    @Override
    public LambdaUpdateChainWrapper<E> lambdaUpdate() {
        UpdateWrapper<E> wrapper = new UpdateWrapper<>();
        configUpdateWrapper(wrapper);
        return new GeaflowLambdaUpdateChainWrapper<>(getBaseMapper(), wrapper.lambda());
    }

    public void configQueryWrapper(QueryWrapper<E> wrapper) {

    }

    public void configUpdateWrapper(UpdateWrapper<E> wrapper) {

    }
}
