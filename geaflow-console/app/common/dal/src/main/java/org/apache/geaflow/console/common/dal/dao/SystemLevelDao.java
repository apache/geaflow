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

import org.apache.geaflow.console.common.dal.entity.IdEntity;
import org.apache.geaflow.console.common.dal.mapper.GeaflowBaseMapper;
import org.apache.geaflow.console.common.util.exception.GeaflowException;

public abstract class SystemLevelDao<M extends GeaflowBaseMapper<E>, E extends IdEntity> extends GeaflowBaseDao<M, E> {

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        if (!ignoreTenant) {
            String message = "Mapper {} must be annotated by @InterceptorIgnore(tenantLine = \"true\")";
            throw new GeaflowException(message, mapperClass.getSimpleName());
        }
    }

}
