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

import org.apache.geaflow.console.common.dal.entity.FunctionEntity;
import org.apache.geaflow.console.common.dal.entity.IdEntity;
import org.apache.geaflow.console.common.dal.mapper.FunctionMapper;
import org.apache.geaflow.console.common.dal.model.FunctionSearch;
import org.springframework.stereotype.Repository;

@Repository
public class FunctionDao extends TenantLevelDao<FunctionMapper, FunctionEntity> implements DataDao<FunctionEntity, FunctionSearch> {

    public long getFileRefCount(String fileId, String excludeFunctionId) {
        return lambdaQuery().eq(FunctionEntity::getJarPackageId, fileId)
            .ne(excludeFunctionId != null, IdEntity::getId, excludeFunctionId)
            .count();
    }
}
