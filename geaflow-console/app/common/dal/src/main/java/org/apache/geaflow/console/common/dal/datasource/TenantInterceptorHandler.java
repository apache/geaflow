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

package org.apache.geaflow.console.common.dal.datasource;

import com.baomidou.mybatisplus.extension.plugins.handler.TenantLineHandler;
import com.google.common.base.Preconditions;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import org.apache.geaflow.console.common.dal.dao.IdDao;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.context.GeaflowContext;

public class TenantInterceptorHandler implements TenantLineHandler {

    @Override
    public Expression getTenantId() {
        GeaflowContext context = ContextHolder.get();
        Preconditions.checkNotNull(context, "Invalid context");

        // allow null tenant id
        String tenantId = context.getTenantId();
        if (tenantId == null) {
            return null;
        }

        return new StringValue(tenantId);
    }

    @Override
    public String getTenantIdColumn() {
        return IdDao.TENANT_ID_FIELD_NAME;
    }

    @Override
    public boolean ignoreTable(String tableName) {
        // allow null tenant id
        return ContextHolder.get().getTenantId() == null;
    }
}
