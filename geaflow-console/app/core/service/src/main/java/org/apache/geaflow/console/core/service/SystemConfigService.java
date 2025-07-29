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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.console.common.dal.dao.NameDao;
import org.apache.geaflow.console.common.dal.dao.SystemConfigDao;
import org.apache.geaflow.console.common.dal.entity.SystemConfigEntity;
import org.apache.geaflow.console.common.dal.model.SystemConfigSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.core.model.config.GeaflowSystemConfig;
import org.apache.geaflow.console.core.service.converter.NameConverter;
import org.apache.geaflow.console.core.service.converter.SystemConfigConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SystemConfigService extends NameService<GeaflowSystemConfig, SystemConfigEntity, SystemConfigSearch> {

    @Autowired
    private SystemConfigDao systemConfigDao;

    @Autowired
    private SystemConfigConverter systemConfigConvert;

    @Override
    protected NameDao<SystemConfigEntity, SystemConfigSearch> getDao() {
        return systemConfigDao;
    }

    @Override
    protected NameConverter<GeaflowSystemConfig, SystemConfigEntity> getConverter() {
        return systemConfigConvert;
    }

    @Override
    protected List<GeaflowSystemConfig> parse(List<SystemConfigEntity> entities) {
        return ListUtil.convert(entities, systemConfigConvert::convert);
    }

    public GeaflowSystemConfig get(String tenantId, String key) {
        return parse(systemConfigDao.get(tenantId, key));
    }

    public boolean exist(String tenantId, String key) {
        return systemConfigDao.exist(tenantId, key);
    }

    public boolean delete(String tenantId, String key) {
        return systemConfigDao.delete(tenantId, key);
    }

    public void setValue(String key, Object value) {
        String str = Optional.ofNullable(value).map(Object::toString).orElse(null);

        // system level config
        if (ContextHolder.get().isSystemSession()) {
            systemConfigDao.setValue(null, key, str);
        }

        // tenant level config
        String tenantId = ContextHolder.get().getTenantId();
        systemConfigDao.setValue(tenantId, key, str);
    }

    public String getValue(String key) {
        // default config
        String defaultValue = systemConfigDao.getValue(null, key);

        // system level config
        if (ContextHolder.get().isSystemSession()) {
            return defaultValue;
        }

        // tenant level config
        String tenantId = ContextHolder.get().getTenantId();
        SystemConfigEntity entity = systemConfigDao.get(tenantId, key);
        return entity != null ? entity.getValue() : defaultValue;
    }

    public String getString(String key) {
        return getValue(key);
    }

    public long getLong(String key) {
        return Optional.ofNullable(getValue(key)).map(Long::parseLong).orElse(0L);
    }

    public int getInteger(String key) {
        return Optional.ofNullable(getValue(key)).map(Integer::parseInt).orElse(0);
    }

    public boolean getBoolean(String key) {
        return Optional.ofNullable(getValue(key)).map(Boolean::parseBoolean).orElse(false);
    }

    public JSONObject getJsonObject(String key) {
        return Optional.ofNullable(getValue(key)).map(JSON::parseObject).orElse(new JSONObject());
    }

    public JSONArray getJsonArray(String key) {
        return Optional.ofNullable(getValue(key)).map(JSON::parseArray).orElse(new JSONArray());
    }
}

