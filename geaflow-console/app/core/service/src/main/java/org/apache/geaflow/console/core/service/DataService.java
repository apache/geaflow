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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.geaflow.console.common.dal.dao.DataDao;
import org.apache.geaflow.console.common.dal.entity.DataEntity;
import org.apache.geaflow.console.common.dal.entity.ResourceCount;
import org.apache.geaflow.console.common.dal.model.DataSearch;
import org.apache.geaflow.console.common.util.Fmt;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.core.model.GeaflowName;
import org.apache.geaflow.console.core.model.data.GeaflowData;
import org.apache.geaflow.console.core.service.converter.DataConverter;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class DataService<M extends GeaflowData, E extends DataEntity, S extends DataSearch> extends
    NameService<M, E, S> {

    @Autowired
    private InstanceService instanceService;

    @Override
    protected abstract DataDao<E, S> getDao();

    @Override
    protected abstract DataConverter<M, E> getConverter();

    @Override
    public List<M> getByNames(List<String> names) {
        throw new GeaflowException("Use getByNames(instanceId, names) instead");
    }

    @Override
    public boolean dropByNames(List<String> names) {
        throw new GeaflowException("Use dropByNames(instanceId, names) instead");
    }

    @Override
    public Map<String, String> getIdsByNames(List<String> names) {
        throw new GeaflowException("Use getIdsByNames(instanceId, names) instead");
    }


    public M getByName(String instanceId, String name) {
        if (name == null) {
            return null;
        }
        List<M> users = getByNames(instanceId, Collections.singletonList(name));
        return users.isEmpty() ? null : users.get(0);
    }

    public boolean dropByName(String instanceId, String name) {
        if (name == null) {
            return false;
        }
        return dropByNames(instanceId, Collections.singletonList(name));
    }


    public String getIdByName(String instanceId, String name) {
        if (name == null) {
            return null;
        }
        Map<String, String> idsByNames = getIdsByNames(instanceId, Collections.singletonList(name));
        return idsByNames.get(name);
    }


    public List<M> getByNames(String instanceId, List<String> names) {
        List<E> entityList = getDao().getByNames(instanceId, names);
        if (CollectionUtils.isEmpty(entityList)) {
            return new ArrayList<>();
        }
        return parse(entityList);
    }

    public boolean dropByNames(String instanceId, List<String> names) {
        Map<String, String> idsByNames = getIdsByNames(instanceId, names);
        return this.drop(new ArrayList<>(idsByNames.values()));
    }


    public Map<String, String> getIdsByNames(String instanceId, List<String> names) {
        return getDao().getIdsByNames(instanceId, names);
    }


    public List<M> getByInstanceId(String instanceId) {
        List<E> entities = getDao().getByInstanceId(instanceId);
        return parse(entities);
    }

    @Override
    public List<String> create(List<M> models) {
        // check duplicate names in the current instance
        checkInstanceUniqueName(models);
        return super.create(models);
    }

    protected void checkInstanceUniqueName(List<M> models) {
        Map<String, List<M>> map = models.stream().collect(Collectors.groupingBy(GeaflowData::getInstanceId));

        for (Entry<String, List<M>> entry : map.entrySet()) {
            String instanceId = entry.getKey();
            List<String> names = ListUtil.convert(entry.getValue(), GeaflowName::getName);
            HashSet<String> nameSet = new HashSet<>(names);
            // check duplicate names in set
            if (nameSet.size() != names.size()) {
                throw new GeaflowException("Duplicated name found in {}", names);
            }
            List<ResourceCount> resourceCounts = instanceService.getResourceCount(instanceId, names);
            List<ResourceCount> filtered = resourceCounts.stream().filter(e -> e.getCount() > 0)
                .collect(Collectors.toList());
            // check duplicate names in databases
            if (!filtered.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                for (ResourceCount resourceCount : filtered) {
                    sb.append(Fmt.as("type:{}, name: {};", resourceCount.getType(), resourceCount.getName()));
                }
                throw new GeaflowException("Name conflict in current instance {}: {}", instanceId, sb.toString());
            }
        }
    }
}
