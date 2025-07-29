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

package org.apache.geaflow.console.biz.shared.impl;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.geaflow.console.biz.shared.DataManager;
import org.apache.geaflow.console.biz.shared.convert.DataViewConverter;
import org.apache.geaflow.console.biz.shared.view.DataView;
import org.apache.geaflow.console.common.dal.model.DataSearch;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.core.model.data.GeaflowData;
import org.apache.geaflow.console.core.service.DataService;
import org.apache.geaflow.console.core.service.InstanceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

public abstract class DataManagerImpl<M extends GeaflowData, V extends DataView, S extends DataSearch> extends
    NameManagerImpl<M, V, S> implements DataManager<V, S> {

    @Autowired
    private InstanceService instanceService;

    @Autowired
    private DataManager<V, S> dataManager;

    @Override
    protected abstract DataService<M, ?, S> getService();

    @Override
    protected abstract DataViewConverter<M, V> getConverter();

    public V getByName(String instanceName, String name) {
        // by instanceName
        List<V> models = getByNames(instanceName, Collections.singletonList(name));
        return models.isEmpty() ? null : models.get(0);
    }

    public List<V> getByNames(String instanceName, List<String> names) {
        String instanceId = getInstanceIdByName(instanceName);
        List<M> models = getService().getByNames(instanceId, names);
        return build(models);
    }

    public boolean dropByName(String instanceName, String name) {
        return dataManager.dropByNames(instanceName, Collections.singletonList(name));
    }

    @Transactional
    public boolean dropByNames(String instanceName, List<String> names) {
        String instanceId = getInstanceIdByName(instanceName);
        return getService().dropByNames(instanceId, names);
    }

    public String create(String instanceName, V view) {
        return dataManager.create(instanceName, Collections.singletonList(view)).get(0);
    }

    @Transactional
    public List<String> create(String instanceName, List<V> views) {
        List<M> models = parse(views);
        // set instanceId
        String instanceId = getInstanceIdByName(instanceName);
        for (M model : models) {
            model.setInstanceId(instanceId);
        }

        List<String> ids = getService().create(models);

        for (int i = 0; i < ids.size(); i++) {
            views.get(i).setId(ids.get(i));
        }
        return ids;
    }

    @Override
    public boolean updateByName(String name, V view) {
        throw new GeaflowException("Use updateByName(instanceName, name, view) instead");
    }

    @Override
    public boolean updateByName(String instanceName, String name, V view) {
        String instanceId = getInstanceIdByName(instanceName);
        String id = getService().getIdByName(instanceId, name);
        Preconditions.checkNotNull(id, "Invalid name %s in instance %s", name, instanceName);
        return updateById(id, view);
    }

    @Override
    public boolean update(String instanceName, List<V> views) {
        String instanceId = getInstanceIdByName(instanceName);
        for (V view : views) {
            String id = getService().getIdByName(instanceId, view.getName());
            Preconditions.checkNotNull(id, "Invalid name %s in instance %s", view.getName(), instanceName);
            view.setId(id);
        }

        return dataManager.update(views);
    }

    @Override
    public List<V> getByNames(List<String> names) {
        throw new GeaflowIllegalException("Instance id is needed");
    }

    @Override
    public List<String> create(List<V> views) {
        throw new GeaflowIllegalException("Instance id is needed");
    }

    @Override
    public boolean dropByName(String name) {
        throw new GeaflowIllegalException("Instance id is needed");
    }

    @Override
    public PageList<V> searchByInstanceName(String instanceName, S search) {
        String instanceId = getInstanceIdByName(instanceName);
        search.setInstanceId(instanceId);
        return search(search);
    }

    @Override
    public void createIfIdAbsent(String instanceName, List<V> views) {
        if (CollectionUtils.isEmpty(views)) {
            return;
        }

        List<V> filtered = views.stream().filter(e -> e.getId() == null).collect(Collectors.toList());
        this.create(instanceName, filtered);
    }

    protected String getInstanceIdByName(String instanceName) {
        String id = instanceService.getIdByName(instanceName);
        if (id == null) {
            throw new GeaflowException("Instance name {} not found", instanceName);
        }
        return id;
    }
}
