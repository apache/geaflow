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
import org.apache.geaflow.console.biz.shared.NameManager;
import org.apache.geaflow.console.biz.shared.convert.NameViewConverter;
import org.apache.geaflow.console.biz.shared.view.NameView;
import org.apache.geaflow.console.common.dal.model.NameSearch;
import org.apache.geaflow.console.core.model.GeaflowName;
import org.apache.geaflow.console.core.service.NameService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

public abstract class NameManagerImpl<M extends GeaflowName, V extends NameView, S extends NameSearch>
    extends IdManagerImpl<M, V, S> implements NameManager<V, S> {

    @Autowired
    private NameManager<V, S> nameManager;

    @Override
    protected abstract NameService<M, ?, S> getService();

    @Override
    protected abstract NameViewConverter<M, V> getConverter();

    @Override
    public V getByName(String name) {
        List<V> models = getByNames(Collections.singletonList(name));
        return models.isEmpty() ? null : models.get(0);
    }

    @Override
    public List<V> getByNames(List<String> names) {
        List<M> models = getService().getByNames(names);
        return build(models);
    }

    @Override
    public boolean updateByName(String name, V view) {
        String id = getService().getIdByName(name);
        Preconditions.checkNotNull(id, "Invalid name %s", name);
        return updateById(id, view);
    }

    @Override
    public boolean dropByName(String name) {
        return nameManager.dropByNames(Collections.singletonList(name));
    }

    @Override
    @Transactional
    public boolean dropByNames(List<String> names) {
        return getService().dropByNames(names);
    }
}

