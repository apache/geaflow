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
import org.apache.geaflow.console.biz.shared.IdManager;
import org.apache.geaflow.console.biz.shared.convert.IdViewConverter;
import org.apache.geaflow.console.biz.shared.view.IdView;
import org.apache.geaflow.console.common.dal.model.IdSearch;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.core.model.GeaflowId;
import org.apache.geaflow.console.core.service.IdService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

public abstract class IdManagerImpl<M extends GeaflowId, V extends IdView, S extends IdSearch> implements
    IdManager<V, S> {

    protected abstract IdService<M, ?, S> getService();

    protected abstract IdViewConverter<M, V> getConverter();

    @Autowired
    private IdManager<V, S> idManager;

    protected V build(M model) {
        return getConverter().convert(model);
    }

    protected List<V> build(List<M> models) {
        return ListUtil.convert(models, this::build);
    }

    protected M parse(V view) {
        List<M> views = parse(Collections.singletonList(view));
        return views.isEmpty() ? null : views.get(0);
    }

    protected abstract List<M> parse(List<V> views);

    @Override
    public PageList<V> search(S search) {
        PageList<M> models = getService().search(search);
        return models.transform(this::build);
    }

    public V get(String id) {
        List<V> list = get(Collections.singletonList(id));
        return list.isEmpty() ? null : list.get(0);
    }

    public String create(V view) {
        return idManager.create(Collections.singletonList(view)).get(0);
    }

    public boolean updateById(String id, V updateView) {
        updateView.setId(id);
        return idManager.update(Collections.singletonList(updateView));
    }

    public boolean drop(String id) {
        return idManager.drop(Collections.singletonList(id));
    }

    public List<V> get(List<String> ids) {
        List<M> models = getService().get(ids);
        return build(models);
    }

    @Transactional
    public List<String> create(List<V> views) {
        List<M> models = parse(views);
        List<String> ids = getService().create(models);

        for (int i = 0; i < ids.size(); i++) {
            views.get(i).setId(ids.get(i));
        }
        return ids;
    }

    @Transactional
    public boolean update(List<V> updateViews) {
        List<V> views = ListUtil.convert(updateViews, e -> {
            V oldView = get(e.getId());
            Preconditions.checkNotNull(oldView, "Invalid id {}", e.getId());
            getConverter().merge(oldView, e);
            return oldView;
        });

        List<M> models = parse(views);
        return getService().update(models);
    }

    @Transactional
    public boolean drop(List<String> ids) {
        return getService().drop(ids);
    }
}
