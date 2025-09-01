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

import java.util.List;
import org.apache.geaflow.console.biz.shared.InstanceManager;
import org.apache.geaflow.console.biz.shared.convert.InstanceViewConverter;
import org.apache.geaflow.console.biz.shared.convert.NameViewConverter;
import org.apache.geaflow.console.biz.shared.view.InstanceView;
import org.apache.geaflow.console.common.dal.model.InstanceSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.core.model.data.GeaflowInstance;
import org.apache.geaflow.console.core.service.InstanceService;
import org.apache.geaflow.console.core.service.NameService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class InstanceManagerImpl extends NameManagerImpl<GeaflowInstance, InstanceView, InstanceSearch> implements
    InstanceManager {

    @Autowired
    private InstanceService instanceService;

    @Autowired
    private InstanceViewConverter instanceViewConverter;

    @Override
    public List<InstanceView> search() {
        List<GeaflowInstance> models = instanceService.search();
        return ListUtil.convert(models, e -> instanceViewConverter.convert(e));
    }

    @Override
    protected NameViewConverter<GeaflowInstance, InstanceView> getConverter() {
        return instanceViewConverter;
    }

    @Override
    protected List<GeaflowInstance> parse(List<InstanceView> views) {
        return ListUtil.convert(views, e -> instanceViewConverter.convert(e));
    }

    @Override
    protected NameService<GeaflowInstance, ?, InstanceSearch> getService() {
        return instanceService;
    }
}
