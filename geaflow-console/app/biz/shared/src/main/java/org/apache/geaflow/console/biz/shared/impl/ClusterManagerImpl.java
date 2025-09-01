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
import org.apache.geaflow.console.biz.shared.ClusterManager;
import org.apache.geaflow.console.biz.shared.convert.ClusterViewConverter;
import org.apache.geaflow.console.biz.shared.convert.NameViewConverter;
import org.apache.geaflow.console.biz.shared.view.ClusterView;
import org.apache.geaflow.console.common.dal.model.ClusterSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.core.model.cluster.GeaflowCluster;
import org.apache.geaflow.console.core.service.ClusterService;
import org.apache.geaflow.console.core.service.NameService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ClusterManagerImpl extends NameManagerImpl<GeaflowCluster, ClusterView, ClusterSearch> implements
    ClusterManager {

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private ClusterViewConverter clusterViewConverter;

    @Override
    protected NameService<GeaflowCluster, ?, ClusterSearch> getService() {
        return clusterService;
    }

    @Override
    protected NameViewConverter<GeaflowCluster, ClusterView> getConverter() {
        return clusterViewConverter;
    }

    @Override
    protected List<GeaflowCluster> parse(List<ClusterView> views) {
        return ListUtil.convert(views, clusterViewConverter::convert);
    }

    @Override
    public String create(ClusterView view) {
        if (clusterService.existName(view.getName())) {
            throw new GeaflowIllegalException("Cluster name {} exists", view.getName());
        }

        return super.create(view);
    }
}
