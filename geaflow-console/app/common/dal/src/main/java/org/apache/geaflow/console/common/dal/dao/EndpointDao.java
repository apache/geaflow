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

import java.util.List;
import org.apache.geaflow.console.common.dal.entity.EndpointEntity;
import org.apache.geaflow.console.common.dal.mapper.EndpointMapper;
import org.apache.geaflow.console.common.dal.model.IdSearch;
import org.springframework.stereotype.Repository;

@Repository
public class EndpointDao extends TenantLevelDao<EndpointMapper, EndpointEntity> implements
    IdDao<EndpointEntity, IdSearch> {

    public List<EndpointEntity> getByGraphId(String graphId) {
        return lambdaQuery().eq(EndpointEntity::getGraphId, graphId).list();
    }

    public EndpointEntity getByEndpoint(String graphId, String edgeId, String sourceId, String targetId) {
        return lambdaQuery()
            .eq(EndpointEntity::getGraphId, graphId)
            .eq(EndpointEntity::getEdgeId, edgeId)
            .eq(EndpointEntity::getSourceId, sourceId)
            .eq(EndpointEntity::getTargetId, targetId).one();
    }

    public boolean exists(String graphId, String edgeId, String sourceId, String targetId) {
        return lambdaQuery().eq(EndpointEntity::getGraphId, graphId)
            .eq(EndpointEntity::getEdgeId, edgeId)
            .eq(EndpointEntity::getSourceId, sourceId)
            .eq(EndpointEntity::getTargetId, targetId).exists();
    }


    public boolean dropByGraphIds(List<String> graphIds) {
        return lambdaUpdate().eq(EndpointEntity::getGraphId, graphIds).remove();
    }

    public boolean dropByEndpoint(String graphId, String edgeId, String sourceId, String targetId) {
        return lambdaUpdate()
            .eq(EndpointEntity::getGraphId, graphId)
            .eq(EndpointEntity::getEdgeId, edgeId)
            .eq(EndpointEntity::getSourceId, sourceId)
            .eq(EndpointEntity::getTargetId, targetId).remove();
    }

}
