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
import org.apache.commons.collections.CollectionUtils;
import org.apache.geaflow.console.common.dal.entity.GraphStructMappingEntity;
import org.apache.geaflow.console.common.dal.mapper.GraphStructMappingMapper;
import org.apache.geaflow.console.common.dal.model.IdSearch;
import org.apache.geaflow.console.common.util.type.GeaflowResourceType;
import org.springframework.stereotype.Repository;

@Repository
public class GraphStructMappingDao extends TenantLevelDao<GraphStructMappingMapper, GraphStructMappingEntity> implements
    IdDao<GraphStructMappingEntity, IdSearch> {

    public void removeGraphStructs(String graphId, List<String> vertexIds, List<String> edgeIds) {
        if (!CollectionUtils.isEmpty(vertexIds)) {
            lambdaUpdate().in(GraphStructMappingEntity::getResourceId, vertexIds)
                .eq(GraphStructMappingEntity::getResourceType, GeaflowResourceType.VERTEX)
                .eq(GraphStructMappingEntity::getGraphId, graphId).remove();
        }

        if (!CollectionUtils.isEmpty(edgeIds)) {
            lambdaUpdate().in(GraphStructMappingEntity::getResourceId, edgeIds)
                .eq(GraphStructMappingEntity::getResourceType, GeaflowResourceType.EDGE)
                .eq(GraphStructMappingEntity::getGraphId, graphId).remove();
        }
    }

    public boolean removeByGraphIds(List<String> graphIds) {
        if (CollectionUtils.isEmpty(graphIds)) {
            return true;
        }

        return lambdaUpdate().in(GraphStructMappingEntity::getGraphId, graphIds).remove();
    }

    public List<GraphStructMappingEntity> getByResourceId(String resourceId, GeaflowResourceType resourceType) {
        return lambdaQuery().eq(GraphStructMappingEntity::getResourceId, resourceId)
            .eq(GraphStructMappingEntity::getResourceType, resourceType)
            .list();
    }
}
