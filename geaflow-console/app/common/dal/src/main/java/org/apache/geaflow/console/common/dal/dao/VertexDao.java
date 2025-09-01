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

import com.github.yulichang.wrapper.MPJLambdaWrapper;
import java.util.List;
import org.apache.geaflow.console.common.dal.entity.EndpointEntity;
import org.apache.geaflow.console.common.dal.entity.GraphStructMappingEntity;
import org.apache.geaflow.console.common.dal.entity.VertexEntity;
import org.apache.geaflow.console.common.dal.mapper.EndpointMapper;
import org.apache.geaflow.console.common.dal.mapper.GraphStructMappingMapper;
import org.apache.geaflow.console.common.dal.mapper.VertexMapper;
import org.apache.geaflow.console.common.dal.model.VertexSearch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class VertexDao extends TenantLevelDao<VertexMapper, VertexEntity> implements DataDao<VertexEntity, VertexSearch> {

    @Autowired
    private GraphStructMappingMapper graphStructMappingMapper;

    @Autowired
    private EndpointMapper endpointMapper;


    public List<VertexEntity> getByGraphId(String graphId) {
        MPJLambdaWrapper<GraphStructMappingEntity> wrapper = new MPJLambdaWrapper<GraphStructMappingEntity>().selectAll(
                VertexEntity.class)
            .innerJoin(VertexEntity.class, VertexEntity::getId, GraphStructMappingEntity::getResourceId)
            .eq(GraphStructMappingEntity::getGraphId, graphId)
            .orderByAsc(GraphStructMappingEntity::getSortKey);
        return graphStructMappingMapper.selectJoinList(VertexEntity.class, wrapper);

    }

    public List<VertexEntity> getByEdge(String edgeId) {
        MPJLambdaWrapper<EndpointEntity> wrapper = new MPJLambdaWrapper<EndpointEntity>().selectAll(
                VertexEntity.class).innerJoin(VertexEntity.class, VertexEntity::getId,
                EndpointEntity::getSourceId)
            .eq(EndpointEntity::getEdgeId, edgeId);
        return endpointMapper.selectJoinList(VertexEntity.class, wrapper);
    }
}
