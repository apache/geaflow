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
import org.apache.geaflow.console.common.dal.entity.EdgeEntity;
import org.apache.geaflow.console.common.dal.entity.GraphStructMappingEntity;
import org.apache.geaflow.console.common.dal.mapper.EdgeMapper;
import org.apache.geaflow.console.common.dal.mapper.GraphStructMappingMapper;
import org.apache.geaflow.console.common.dal.model.EdgeSearch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class EdgeDao extends TenantLevelDao<EdgeMapper, EdgeEntity> implements DataDao<EdgeEntity
    , EdgeSearch> {

    @Autowired
    private GraphStructMappingMapper graphStructMappingMapper;

    public List<EdgeEntity> getByGraphId(String graphId) {
        MPJLambdaWrapper<GraphStructMappingEntity> wrapper = new MPJLambdaWrapper<GraphStructMappingEntity>().selectAll(
                EdgeEntity.class).innerJoin(EdgeEntity.class, EdgeEntity::getId,
                GraphStructMappingEntity::getResourceId)
            .eq(GraphStructMappingEntity::getGraphId, graphId)
            .orderByAsc(GraphStructMappingEntity::getSortKey);
        return graphStructMappingMapper.selectJoinList(EdgeEntity.class, wrapper);
    }


}
