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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.geaflow.console.common.dal.dao.DataDao;
import org.apache.geaflow.console.common.dal.dao.EdgeDao;
import org.apache.geaflow.console.common.dal.entity.EdgeEntity;
import org.apache.geaflow.console.common.dal.entity.IdEntity;
import org.apache.geaflow.console.common.dal.model.EdgeSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.type.GeaflowResourceType;
import org.apache.geaflow.console.core.model.GeaflowId;
import org.apache.geaflow.console.core.model.data.GeaflowEdge;
import org.apache.geaflow.console.core.model.data.GeaflowField;
import org.apache.geaflow.console.core.service.converter.DataConverter;
import org.apache.geaflow.console.core.service.converter.EdgeConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class EdgeService extends DataService<GeaflowEdge, EdgeEntity, EdgeSearch> {

    private final GeaflowResourceType resourceType = GeaflowResourceType.EDGE;
    @Autowired
    private EdgeDao edgeDao;

    @Autowired
    private FieldService fieldService;

    @Autowired
    private GraphService graphService;

    @Autowired
    private EdgeConverter edgeConverter;

    @Override
    protected DataDao<EdgeEntity, EdgeSearch> getDao() {
        return edgeDao;
    }

    @Override
    protected DataConverter<GeaflowEdge, EdgeEntity> getConverter() {
        return edgeConverter;
    }

    @Override
    public List<String> create(List<GeaflowEdge> models) {
        List<String> edgeIds = super.create(models);
        // save fields
        for (GeaflowEdge model : models) {
            fieldService.createByResource(new ArrayList<>(model.getFields().values()), model.getId(), resourceType);
        }
        return edgeIds;
    }

    @Override
    protected List<GeaflowEdge> parse(List<EdgeEntity> edgeEntities) {
        List<String> edgeIds = ListUtil.convert(edgeEntities, IdEntity::getId);
        // select fields
        Map<String, List<GeaflowField>> fieldsMap = fieldService.getByResources(edgeIds, GeaflowResourceType.EDGE);

        return edgeEntities.stream().map(e -> {
            List<GeaflowField> fields = fieldsMap.get(e.getId());
            return edgeConverter.convert(e, fields);
        }).collect(Collectors.toList());
    }

    @Override
    public boolean update(List<GeaflowEdge> edges) {
        List<String> ids = ListUtil.convert(edges, GeaflowId::getId);

        fieldService.removeByResources(ids, resourceType);
        for (GeaflowEdge edge : edges) {
            List<GeaflowField> newFields = new ArrayList<>(edge.getFields().values());
            fieldService.createByResource(newFields, edge.getId(), resourceType);
        }
        return super.update(edges);
    }

    @Override
    public boolean drop(List<String> ids) {
        // can't drop if is used in graph.
        for (String id : ids) {
            graphService.checkBindingRelations(id, GeaflowResourceType.EDGE);
        }

        fieldService.removeByResources(ids, resourceType);
        return super.drop(ids);
    }

    public List<GeaflowEdge> getEdgesByGraphId(String graphId) {
        List<EdgeEntity> edges = edgeDao.getByGraphId(graphId);
        return parse(edges);
    }

}
