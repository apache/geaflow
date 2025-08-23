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
import org.apache.geaflow.console.common.dal.dao.VertexDao;
import org.apache.geaflow.console.common.dal.entity.IdEntity;
import org.apache.geaflow.console.common.dal.entity.VertexEntity;
import org.apache.geaflow.console.common.dal.model.VertexSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.type.GeaflowResourceType;
import org.apache.geaflow.console.core.model.GeaflowId;
import org.apache.geaflow.console.core.model.data.GeaflowField;
import org.apache.geaflow.console.core.model.data.GeaflowVertex;
import org.apache.geaflow.console.core.service.converter.DataConverter;
import org.apache.geaflow.console.core.service.converter.VertexConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class VertexService extends DataService<GeaflowVertex, VertexEntity, VertexSearch> {

    @Autowired
    private VertexDao vertexDao;

    @Autowired
    private GraphService graphService;

    @Autowired
    private FieldService fieldService;

    @Autowired
    private VertexConverter vertexConverter;
    private GeaflowResourceType resourceType = GeaflowResourceType.VERTEX;

    @Override
    protected DataDao<VertexEntity, VertexSearch> getDao() {
        return vertexDao;
    }

    @Override
    protected DataConverter<GeaflowVertex, VertexEntity> getConverter() {
        return vertexConverter;
    }

    @Override
    public List<String> create(List<GeaflowVertex> models) {
        List<String> vertexIds = super.create(models);

        for (GeaflowVertex model : models) {
            fieldService.createByResource(new ArrayList<>(model.getFields().values()), model.getId(), resourceType);
        }

        return vertexIds;
    }

    @Override
    protected List<GeaflowVertex> parse(List<VertexEntity> vertexEntities) {
        List<String> vertexIds = ListUtil.convert(vertexEntities, IdEntity::getId);
        //select fields of each vertex
        Map<String, List<GeaflowField>> fieldsMap = fieldService.getByResources(vertexIds, resourceType);
        return vertexEntities.stream().map(e -> {
            List<GeaflowField> fields = fieldsMap.get(e.getId());
            return vertexConverter.convert(e, fields);
        }).collect(Collectors.toList());
    }

    @Override
    public boolean update(List<GeaflowVertex> vertices) {
        // update field
        List<String> ids = ListUtil.convert(vertices, GeaflowId::getId);

        fieldService.removeByResources(ids, resourceType);
        for (GeaflowVertex vertex : vertices) {
            List<GeaflowField> newFields = new ArrayList<>(vertex.getFields().values());
            fieldService.createByResource(newFields, vertex.getId(), resourceType);
        }
        return super.update(vertices);
    }

    @Override
    public boolean drop(List<String> ids) {
        // can't drop if is used in graph.
        for (String id : ids) {
            graphService.checkBindingRelations(id, GeaflowResourceType.VERTEX);
        }

        fieldService.removeByResources(ids, resourceType);
        return super.drop(ids);
    }


    public List<GeaflowVertex> getVerticesByGraphId(String graphId) {
        List<VertexEntity> entities = vertexDao.getByGraphId(graphId);
        return parse(entities);
    }

    public List<GeaflowVertex> getVerticesByGraphId(String graphId, Map<String, GeaflowVertex> vertexMap) {
        List<VertexEntity> entities = vertexDao.getByGraphId(graphId);
        // filter vertices that not exist
        List<VertexEntity> rests = new ArrayList<>();
        List<VertexEntity> exists = new ArrayList<>();

        entities.forEach(e -> {
            if (vertexMap.containsKey(e.getId())) {
                exists.add(e);
            } else {
                rests.add(e);
            }
        });

        List<GeaflowVertex> vertices = parse(rests);
        List<GeaflowVertex> existVertices = ListUtil.convert(exists, e -> vertexMap.get(e.getId()));
        vertices.addAll(existVertices);
        return vertices;
    }

    public List<GeaflowVertex> getVerticesByEdgeId(String edgeId) {
        // get vertexEntity
        List<VertexEntity> vertices = vertexDao.getByEdge(edgeId);
        return parse(vertices);
    }

}
