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

package org.apache.geaflow.analytics.service.query;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.geaflow.cluster.response.ResponseResult;
import org.apache.geaflow.dsl.calcite.MetaFieldType;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.model.graph.IGraphElementWithLabelField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultResultSetFormatUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultResultSetFormatUtils.class);

    private static final String VERTEX = "nodes";
    private static final String EDGE = "edges";

    private static final String VIEW_RESULT = "viewResult";

    private static final String JSON_RESULT = "jsonResult";

    public static String formatResult(Object queryResult, RelDataType currentResultType) {
        final JSONObject finalResult = new JSONObject();
        JSONArray jsonResult = new JSONArray();
        JSONObject viewResult = new JSONObject();
        List<ViewVertex> vertices = new ArrayList<>();
        List<ViewEdge> edges = new ArrayList<>();
        List<List<ResponseResult>> list = (List<List<ResponseResult>>) queryResult;

        for (List<ResponseResult> responseResults : list) {
            for (ResponseResult responseResult : responseResults) {
                for (Object o : responseResult.getResponse()) {
                    jsonResult.add(formatRow(o, currentResultType, vertices, edges));
                }
            }
        }

        List<ViewVertex> filteredVertices =
            vertices.stream().collect(Collectors.collectingAndThen(Collectors.toCollection(() -> new TreeSet<>(
                Comparator.comparing(ViewVertex::getId))), ArrayList::new));

        viewResult.put(VERTEX, filteredVertices);
        viewResult.put(EDGE, edges);
        finalResult.put(VIEW_RESULT, viewResult);
        finalResult.put(JSON_RESULT, jsonResult);
        return JSON.toJSONString(finalResult, SerializerFeature.DisableCircularReferenceDetect);
    }

    private static Object formatRow(Object o, RelDataType currentResultType, List<ViewVertex> vertices, List<ViewEdge> edges) {
        if (o == null) {
            return null;
        }
        if (o instanceof ObjectRow) {
            JSONObject jsonObject = new JSONObject();
            ObjectRow objectRow = (ObjectRow) o;
            Object[] fields = objectRow.getFields();
            for (int i = 0; i < fields.length; i++) {
                RelDataTypeField relDataTypeField = currentResultType.getFieldList().get(i);
                Object field = fields[i];
                Object formatResult;
                if (field instanceof RowVertex) {
                    RowVertex vertex = (RowVertex) field;
                    ObjectRow vertexValue = (ObjectRow) vertex.getValue();
                    Map<String, Object> properties = new HashMap<>();
                    if (vertexValue != null) {
                        Object[] vertexProperties = vertexValue.getFields();
                        int metaFieldCount = getMetaFieldCount(relDataTypeField.getType());
                        List<RelDataTypeField> typeList = relDataTypeField.getType().getFieldList();
                        // Find the correspond key in properties.
                        for (int j = 0; j < vertexProperties.length; j++) {
                            properties.put(typeList.get(j + metaFieldCount).getName(), vertexProperties[j]);
                        }
                    }

                    formatResult = new ViewVertex(String.valueOf(vertex.getId()), getLabel(vertex), properties);
                    vertices.add((ViewVertex) formatResult);
                } else if (field instanceof RowEdge) {
                    RowEdge edge = (RowEdge) field;
                    ObjectRow edgeValue = (ObjectRow) edge.getValue();
                    Map<String, Object> properties = new HashMap<>();
                    if (edgeValue != null) {
                        Object[] edgeProperties = edgeValue.getFields();
                        int metaFieldCount = getMetaFieldCount(relDataTypeField.getType());
                        List<RelDataTypeField> typeList = relDataTypeField.getType().getFieldList();
                        for (int j = 0; j < edgeProperties.length; j++) {
                            properties.put(typeList.get(j + metaFieldCount).getName(), edgeProperties[j]);
                        }
                    }
                    formatResult = new ViewEdge(String.valueOf(edge.getSrcId()), String.valueOf(edge.getTargetId()),
                        getLabel(edge), properties, edge.getDirect().name());
                    edges.add((ViewEdge) formatResult);
                } else {
                    formatResult = field.toString();
                }
                jsonObject.put(relDataTypeField.getKey(), formatResult);
            }

            return jsonObject;
        } else {
            return o.toString();
        }
    }

    private static String getLabel(IGraphElementWithLabelField field) {
        try {
            return field.getLabel();
        } catch (Exception e) {
            LOGGER.warn("field {} get label error", field, e);
            return null;
        }
    }

    public static int getMetaFieldCount(RelDataType type) {
        List<RelDataTypeField> fieldList = type.getFieldList();
        int count = 0;
        for (RelDataTypeField relDataTypeField : fieldList) {
            if (!(relDataTypeField.getType() instanceof MetaFieldType)) {
                break;
            }
            count++;
        }
        return count;
    }
}
