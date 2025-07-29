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

package org.apache.geaflow.dsl.catalog.console;

import static org.apache.geaflow.dsl.util.SqlTypeUtil.convertTypeName;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.catalog.console.GraphModel.Endpoint;
import org.apache.geaflow.dsl.common.descriptor.EdgeDescriptor;
import org.apache.geaflow.dsl.common.descriptor.GraphDescriptor;
import org.apache.geaflow.dsl.common.descriptor.NodeDescriptor;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.schema.GeaFlowFunction;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.EdgeTable;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.VertexTable;
import org.apache.geaflow.dsl.schema.GeaFlowTable;

public class CatalogUtil {

    public static TableModel convertToTableModel(GeaFlowTable table) {
        TableModel tableModel = new TableModel();
        PluginConfigModel pluginConfigModel = new PluginConfigModel();
        pluginConfigModel.setType(table.getTableType());
        pluginConfigModel.setConfig(convertToTableModelConfig(table.getConfig()));
        tableModel.setPluginConfig(pluginConfigModel);
        tableModel.setName(table.getName());
        List<TableField> fields = table.getFields();
        List<FieldModel> fieldModelList = convertToFieldModel(fields);
        tableModel.setFields(fieldModelList);
        return tableModel;
    }

    public static GeaFlowTable convertToGeaFlowTable(TableModel model, String instanceName) {
        if (model == null) {
            return null;
        }
        List<FieldModel> fieldModels = model.getFields();
        List<TableField> fields = convertToTableField(fieldModels);
        return new GeaFlowTable(instanceName, model.getName(), fields, new ArrayList<>(),
            new ArrayList<>(), convertToGeaFlowTableConfig(model.getPluginConfig()), true, false);
    }

    public static VertexModel convertToVertexModel(VertexTable table) {
        VertexModel vertexModel = new VertexModel();
        vertexModel.setName(table.getTypeName());
        List<TableField> fields = table.getFields();
        String idFieldName = table.getIdFieldName();
        List<FieldModel> fieldModels = new ArrayList<>(fields.size());
        for (TableField field : fields) {
            GeaFlowFieldCategory fieldCategory;
            if (field.getName().equals(idFieldName)) {
                fieldCategory = GeaFlowFieldCategory.VERTEX_ID;
            } else {
                fieldCategory = GeaFlowFieldCategory.PROPERTY;
            }
            fieldModels.add(new FieldModel(field.getName(), null,
                GeaFlowFieldType.getFieldType(field.getType()), fieldCategory));
        }
        vertexModel.setFields(fieldModels);
        return vertexModel;
    }

    public static VertexTable convertToVertexTable(String instanceName, VertexModel model) {
        if (model == null) {
            return null;
        }
        List<FieldModel> fieldModels = model.getFields();
        List<TableField> fields = new ArrayList<>(fieldModels.size());
        String idFieldName = null;
        for (FieldModel fieldModel : fieldModels) {
            if (fieldModel.getCategory() == GeaFlowFieldCategory.VERTEX_ID) {
                idFieldName = fieldModel.getName();
            }
            String typeName = convertTypeName(fieldModel.getType().name());
            IType<?> fieldType = Types.of(typeName);
            TableField field = new TableField(fieldModel.getName(), fieldType, false);
            fields.add(field);
        }
        return new VertexTable(instanceName, model.getName(), fields, idFieldName);
    }

    public static EdgeModel convertToEdgeModel(EdgeTable table) {
        EdgeModel edgeModel = new EdgeModel();
        edgeModel.setName(table.getTypeName());
        List<TableField> fields = table.getFields();
        List<FieldModel> fieldModels = new ArrayList<>(fields.size());
        String srcIdFieldName = table.getSrcIdFieldName();
        String targetIdFieldName = table.getTargetIdFieldName();
        String timestampFieldName = table.getTimestampFieldName();
        for (TableField field : fields) {
            GeaFlowFieldCategory fieldCategory;
            if (field.getName().equals(srcIdFieldName)) {
                fieldCategory = GeaFlowFieldCategory.EDGE_SOURCE_ID;
            } else if (field.getName().equals(targetIdFieldName)) {
                fieldCategory = GeaFlowFieldCategory.EDGE_TARGET_ID;
            } else if (field.getName().equals(timestampFieldName)) {
                fieldCategory = GeaFlowFieldCategory.EDGE_TIMESTAMP;
            } else {
                fieldCategory = GeaFlowFieldCategory.PROPERTY;
            }
            fieldModels.add(new FieldModel(field.getName(), null,
                GeaFlowFieldType.getFieldType(field.getType()), fieldCategory));
        }
        edgeModel.setFields(fieldModels);
        return edgeModel;
    }

    public static EdgeTable convertToEdgeTable(String instanceName, EdgeModel model) {
        if (model == null) {
            return null;
        }
        List<FieldModel> fieldModels = model.getFields();
        List<TableField> fields = new ArrayList<>(fieldModels.size());
        String srcIdFieldName = null;
        String targetIdFieldName = null;
        String timestampFieldName = null;
        for (FieldModel fieldModel : fieldModels) {
            switch (fieldModel.getCategory()) {
                case EDGE_SOURCE_ID:
                    srcIdFieldName = fieldModel.getName();
                    break;
                case EDGE_TARGET_ID:
                    targetIdFieldName = fieldModel.getName();
                    break;
                case EDGE_TIMESTAMP:
                    timestampFieldName = fieldModel.getName();
                    break;
                default:
            }
            String typeName = convertTypeName(fieldModel.getType().name());
            IType<?> fieldType = Types.of(typeName);
            TableField field = new TableField(fieldModel.getName(), fieldType, false);
            fields.add(field);
        }
        return new EdgeTable(instanceName, model.getName(), fields, srcIdFieldName, targetIdFieldName,
            timestampFieldName);
    }

    public static GraphModel convertToGraphModel(GeaFlowGraph graph) {
        GraphModel graphModel = new GraphModel();
        PluginConfigModel pluginConfigModel = new PluginConfigModel();
        pluginConfigModel.setType(graph.getStoreType());
        pluginConfigModel.setConfig(convertToGraphModelConfig(graph.getConfig().getConfigMap()));

        graphModel.setPluginConfig(pluginConfigModel);
        List<VertexTable> vertexTables = graph.getVertexTables();
        List<VertexModel> vertexModels = new ArrayList<>(vertexTables.size());
        for (VertexTable vertexTable : vertexTables) {
            vertexModels.add(convertToVertexModel(vertexTable));
        }
        graphModel.setVertices(vertexModels);
        List<EdgeTable> edgeTables = graph.getEdgeTables();
        List<EdgeModel> edgeModels = new ArrayList<>(edgeTables.size());
        for (EdgeTable edgeTable : edgeTables) {
            edgeModels.add(convertToEdgeModel(edgeTable));
        }
        graphModel.setEdges(edgeModels);
        graphModel.setName(graph.getName());
        return graphModel;
    }

    public static GeaFlowGraph convertToGeaFlowGraph(GraphModel model, String instanceName) {
        if (model == null) {
            return null;
        }
        List<VertexModel> vertices = model.getVertices();
        List<VertexTable> vertexTables = new ArrayList<>(vertices.size());
        for (VertexModel vertexModel : vertices) {
            vertexTables.add(convertToVertexTable(instanceName, vertexModel));
        }
        GraphDescriptor desc = new GraphDescriptor();
        for (VertexTable vertex : vertexTables) {
            desc.addNode(new NodeDescriptor(desc.getIdName(model.getName()),
                vertex.getTypeName()));
        }

        List<EdgeModel> edges = model.getEdges();
        List<EdgeTable> edgeTables = new ArrayList<>(edges.size());
        for (EdgeModel edgeModel : edges) {
            edgeTables.add(convertToEdgeTable(instanceName, edgeModel));
        }
        if (model.getEndpoints() != null) {
            for (Endpoint endpoint : model.getEndpoints()) {
                desc.addEdge(new EdgeDescriptor(desc.getIdName(model.getName()),
                    endpoint.getEdgeName(), endpoint.getSourceName(),
                    endpoint.getTargetName()));
            }
        }
        GeaFlowGraph geaFlowGraph = new GeaFlowGraph(instanceName, model.getName(), vertexTables,
            edgeTables, convertToGeaFlowGraphConfig(model.getPluginConfig()), Collections.emptyMap(),
            true, false);
        geaFlowGraph.setDescriptor(geaFlowGraph.getValidDescriptorInGraph(desc));
        return geaFlowGraph;
    }

    public static GeaFlowFunction convertToGeaFlowFunction(FunctionModel model) {
        return GeaFlowFunction.of(model.getName(), Lists.newArrayList(model.getEntryClass()));
    }

    private static List<FieldModel> convertToFieldModel(List<TableField> fields) {
        List<FieldModel> fieldModelList = new ArrayList<>(fields.size());
        for (TableField field : fields) {
            FieldModel fieldModel = new FieldModel(field.getName(), null,
                GeaFlowFieldType.getFieldType(field.getType()), GeaFlowFieldCategory.PROPERTY);
            fieldModelList.add(fieldModel);
        }
        return fieldModelList;
    }

    private static List<TableField> convertToTableField(List<FieldModel> fieldModels) {
        List<TableField> fields = new ArrayList<>(fieldModels.size());
        for (FieldModel fieldModel : fieldModels) {
            String typeName = convertTypeName(fieldModel.getType().name());
            IType<?> fieldType = Types.of(typeName);
            TableField field = new TableField(fieldModel.getName(), fieldType, false);
            fields.add(field);
        }
        return fields;
    }

    private static Map<String, String> convertToTableModelConfig(Map<String, String> tableConfig) {
        Map<String, String> modelConfig = new HashMap<>(tableConfig);
        modelConfig.remove(DSLConfigKeys.GEAFLOW_DSL_TABLE_TYPE.getKey());
        return modelConfig;
    }

    private static Map<String, String> convertToGeaFlowTableConfig(PluginConfigModel configModel) {
        Map<String, String> tableConfig = new HashMap<>(configModel.getConfig());
        tableConfig.put(DSLConfigKeys.GEAFLOW_DSL_TABLE_TYPE.getKey(),
            configModel.getType());
        return tableConfig;
    }

    private static Map<String, String> convertToGraphModelConfig(Map<String, String> graphConfig) {
        Map<String, String> modelConfig = new HashMap<>(graphConfig);
        modelConfig.remove(DSLConfigKeys.GEAFLOW_DSL_STORE_TYPE.getKey());
        return modelConfig;
    }

    private static Map<String, String> convertToGeaFlowGraphConfig(PluginConfigModel configModel) {
        Map<String, String> graphConfig = new HashMap<>(configModel.getConfig());
        graphConfig.put(DSLConfigKeys.GEAFLOW_DSL_STORE_TYPE.getKey(),
            configModel.getType());
        return graphConfig;
    }
}
