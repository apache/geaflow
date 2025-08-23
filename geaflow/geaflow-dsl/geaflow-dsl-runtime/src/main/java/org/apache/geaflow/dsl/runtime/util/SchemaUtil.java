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

package org.apache.geaflow.dsl.runtime.util;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.BinaryRow;
import org.apache.geaflow.dsl.common.data.impl.DefaultPath;
import org.apache.geaflow.dsl.common.data.impl.VertexEdgeFactory;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.types.PathType;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.VertexType;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.runtime.traversal.data.FieldAlignEdge;
import org.apache.geaflow.dsl.runtime.traversal.data.FieldAlignVertex;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.EdgeTable;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.VertexTable;
import org.apache.geaflow.dsl.util.SqlTypeUtil;
import org.apache.geaflow.model.graph.meta.GraphMetaType;
import org.apache.geaflow.view.GraphViewBuilder;
import org.apache.geaflow.view.IViewDesc.BackendType;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.apache.geaflow.view.meta.ViewMetaBookKeeper;

public class SchemaUtil {

    public static final String VERTEX_EDGE_CONSTRUCTOR_FIELD = "CONSTRUCTOR";

    public static GraphMetaType buildGraphMeta(GeaFlowGraph graph) {
        Map<String, VertexType> vertexTypes = getVertexTypes(graph);
        Map<String, EdgeType> edgeTypes = getEdgeTypes(graph);
        RowVertex vertex = VertexEdgeFactory.createVertex(vertexTypes.values().iterator().next());
        RowEdge edge = VertexEdgeFactory.createEdge(edgeTypes.values().iterator().next());

        try {
            Field vertexConstructorField = vertex.getClass().getField(VERTEX_EDGE_CONSTRUCTOR_FIELD);
            Field edgeConstructorField = edge.getClass().getField(VERTEX_EDGE_CONSTRUCTOR_FIELD);
            Supplier<?> vertexConstructor = (Supplier<?>) vertexConstructorField.get(null);
            Supplier<?> edgeConstructor = (Supplier<?>) edgeConstructorField.get(null);
            return new GraphMetaType(graph.getIdType(), vertex.getClass(), vertexConstructor,
                BinaryRow.class, edge.getClass(), edgeConstructor, BinaryRow.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new GeaFlowDSLException("Fail to get vertex or edge constructor", e);
        }
    }


    public static GraphViewDesc buildGraphViewDesc(GeaFlowGraph graph, Configuration conf) {
        Configuration graphConfig = new Configuration(graph.getConfig().getConfigMap());
        long latestVersion = getGraphLatestVersion(graph, conf);
        BackendType storeType = BackendType.of(graph.getStoreType());
        return GraphViewBuilder.createGraphView(graph.getUniqueName())
            .withShardNum(graph.getShardCount())
            .withBackend(storeType)
            .withLatestVersion(latestVersion)
            .withProps(graphConfig.getConfigMap())
            .withSchema(buildGraphMeta(graph))
            .build();
    }

    private static long getGraphLatestVersion(GeaFlowGraph graph, Configuration conf) {
        try {
            Configuration globalConfig = graph.getConfigWithGlobal(conf);
            ViewMetaBookKeeper keeper = new ViewMetaBookKeeper(graph.getUniqueName(), globalConfig);
            return keeper.getLatestViewVersion(graph.getUniqueName());
        } catch (IOException e) {
            throw new GeaFlowDSLException(e);
        }
    }

    public static Map<String, VertexType> getVertexTypes(GeaFlowGraph graph) {
        GQLJavaTypeFactory typeFactory = GQLJavaTypeFactory.create();

        Map<String, VertexType> vertexTypes = new HashMap<>();
        for (VertexTable vertexTable : graph.getVertexTables()) {
            VertexType vertexType = (VertexType) SqlTypeUtil.convertType(vertexTable.getRowType(typeFactory));
            vertexTypes.put(vertexTable.getTypeName(), vertexType);
        }
        return vertexTypes;
    }

    public static Map<String, EdgeType> getEdgeTypes(GeaFlowGraph graph) {
        GQLJavaTypeFactory typeFactory = GQLJavaTypeFactory.create();

        Map<String, EdgeType> edgeTypes = new HashMap<>();
        for (EdgeTable edgeTable : graph.getEdgeTables()) {
            EdgeType edgeType = (EdgeType) SqlTypeUtil.convertType(edgeTable.getRowType(typeFactory));
            edgeTypes.put(edgeTable.getTypeName(), edgeType);
        }
        return edgeTypes;
    }

    private static int[] getFieldMappingIndices(StructType inputType, StructType outputType) {
        int[] mapping = new int[outputType.size()];
        for (int i = 0; i < outputType.size(); i++) {
            String outputField = outputType.getField(i).getName();
            mapping[i] = inputType.indexOf(outputField);
            if (mapping[i] < 0) {
                switch (outputField) {
                    case VertexType.DEFAULT_ID_FIELD_NAME:
                        mapping[i] = VertexType.ID_FIELD_POSITION;
                        break;
                    case EdgeType.DEFAULT_SRC_ID_NAME:
                        mapping[i] = EdgeType.SRC_ID_FIELD_POSITION;
                        break;
                    case EdgeType.DEFAULT_TARGET_ID_NAME:
                        mapping[i] = EdgeType.TARGET_ID_FIELD_POSITION;
                        break;
                    case EdgeType.DEFAULT_TS_NAME:
                        mapping[i] = EdgeType.TIME_FIELD_POSITION;
                        break;
                    default:
                        if (mapping[i] < -1) {
                            throw new GeaFlowDSLException("Cannot find field {}, illegal index {}",
                                outputField, mapping[i]);
                        }
                }
            }
        }
        return mapping;
    }

    public static RowVertex alignToVertexSchema(RowVertex vertex, VertexType inputVertexType,
                                                VertexType outputVertexType) {
        if (vertex == null) {
            return null;
        }
        if (inputVertexType.equals(outputVertexType)) {
            return vertex;
        }
        int[] mapping = getFieldMappingIndices(inputVertexType, outputVertexType);
        return FieldAlignVertex.createFieldAlignedVertex(vertex, mapping);
    }

    public static RowEdge alignToEdgeSchema(RowEdge edge, EdgeType inputEdgeType,
                                            EdgeType outputEdgeType) {
        if (edge == null) {
            return null;
        }
        if (inputEdgeType.equals(outputEdgeType)) {
            return edge;
        }
        int[] mapping = getFieldMappingIndices(inputEdgeType, outputEdgeType);
        return FieldAlignEdge.createFieldAlignedEdge(edge, mapping);
    }

    public static Path alignToPathSchema(Path path, PathType inputPathType,
                                         PathType outputPathType) {
        if (path == null) {
            return null;
        }
        int[] mapping = getFieldMappingIndices(inputPathType, outputPathType);

        List<Row> pathNodes = new ArrayList<>(path.size());
        for (int i = 0; i < mapping.length; i++) {
            int index = mapping[i];
            if (index >= 0) {
                IType<?> nodeType = inputPathType.getType(index);
                IType<?> outputType = outputPathType.getType(i);
                Row node = path.getField(index, nodeType);

                if (nodeType instanceof VertexType) {
                    node = alignToVertexSchema((RowVertex) node, (VertexType) nodeType,
                        (VertexType) outputType);
                } else if (nodeType instanceof EdgeType) {
                    node = alignToEdgeSchema((RowEdge) node, (EdgeType) nodeType,
                        (EdgeType) outputType);
                } else {
                    throw new IllegalArgumentException("Illegal node type: " + nodeType);
                }
                pathNodes.add(node);
            } else {
                pathNodes.add(null);
            }
        }
        return new DefaultPath(pathNodes);
    }
}
