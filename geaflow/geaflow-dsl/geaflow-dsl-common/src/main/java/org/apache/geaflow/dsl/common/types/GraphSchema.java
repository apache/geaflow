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

package org.apache.geaflow.dsl.common.types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;

public class GraphSchema extends StructType {

    public static final String LABEL_FIELD_NAME = "~label";

    private final String graphName;

    public GraphSchema(String graphName, List<TableField> fields) {
        super(fields);
        this.graphName = Objects.requireNonNull(graphName);
    }

    @Override
    public String getName() {
        return Types.TYPE_NAME_GRAPH;
    }

    public String getGraphName() {
        return graphName;
    }

    public VertexType getVertex(String name) {
        return (VertexType) getField(name).getType();
    }

    public EdgeType getEdge(String name) {
        return (EdgeType) getField(name).getType();
    }

    public List<VertexType> getVertices() {
        List<VertexType> vertexTypes = new ArrayList<>();
        for (TableField field : fields) {
            if (field.getType() instanceof VertexType) {
                vertexTypes.add((VertexType) field.getType());
            }
        }
        return vertexTypes;
    }

    public List<EdgeType> getEdges() {
        List<EdgeType> edgeTypes = new ArrayList<>();
        for (TableField field : fields) {
            if (field.getType() instanceof EdgeType) {
                edgeTypes.add((EdgeType) field.getType());
            }
        }
        return edgeTypes;
    }

    public IType<?> getIdType() {
        List<VertexType> vertexTypes = getVertices();
        assert vertexTypes.size() > 0 : "Empty graph";
        return vertexTypes.get(0).getId().getType();
    }

    public List<TableField> getAddingFields(GraphSchema baseSchema) {
        if (baseSchema.equals(this)) {
            return Collections.emptyList();
        }
        // the global vertex variable is applied to all the vertex tables.
        // so any of the vertex table has the same adding fields.
        VertexType vertexType = getVertices().get(0);
        VertexType baseVertex = baseSchema.getVertices().get(0);
        assert vertexType != null && baseVertex != null;
        return vertexType.getAddingFields(baseVertex);
    }


    @Override
    public int compare(Row x, Row y) {
        throw new GeaFlowDSLException("Illegal call.");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GraphSchema)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        GraphSchema that = (GraphSchema) o;
        return Objects.equals(graphName, that.graphName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), graphName);
    }

    @Override
    public GraphSchema merge(StructType other) {
        assert other instanceof GraphSchema : "GraphSchema should merge with graph schema";
        assert graphName.equals(((GraphSchema) other).graphName) : "Cannot merge with different graph schema";

        List<TableField> mergedFields = new ArrayList<>(fields);
        List<String> mergedFieldNames = fields.stream().map(TableField::getName)
            .collect(Collectors.toList());
        for (TableField field : other.fields) {
            int index = mergedFieldNames.indexOf(field.getName());
            if (index >= 0) {
                StructType thisType = (StructType) mergedFields.get(index).getType();
                StructType thatType = (StructType) field.getType();
                StructType mergedType = thisType.merge(thatType);

                mergedFields.set(index, field.copy(mergedType));
            } else {
                mergedFields.add(field);
                mergedFieldNames.add(field.getName());
            }
        }
        return new GraphSchema(graphName, mergedFields);
    }
}
