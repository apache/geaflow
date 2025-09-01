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

package org.apache.geaflow.dsl.schema;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.calcite.EdgeRecordType;
import org.apache.geaflow.dsl.calcite.GraphRecordType;
import org.apache.geaflow.dsl.calcite.VertexRecordType;
import org.apache.geaflow.dsl.common.descriptor.GraphDescriptor;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.util.SqlTypeUtil;

public class GeaFlowGraph extends AbstractTable implements Serializable {

    private final String instanceName;
    private final String name;
    private final List<VertexTable> vertexTables;
    private final List<EdgeTable> edgeTables;
    private final Map<String, String> usingTables;
    private final Map<String, String> config;
    private final boolean ifNotExists;
    private final boolean isTemporary;
    private GraphDescriptor graphDescriptor;

    public GeaFlowGraph(String instanceName, String name, List<VertexTable> vertexTables,
                        List<EdgeTable> edgeTables, Map<String, String> config,
                        Map<String, String> usingTables, boolean ifNotExists, boolean isTemporary) {
        this.instanceName = instanceName;
        this.name = name;
        this.vertexTables = vertexTables;
        this.edgeTables = edgeTables;
        this.config = new HashMap<>(config);
        this.usingTables = ImmutableMap.copyOf(usingTables);
        this.ifNotExists = ifNotExists;
        this.isTemporary = isTemporary;

        for (VertexTable vertexTable : this.vertexTables) {
            vertexTable.setGraph(this);
        }
        for (EdgeTable edgeTable : this.edgeTables) {
            edgeTable.setGraph(this);
        }
        this.validate();
    }

    public GeaFlowGraph(String instanceName, String name, List<VertexTable> vertexTables,
                        List<EdgeTable> edgeTables, Map<String, String> config,
                        Map<String, String> usingTables, boolean ifNotExists, boolean isTemporary,
                        GraphDescriptor descriptor) {
        this(instanceName, name, vertexTables, edgeTables, config, usingTables, ifNotExists, isTemporary);
        this.graphDescriptor = Objects.requireNonNull(descriptor);
    }

    public void validate() {
        if (this.vertexTables.size() > 0) {
            TableField commonVertexIdField = this.vertexTables.get(0).getIdField();
            for (VertexTable vertexTable : this.vertexTables) {
                if (!vertexTable.getIdField().getType().equals(commonVertexIdField.getType())) {
                    throw new GeaFlowDSLException("Id field type should be same between vertex " + "tables");
                }
            }
        }
        if (this.edgeTables.size() > 0) {
            TableField commonSrcIdField = this.edgeTables.get(0).getSrcIdField();
            TableField commonTargetIdField = this.edgeTables.get(0).getTargetIdField();
            Optional<TableField> commonTsField =
                Optional.ofNullable(this.edgeTables.get(0).getTimestampField());
            for (EdgeTable edgeTable : this.edgeTables) {
                if (!edgeTable.getSrcIdField().getType().equals(commonSrcIdField.getType())) {
                    throw new GeaFlowDSLException("SOURCE ID field type should be same between edge "
                        + "tables");
                } else if (!edgeTable.getTargetIdField().getType().equals(commonTargetIdField.getType())) {
                    throw new GeaFlowDSLException("DESTINATION ID field type should be same "
                        + "between edge tables");
                }

                if (commonTsField.isPresent()) {
                    if (edgeTable.getTimestampField() == null) {
                        throw new GeaFlowDSLException("TIMESTAMP should defined or not defined in all edge tables");
                    } else if (!edgeTable.getTimestampField().getType().equals(commonTsField.get().getType())) {
                        throw new GeaFlowDSLException("TIMESTAMP field type should be same between edge "
                            + "tables");
                    }
                } else {
                    if (edgeTable.getTimestampField() != null) {
                        throw new GeaFlowDSLException("TIMESTAMP should defined or not defined in all edge tables");
                    }
                }
            }
        }
    }

    public boolean containTable(GeaFlowTable table) {
        if (table == null) {
            return false;
        }
        if (table instanceof VertexTable) {
            return this.getVertexTables().stream().anyMatch(v -> v.getName().equals(table.getName()));
        }
        if (table instanceof EdgeTable) {
            return this.getEdgeTables().stream().anyMatch(v -> v.getName().equals(table.getName()));
        }
        return false;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        List<RelDataTypeField> fields = new ArrayList<>();
        for (VertexTable table : vertexTables) {
            VertexRecordType type = table.getRowType(typeFactory);
            fields.add(new RelDataTypeFieldImpl(table.getTypeName(), fields.size(), type));
        }
        for (EdgeTable table : edgeTables) {
            EdgeRecordType type = table.getRowType(typeFactory);
            fields.add(new RelDataTypeFieldImpl(table.getTypeName(), fields.size(), type));
        }
        return new GraphRecordType(name, fields);
    }

    public GraphSchema getGraphSchema(RelDataTypeFactory typeFactory) {
        return (GraphSchema) SqlTypeUtil.convertType(getRowType(typeFactory));
    }

    public String getInstanceName() {
        return instanceName;
    }

    public String getName() {
        return name;
    }

    public String getUniqueName() {
        return instanceName + "_" + name;
    }

    public List<VertexTable> getVertexTables() {
        return vertexTables;
    }

    public List<EdgeTable> getEdgeTables() {
        return edgeTables;
    }

    public Configuration getConfig() {
        return new Configuration(config);
    }

    public GeaFlowGraph setDescriptor(GraphDescriptor desc) {
        this.graphDescriptor = Objects.requireNonNull(desc);
        return this;
    }

    public GraphDescriptor getValidDescriptorInGraph(GraphDescriptor desc) {
        GraphDescriptor newDesc = new GraphDescriptor();
        newDesc.addNode(desc.nodes.stream().filter(
            node -> this.vertexTables.stream().anyMatch(v -> v.getTypeName().equals(node.type))
        ).collect(Collectors.toList()));
        newDesc.addEdge(desc.edges.stream().filter(
            edge -> {
                EdgeTable edgeTable = null;
                for (EdgeTable e : this.getEdgeTables()) {
                    if (e.getTypeName().equals(edge.type)) {
                        edgeTable = e;
                        break;
                    }
                }
                VertexTable sourceVertexTable = null;
                for (VertexTable v : this.getVertexTables()) {
                    if (v.getTypeName().equals(edge.sourceType)) {
                        sourceVertexTable = v;
                        break;
                    }
                }
                VertexTable targetVertexTable = null;
                for (VertexTable v : this.getVertexTables()) {
                    if (v.getTypeName().equals(edge.targetType)) {
                        targetVertexTable = v;
                        break;
                    }
                }
                boolean exist = edgeTable != null
                    && sourceVertexTable != null && targetVertexTable != null;
                return exist && edgeTable.getSrcIdField().getType().equals(sourceVertexTable.getIdField().getType())
                    && edgeTable.getTargetIdField().getType().equals(targetVertexTable.getIdField().getType());
            }
        ).collect(Collectors.toList()));
        return newDesc;
    }

    public GraphDescriptor getDescriptor() {
        return graphDescriptor == null ? new GraphDescriptor() : graphDescriptor;
    }

    public Configuration getConfigWithGlobal(Configuration globalConf) {
        Map<String, String> conf = new HashMap<>(globalConf.getConfigMap());
        conf.putAll(this.config);
        return new Configuration(conf);
    }

    public Configuration getConfigWithGlobal(Map<String, String> globalConf, Map<String, String> setOptions) {
        Map<String, String> conf = new HashMap<>(globalConf);
        conf.putAll(this.config);
        conf.putAll(setOptions);
        return new Configuration(conf);
    }

    public String getStoreType() {
        return Configuration.getString(DSLConfigKeys.GEAFLOW_DSL_STORE_TYPE, config);
    }

    public int getShardCount() {
        return Configuration.getInteger(DSLConfigKeys.GEAFLOW_DSL_STORE_SHARD_COUNT, config);
    }

    public IType<?> getIdType() {
        VertexTable vertexTable = vertexTables.iterator().next();
        return vertexTable.getIdField().getType();
    }

    public IType<?> getLabelType() {
        return Types.STRING;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    public GraphElementTable getTable(String tableName) {
        for (VertexTable vertexTable : vertexTables) {
            if (vertexTable.getTypeName().equalsIgnoreCase(tableName)) {
                return vertexTable;
            }
        }
        for (EdgeTable edgeTable : edgeTables) {
            if (edgeTable.getTypeName().equalsIgnoreCase(tableName)) {
                return edgeTable;
            }
        }
        return null;
    }

    public Map<String, String> getUsingTables() {
        return usingTables;
    }

    public static class VertexTable extends GeaFlowTable implements GraphElementTable, Serializable {

        private final String idField;

        private GeaFlowGraph graph;

        public VertexTable(String instanceName, String typeName, List<TableField> fields, String idField) {
            super(instanceName, typeName, fields, Collections.singletonList(idField),
                Collections.emptyList(), new HashMap<>(), true, true);
            this.idField = Objects.requireNonNull(idField);
            checkFields();
        }

        private void checkFields() {
            for (TableField field : getFields()) {
                GraphRecordType.validateFieldName(field.getName());
            }
            Set<String> fieldNames = getFields().stream().map(TableField::getName)
                .collect(Collectors.toSet());
            if (fieldNames.size() != super.getFields().size()) {
                throw new GeaFlowDSLException("Duplicate field has found in vertex table: " + getName());
            }
            if (!fieldNames.contains(idField)) {
                throw new GeaFlowDSLException("id field:'" + idField + "' is not found in the fields: " + fieldNames);
            }
        }

        public void setGraph(GeaFlowGraph graph) {
            this.graph = graph;
        }

        @Override
        public GeaFlowGraph getGraph() {
            return graph;
        }

        @Override
        public String getTypeName() {
            return getName();
        }


        public TableField getIdField() {
            return findField(getFields(), idField);
        }

        public String getIdFieldName() {
            return idField;
        }

        @Override
        public VertexRecordType getRowType(RelDataTypeFactory typeFactory) {
            List<RelDataTypeField> dataFields = new ArrayList<>(getFields().size());
            for (int i = 0; i < getFields().size(); i++) {
                TableField field = getFields().get(i);
                RelDataType type = SqlTypeUtil.convertToRelType(field.getType(), field.isNullable(), typeFactory);
                RelDataTypeField dataField = new RelDataTypeFieldImpl(field.getName(), i, type);
                dataFields.add(dataField);
            }
            return VertexRecordType.createVertexType(dataFields, idField, typeFactory);
        }

        @Override
        public String toString() {
            return "VertexTable{" + "typeName='" + getTypeName() + '\'' + ", fields=" + getFields()
                + ", idField='" + idField + '\'' + '}';
        }
    }

    public static class EdgeTable extends GeaFlowTable implements GraphElementTable, Serializable {

        private final String srcIdField;
        private final String targetIdField;
        private final String timestampField;

        private GeaFlowGraph graph;

        public EdgeTable(String instanceName, String typeName, List<TableField> fields, String srcIdField,
                         String targetIdField, String timestampField) {
            super(instanceName, typeName, fields, Lists.newArrayList(srcIdField, targetIdField),
                Collections.emptyList(), new HashMap<>(), true, true);
            this.srcIdField = Objects.requireNonNull(srcIdField);
            this.targetIdField = Objects.requireNonNull(targetIdField);
            this.timestampField = timestampField;
            checkFields();
        }

        private void checkFields() {
            for (TableField field : getFields()) {
                GraphRecordType.validateFieldName(field.getName());
            }
            Set<String> fieldNames = getFields().stream().map(TableField::getName)
                .collect(Collectors.toSet());
            if (fieldNames.size() != getFields().size()) {
                throw new GeaFlowDSLException("Duplicate field has found in edge table: " + getName());
            }
            if (!fieldNames.contains(srcIdField)) {
                throw new GeaFlowDSLException("source id:" + srcIdField + " is not found in fields: " + fieldNames);
            }
            if (!fieldNames.contains(targetIdField)) {
                throw new GeaFlowDSLException(
                    "target id:" + targetIdField + " is not found in fields: " + fieldNames);
            }
        }

        public void setGraph(GeaFlowGraph graph) {
            this.graph = graph;
        }

        @Override
        public GeaFlowGraph getGraph() {
            return graph;
        }

        @Override
        public String getTypeName() {
            return getName();
        }

        public TableField getSrcIdField() {
            return findField(getFields(), srcIdField);
        }

        public TableField getTargetIdField() {
            return findField(getFields(), targetIdField);
        }

        public TableField getTimestampField() {
            if (timestampField == null) {
                return null;
            }
            return findField(getFields(), timestampField);
        }

        public String getSrcIdFieldName() {
            return srcIdField;
        }

        public String getTargetIdFieldName() {
            return targetIdField;
        }

        public String getTimestampFieldName() {
            return timestampField;
        }

        @Override
        public EdgeRecordType getRowType(RelDataTypeFactory typeFactory) {
            List<RelDataTypeField> dataFields = new ArrayList<>(getFields().size());
            for (int i = 0; i < getFields().size(); i++) {
                TableField field = getFields().get(i);
                RelDataType type = SqlTypeUtil.convertToRelType(field.getType(), field.isNullable(), typeFactory);
                RelDataTypeField dataField = new RelDataTypeFieldImpl(field.getName(), i, type);
                dataFields.add(dataField);
            }
            return EdgeRecordType.createEdgeType(dataFields, srcIdField, targetIdField, timestampField, typeFactory);
        }

        @Override
        public String toString() {
            return "EdgeTable{" + "typeName='" + getName() + '\'' + ", fields=" + getFields()
                + ", srcIdField='" + srcIdField + '\'' + ", targetIdField='" + targetIdField + '\''
                + ", timestampField='" + timestampField + '\'' + '}';
        }
    }

    public interface GraphElementTable extends Table {

        String getTypeName();

        GeaFlowGraph getGraph();
    }

    private static TableField findField(List<TableField> fields, String name) {
        for (TableField field : fields) {
            if (Objects.equals(field.getName(), name)) {
                return field;
            }
        }
        throw new IllegalArgumentException("Field name: '" + name + "' is not found");
    }

    @Override
    public String toString() {
        return "GeaFlowGraph{" + "name='" + name + '\'' + ", vertexTables=" + vertexTables
            + ", edgeTables=" + edgeTables + ", config=" + config + ", ifNotExists=" + ifNotExists
            + '}';
    }
}
