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

package org.apache.geaflow.dsl.common.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.data.impl.VertexEdgeFactory;
import org.apache.geaflow.dsl.common.types.ArrayType;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.VertexType;
import org.apache.geaflow.model.graph.IGraphElementWithTimeField;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.testng.Assert;

public class TestSchemaUtil {

    public static IType<?>[] getIdTypes() {
        return new IType[]{Types.INTEGER, Types.LONG, Types.DOUBLE, Types.BINARY_STRING,
            Types.SHORT};
    }

    public static VertexType getVertexType(IType<?> idType) {
        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("id", idType, false));
        fields.add(new TableField("label", Types.BINARY_STRING, false));
        fields.addAll(getRowTypeFields());
        return new VertexType(fields);
    }

    public static EdgeType getEdgeType(IType<?> idType, boolean hasTimestamp) {
        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("srcId", idType, false));
        fields.add(new TableField("dstId", idType, false));
        fields.add(new TableField("label", Types.BINARY_STRING, false));
        if (hasTimestamp) {
            fields.add(new TableField("ts", Types.LONG, false));
        }
        fields.addAll(getRowTypeFields());
        return new EdgeType(fields, hasTimestamp);
    }

    public static StructType getRowType() {
        return new StructType(getRowTypeFields());
    }

    private static List<TableField> getRowTypeFields() {
        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("v0", Types.INTEGER, false));
        fields.add(new TableField("v1", Types.INTEGER, false));
        fields.add(new TableField("v2", Types.LONG, false));
        fields.add(new TableField("v3", Types.LONG, false));
        fields.add(new TableField("v4", Types.SHORT, false));
        fields.add(new TableField("v5", Types.SHORT, false));
        fields.add(new TableField("v6", Types.DOUBLE, false));
        fields.add(new TableField("v7", Types.DOUBLE, false));
        fields.add(new TableField("v8", Types.BINARY_STRING, false));
        fields.add(new TableField("v9", Types.BINARY_STRING, false));
        fields.add(new TableField("v10", new ArrayType(Types.INTEGER), false));
        fields.add(new TableField("v11", new ArrayType(Types.BINARY_STRING), false));
        fields.add(new TableField("v12", new ArrayType(Types.BINARY_STRING), false));
        return fields;
    }

    public static RowVertex getVertex(IType<?> idType) {
        return getVertex(idType, getRow());
    }

    public static RowVertex getVertex(IType<?> idType, ObjectRow row) {
        RowVertex vertex = VertexEdgeFactory.createVertex(getVertexType(idType));
        vertex.setId(getSrcId(idType));
        vertex.setBinaryLabel(BinaryString.fromString("vertexLabel"));
        vertex.setValue(row);
        return vertex;
    }

    public static RowEdge getEdge(IType<?> idType, boolean hasTimestamp) {
        return getEdge(idType, getRow(), hasTimestamp);
    }

    public static RowEdge getEdge(IType<?> idType, ObjectRow row, boolean hasTimestamp) {
        RowEdge edge = VertexEdgeFactory.createEdge(getEdgeType(idType, hasTimestamp));
        edge.setSrcId(getSrcId(idType));
        edge.setTargetId(getTargetId(idType));
        edge.setBinaryLabel(BinaryString.fromString("edgeLabel"));
        if (hasTimestamp) {
            ((IGraphElementWithTimeField) edge).setTime(284L);
        }
        edge.setValue(row);
        return edge.withDirection(EdgeDirection.IN);
    }

    public static Object getSrcId(IType<?> type) {
        String idTypeName = type.getName().toUpperCase(Locale.ROOT);
        switch (idTypeName) {
            case Types.TYPE_NAME_INTEGER:
                return 1;
            case Types.TYPE_NAME_LONG:
                return 1L;
            case Types.TYPE_NAME_DOUBLE:
                return 1.0d;
            case Types.TYPE_NAME_BINARY_STRING:
                return BinaryString.fromString("1");
            default:
                return (short) 1;
        }
    }

    public static Object getTargetId(IType<?> type) {
        String idTypeName = type.getName().toUpperCase(Locale.ROOT);
        switch (idTypeName) {
            case Types.TYPE_NAME_INTEGER:
                return 2;
            case Types.TYPE_NAME_LONG:
                return 2L;
            case Types.TYPE_NAME_DOUBLE:
                return 2.0d;
            case Types.TYPE_NAME_BINARY_STRING:
                return BinaryString.fromString("2");
            default:
                return (short) 2;
        }
    }

    public static ObjectRow getRow() {
        Object[] fields = new Object[]{111, null, 1234L, null, (short) 256, null, 1.0d, null,
            "testValue__#123", null, new int[]{1, 2, 5}, new String[]{"23", "wyuety12", null, "w237", null}, null};
        return ObjectRow.create(fields);
    }

    public static void checkResult(Object actual, Object expect, IType<?> type) {
        if (type instanceof VertexType) {
            checkVertex((RowVertex) actual, (RowVertex) expect, (VertexType) type);
        } else if (type instanceof EdgeType) {
            checkEdge((RowEdge) actual, (RowEdge) expect, (EdgeType) type);
        } else if (type instanceof StructType) {
            checkRow((Row) actual, (Row) expect, (StructType) type);
        } else if (type instanceof ArrayType) {
            Object[] actualArray = (Object[]) actual;
            Object[] expectArray = (Object[]) expect;
            Assert.assertEquals(actualArray.length, expectArray.length);

            IType<?> componentType = ((ArrayType) type).getComponentType();
            for (int i = 0; i < actualArray.length; i++) {
                checkResult(actualArray[i], expectArray[i], componentType);
            }
        } else {
            Assert.assertEquals(actual, expect);
        }
    }

    private static void checkVertex(RowVertex actual, RowVertex expect, VertexType vertexType) {
        Assert.assertEquals(actual.getId(), expect.getId());
        Assert.assertEquals(actual.getBinaryLabel(), expect.getBinaryLabel());

        List<TableField> valueFields = vertexType.getValueFields();
        checkResult(actual.getValue(), expect.getValue(), new StructType(valueFields));
    }

    private static void checkEdge(RowEdge actual, RowEdge expect, EdgeType edgeType) {
        Assert.assertEquals(actual.getSrcId(), expect.getSrcId());
        Assert.assertEquals(actual.getTargetId(), expect.getTargetId());
        Assert.assertEquals(actual.getBinaryLabel(), expect.getBinaryLabel());
        if (edgeType.getTimestamp().isPresent()) {
            Assert.assertEquals(((IGraphElementWithTimeField) actual).getTime(),
                ((IGraphElementWithTimeField) expect).getTime());
        }
        Assert.assertEquals(actual.getDirect(), expect.getDirect());
        checkResult(actual.getValue(), expect.getValue(), new StructType(edgeType.getValueFields()));
    }

    private static void checkRow(Row actual, Row expect, StructType rowType) {
        IType<?>[] types = rowType.getTypes();
        for (int i = 0; i < types.length; i++) {
            Assert.assertEquals(actual.getField(i, types[i]), expect.getField(i, types[i]));
        }
    }
}
