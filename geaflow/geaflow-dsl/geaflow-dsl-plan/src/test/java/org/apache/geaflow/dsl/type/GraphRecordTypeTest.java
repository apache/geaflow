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

package org.apache.geaflow.dsl.type;

import static org.testng.Assert.assertEquals;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.geaflow.dsl.calcite.EdgeRecordType;
import org.apache.geaflow.dsl.calcite.GraphRecordType;
import org.apache.geaflow.dsl.calcite.VertexRecordType;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.planner.GQLRelDataTypeSystem;
import org.testng.annotations.Test;

public class GraphRecordTypeTest {

    @Test
    public void testGraphRecordType() {
        SqlTypeName stringType = SqlTypeName.VARCHAR;
        SqlTypeName longType = SqlTypeName.BIGINT;
        SqlTypeName doubleType = SqlTypeName.DOUBLE;
        RelDataTypeSystem typeSystem = new GQLRelDataTypeSystem();
        GQLJavaTypeFactory typeFactory = GQLJavaTypeFactory.create();

        RelDataType relStringType = new BasicSqlType(typeSystem, stringType);
        RelDataType relLongType = new BasicSqlType(typeSystem, longType);
        RelDataType relDoubleType = new BasicSqlType(typeSystem, doubleType);

        RelDataTypeField field1 = new RelDataTypeFieldImpl("name", 0, relStringType);
        RelDataTypeField field2 = new RelDataTypeFieldImpl("id", 1, relLongType);
        RelDataTypeField field3 = new RelDataTypeFieldImpl("age", 2, relDoubleType);
        RelDataType vertexType = VertexRecordType.createVertexType(
            Lists.newArrayList(field1, field2, field3),
            "id", typeFactory
        );
        RelDataTypeField vertexField = new RelDataTypeFieldImpl("user", 0, vertexType);

        RelDataTypeField field4 = new RelDataTypeFieldImpl("src", 0, relLongType);
        RelDataTypeField field5 = new RelDataTypeFieldImpl("dst", 1, relLongType);
        RelDataTypeField field6 = new RelDataTypeFieldImpl("weight", 2, relDoubleType);
        RelDataType edgeType = EdgeRecordType.createEdgeType(
            Lists.newArrayList(field4, field5, field6),
            "src", "dst", null, typeFactory
        );
        RelDataTypeField edgeField = new RelDataTypeFieldImpl("follow", 1, edgeType);

        GraphRecordType graphRecordType = new GraphRecordType("g0",
            Lists.newArrayList(vertexField, edgeField)
        );
        assertEquals(
            graphRecordType.getVertexType(Lists.newArrayList("user"), typeFactory)
                .toString(),
            "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, DOUBLE age)"
        );
        assertEquals(
            graphRecordType.getEdgeType(Lists.newArrayList("follow"), typeFactory)
                .toString(),
            "Edge: RecordType:peek(BIGINT src, BIGINT dst, VARCHAR ~label, DOUBLE weight)"
        );
    }
}
