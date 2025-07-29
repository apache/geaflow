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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.EdgeTable;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.VertexTable;
import org.apache.geaflow.view.IViewDesc.BackendType;
import org.testng.annotations.Test;

public class GeaFlowGraphTest {

    @Test
    public void testGeaFlowGraph() {
        GQLJavaTypeFactory typeFactory = GQLJavaTypeFactory.create();
        TableField field1 = new TableField("name", Types.STRING, true);
        TableField field2 = new TableField("id", Types.LONG, false);
        TableField field3 = new TableField("age", Types.DOUBLE, true);
        VertexTable vertexTable = new VertexTable(
            "default",
            "person",
            Lists.newArrayList(field1, field2, field3),
            "id"
        );
        assertEquals(vertexTable.getTypeName(), "person");
        assertEquals(vertexTable.getFields().size(), 3);
        assertNotNull(vertexTable.getIdField());

        TableField field4 = new TableField("src", Types.LONG, false);
        TableField field5 = new TableField("dst", Types.LONG, false);
        TableField field6 = new TableField("weight", Types.DOUBLE, true);
        EdgeTable edgeTable = new EdgeTable(
            "default",
            "follow",
            Lists.newArrayList(field4, field5, field6),
            "src", "dst", null
        );
        assertEquals(edgeTable.getTypeName(), "follow");
        assertEquals(edgeTable.getFields().size(), 3);
        assertNotNull(edgeTable.getSrcIdField());
        assertNotNull(edgeTable.getTargetIdField());
        assertNull(edgeTable.getTimestampField());

        Map<String, String> config = new HashMap<>();
        config.put(DSLConfigKeys.GEAFLOW_DSL_STORE_TYPE.getKey(), "MEMORY");
        GeaFlowGraph graph = new GeaFlowGraph(
            "default",
            "g0",
            Lists.newArrayList(vertexTable),
            Lists.newArrayList(edgeTable),
            config, new HashMap<>(),
            false, false);

        RelDataType relDataType = graph.getRowType(typeFactory);
        assertEquals(relDataType.toString(), "Graph:RecordType:peek("
            + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, DOUBLE age) person, "
            + "Edge: RecordType:peek(BIGINT src, BIGINT dst, VARCHAR ~label, DOUBLE weight) follow)"
        );
        assertEquals(graph.getName(), "g0");
        assertEquals(graph.getLabelType().getName(), "STRING");
        assertEquals(graph.getVertexTables().size(), 1);
        assertEquals(graph.getEdgeTables().size(), 1);
        assertEquals(graph.getConfig().getConfigMap().size(), 1);
        assertEquals(BackendType.of(graph.getStoreType()), BackendType.Memory);

        Map<String, String> globalConfMap = new HashMap<>();
        globalConfMap.put(DSLConfigKeys.GEAFLOW_DSL_STORE_TYPE.getKey(), "rocksdb");
        Configuration globalConf = new Configuration(globalConfMap);
        Configuration conf = graph.getConfigWithGlobal(globalConf);
        assertEquals(conf.getString(DSLConfigKeys.GEAFLOW_DSL_STORE_TYPE), "MEMORY");
    }
}
