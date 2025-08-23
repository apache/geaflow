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

package org.apache.geaflow.console.test;

import org.apache.geaflow.console.common.util.type.GeaflowFieldCategory;
import org.apache.geaflow.console.common.util.type.GeaflowFieldType;
import org.apache.geaflow.console.core.model.data.GeaflowEdge;
import org.apache.geaflow.console.core.model.data.GeaflowEndpoint;
import org.apache.geaflow.console.core.model.data.GeaflowField;
import org.apache.geaflow.console.core.model.data.GeaflowGraph;
import org.apache.geaflow.console.core.model.data.GeaflowVertex;
import org.apache.geaflow.console.core.service.llm.GraphSchemaTranslator;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

public class GraphSchemaTranslateTest {

    @Test
    public void testGenerateCode() {

        GeaflowVertex person = new GeaflowVertex("person", "");
        person.setId("1");
        person.addFields(Lists.newArrayList(
            new GeaflowField("id", "", GeaflowFieldType.INT, GeaflowFieldCategory.VERTEX_ID),
            new GeaflowField("name", "", GeaflowFieldType.VARCHAR, GeaflowFieldCategory.PROPERTY),
            new GeaflowField("age", "", GeaflowFieldType.INT, GeaflowFieldCategory.PROPERTY)));

        GeaflowVertex software = new GeaflowVertex("software", "");
        software.setId("2");
        software.addFields(Lists.newArrayList(
            new GeaflowField("id", "", GeaflowFieldType.INT, GeaflowFieldCategory.VERTEX_ID),
            new GeaflowField("lang", "", GeaflowFieldType.VARCHAR, GeaflowFieldCategory.PROPERTY),
            new GeaflowField("price", "", GeaflowFieldType.INT, GeaflowFieldCategory.PROPERTY)));


        GeaflowEdge knows = new GeaflowEdge("knows", "");
        knows.setId("3");
        knows.addFields(Lists.newArrayList(
            new GeaflowField("srcId", "", GeaflowFieldType.INT, GeaflowFieldCategory.EDGE_SOURCE_ID),
            new GeaflowField("targetId", "", GeaflowFieldType.INT, GeaflowFieldCategory.EDGE_TARGET_ID),
            new GeaflowField("weight", "", GeaflowFieldType.DOUBLE, GeaflowFieldCategory.PROPERTY)));

        GeaflowEdge creates = new GeaflowEdge("creates", "");
        creates.setId("4");
        creates.addFields(Lists.newArrayList(
            new GeaflowField("srcId", "", GeaflowFieldType.INT, GeaflowFieldCategory.EDGE_SOURCE_ID),
            new GeaflowField("targetId", "", GeaflowFieldType.INT, GeaflowFieldCategory.EDGE_TARGET_ID),
            new GeaflowField("weight", "", GeaflowFieldType.DOUBLE, GeaflowFieldCategory.PROPERTY)));

        GeaflowEdge uses = new GeaflowEdge("uses", "");
        uses.setId("5");
        uses.addFields(Lists.newArrayList(
            new GeaflowField("srcId", "", GeaflowFieldType.INT, GeaflowFieldCategory.EDGE_SOURCE_ID),
            new GeaflowField("targetId", "", GeaflowFieldType.INT, GeaflowFieldCategory.EDGE_TARGET_ID),
            new GeaflowField("weight", "", GeaflowFieldType.DOUBLE, GeaflowFieldCategory.PROPERTY)));

        GeaflowGraph graph = new GeaflowGraph();
        graph.addVertex(person);
        graph.addVertex(software);
        graph.addEdge(knows);
        graph.addEdge(creates);
        graph.addEdge(uses);
        graph.setEndpoints(Lists.newArrayList(
            new GeaflowEndpoint("3", "1", "1"),
            new GeaflowEndpoint("4", "1", "2")));
        String result = GraphSchemaTranslator.translateGraphSchema(graph);
        Assert.assertEquals(result, "CREATE GRAPH g (\n"
            + "    Vertex person (\n"
            + "        id INT ID,\n"
            + "        name VARCHAR,\n"
            + "        age INT\n"
            + "    ),\n"
            + "    Vertex software (\n"
            + "        id INT ID,\n"
            + "        lang VARCHAR,\n"
            + "        price INT\n"
            + "    ),\n"
            + "    Edge knows (\n"
            + "        srcId INT FROM person SOURCE ID,\n"
            + "        targetId INT FROM person DESTINATION ID,\n"
            + "        weight DOUBLE\n"
            + "    ),\n"
            + "    Edge creates (\n"
            + "        srcId INT FROM person SOURCE ID,\n"
            + "        targetId INT FROM software DESTINATION ID,\n"
            + "        weight DOUBLE\n"
            + "    ),\n"
            + "    Edge uses (\n"
            + "        srcId INT,\n"
            + "        targetId INT,\n"
            + "        weight DOUBLE\n"
            + "    )\n"
            + ");");
    }
}
