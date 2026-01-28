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

package org.apache.geaflow.ai.common.util;

import com.google.gson.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.ai.common.config.Constants;
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.graph.io.*;

public class SeDeUtil {

    private static final Gson GSON = new Gson();

    public static String serializeGraphSchema(GraphSchema schema) {
        return GSON.toJson(schema);
    }

    public static GraphSchema deserializeGraphSchema(String json) {
        return GSON.fromJson(json, GraphSchema.class);
    }

    public static Schema deserializeEntitySchema(String json) {
        boolean isVertex = new JsonParser().parse(json).getAsJsonObject().has("idField");
        if (isVertex) {
            return GSON.fromJson(json, VertexSchema.class);
        } else {
            return GSON.fromJson(json, EdgeSchema.class);
        }
    }

    public static String serializeEntitySchema(Schema schema) {
        if (schema instanceof VertexSchema) {
            return GSON.toJson((VertexSchema) schema);
        } else {
            return GSON.toJson((EdgeSchema) schema);
        }
    }

    public static List<GraphEntity> deserializeEntities(String json) {
        JsonArray jsonArray;
        List<GraphEntity> entities = new ArrayList<>();
        try {
            jsonArray = new JsonParser().parse(json).getAsJsonArray();
        } catch (Throwable e) {
            JsonObject jsonObject = new JsonParser().parse(json).getAsJsonObject();
            if (jsonObject.has(Constants.PREFIX_ID)) {
                entities.add(new GraphVertex(GSON.fromJson(json, Vertex.class)));
            } else {
                entities.add(new GraphEdge(GSON.fromJson(json, Edge.class)));
            }
            return entities;
        }
        for (JsonElement element : jsonArray) {
            JsonObject jsonObject = element.getAsJsonObject();
            if (jsonObject.has(Constants.PREFIX_ID)) {
                entities.add(new GraphVertex(GSON.fromJson(json, Vertex.class)));
            } else {
                entities.add(new GraphEdge(GSON.fromJson(json, Edge.class)));
            }
        }
        return entities;
    }

}
