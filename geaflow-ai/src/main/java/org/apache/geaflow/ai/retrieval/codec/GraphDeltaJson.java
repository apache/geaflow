/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 */
package org.apache.geaflow.ai.retrieval.codec;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphDelta;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;

/** Deterministic JSONL boundary for graph delta artifacts. */
public final class GraphDeltaJson {

    private GraphDeltaJson() {
    }

    public static GraphDelta read(BufferedReader reader, GraphVersion version,
                                  GraphVersion parent, String expectedDigest) throws IOException {
        GraphDelta.Builder builder = GraphDelta.builder(version, parent);
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) {
                continue;
            }
            JsonObject operation = new JsonParser().parse(line).getAsJsonObject();
            String op = required(operation, "op");
            String type = required(operation, "type");
            if ("upsert".equals(op)) {
                JsonObject value = operation.getAsJsonObject("value");
                switch (type) {
                    case "chunk":
                        builder.upsertChunk(RetrievalModelJson.fromJson(value.toString(), TextChunk.class));
                        break;
                    case "entity":
                        builder.upsertEntity(RetrievalModelJson.fromJson(value.toString(), EntityRef.class));
                        break;
                    case "vertex":
                        builder.upsertVertex(RetrievalModelJson.fromJson(value.toString(),
                            GraphVertexRef.class));
                        break;
                    case "edge":
                        builder.upsertEdge(RetrievalModelJson.fromJson(value.toString(), GraphEdgeRef.class));
                        break;
                    case "alias":
                        builder.upsertAlias(required(value, "entityId"), required(value, "alias"));
                        break;
                    default:
                        throw new IOException("unknown graph delta type: " + type);
                }
            } else if ("delete".equals(op)) {
                String id = required(operation, "id");
                switch (type) {
                    case "chunk":
                        builder.deleteChunk(id);
                        break;
                    case "entity":
                        builder.deleteEntity(id);
                        break;
                    case "vertex":
                        builder.deleteVertex(id);
                        break;
                    case "edge":
                        builder.deleteEdge(id);
                        break;
                    case "alias":
                        int separator = id.indexOf('\u001f');
                        if (separator <= 0 || separator == id.length() - 1) {
                            throw new IOException("invalid alias tombstone");
                        }
                        builder.deleteAlias(id.substring(0, separator), id.substring(separator + 1));
                        break;
                    default:
                        throw new IOException("unknown graph delta type: " + type);
                }
            } else {
                throw new IOException("unknown graph delta operation: " + op);
            }
        }
        GraphDelta result = builder.build();
        if (expectedDigest != null && !expectedDigest.equals(result.getDigest())) {
            throw new IOException("graph delta digest mismatch");
        }
        return result;
    }

    public static void write(GraphDelta delta, Writer writer) throws IOException {
        for (TextChunk value : delta.getChunks()) {
            writeUpsert(writer, "chunk", value);
        }
        for (EntityRef value : delta.getEntities()) {
            writeUpsert(writer, "entity", value);
        }
        for (GraphVertexRef value : delta.getVertices()) {
            writeUpsert(writer, "vertex", value);
        }
        for (GraphEdgeRef value : delta.getEdges()) {
            writeUpsert(writer, "edge", value);
        }
        for (String value : delta.getTombstones().getChunks()) {
            writeDelete(writer, "chunk", value);
        }
        for (String value : delta.getTombstones().getEntities()) {
            writeDelete(writer, "entity", value);
        }
        for (String value : delta.getTombstones().getVertices()) {
            writeDelete(writer, "vertex", value);
        }
        for (String value : delta.getTombstones().getEdges()) {
            writeDelete(writer, "edge", value);
        }
        for (String value : delta.getTombstones().getAliases()) {
            writeDelete(writer, "alias", value);
        }
        for (java.util.Map.Entry<String, java.util.Set<String>> entry : delta.getAliases().entrySet()) {
            for (String alias : entry.getValue()) {
                JsonObject operation = new JsonObject();
                operation.addProperty("op", "upsert");
                operation.addProperty("type", "alias");
                JsonObject value = new JsonObject();
                value.addProperty("entityId", entry.getKey());
                value.addProperty("alias", alias);
                operation.add("value", value);
                writer.write(operation.toString());
                writer.write('\n');
            }
        }
    }

    private static void writeUpsert(Writer writer, String type, Object value) throws IOException {
        JsonObject operation = new JsonObject();
        operation.addProperty("op", "upsert");
        operation.addProperty("type", type);
        operation.add("value", new JsonParser().parse(RetrievalModelJson.toJson(value)));
        writer.write(operation.toString());
        writer.write('\n');
    }

    private static void writeDelete(Writer writer, String type, String id) throws IOException {
        JsonObject operation = new JsonObject();
        operation.addProperty("op", "delete");
        operation.addProperty("type", type);
        operation.addProperty("id", id);
        writer.write(operation.toString());
        writer.write('\n');
    }

    private static String required(JsonObject object, String key) throws IOException {
        if (object == null || !object.has(key) || object.get(key).isJsonNull()) {
            throw new IOException("graph delta field is missing: " + key);
        }
        return object.get(key).getAsString();
    }
}
