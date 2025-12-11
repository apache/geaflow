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

package org.apache.geaflow.ai;

import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.graph.io.*;
import org.apache.geaflow.ai.verbalization.PromptFormatter;

import java.util.stream.Collectors;

public class LdbcPromptFormatter implements PromptFormatter {

    public Vertex vertexMapper(Vertex v) {
        String idPrefix = v.getLabel();
        return new Vertex(v.getLabel(), idPrefix + v.getId(), v.getValues());
    }

    public Edge edgeMapper(Edge e) {
        String srcPrefix = "";
        String dstPrefix = "";
        switch (e.getLabel()) {
            case "Forum_hasMember_Person":
            case "Forum_hasModerator_Person":
                srcPrefix = "Forum";
                dstPrefix = "Person";
                break;
            case "Person_knows_Person":
                srcPrefix = "Person";
                dstPrefix = "Person";
                break;
            case "Comment_replyOf_Post":
                srcPrefix = "Comment";
                dstPrefix = "Post";
                break;
            case "Person_isLocatedIn_City":
                srcPrefix = "Person";
                dstPrefix = "Place";
                break;
            case "Comment_hasTag_Tag":
                srcPrefix = "Comment";
                dstPrefix = "Tag";
                break;
            case "Person_workAt_Company":
                srcPrefix = "Person";
                dstPrefix = "Company";
                break;
            case "Comment_replyOf_Comment":
                srcPrefix = "Comment";
                dstPrefix = "Comment";
                break;
            case "Organisation_isLocatedIn_Place":
                srcPrefix = "Organisation";
                dstPrefix = "Place";
                break;
            case "Comment_isLocatedIn_Country":
                srcPrefix = "Comment";
                dstPrefix = "Country";
                break;
            case "Person_hasInterest_Tag":
                srcPrefix = "Person";
                dstPrefix = "Tag";
                break;
            case "Forum_hasTag_Tag":
                srcPrefix = "Forum";
                dstPrefix = "Tag";
                break;
            case "Person_studyAt_University":
                srcPrefix = "Person";
                dstPrefix = "Organisation";
                break;
            case "Comment_hasCreator_Person":
                srcPrefix = "Comment";
                dstPrefix = "Person";
                break;
            case "TagClass_isSubclassOf_TagClass":
                srcPrefix = "TagClass";
                dstPrefix = "TagClass";
                break;
            case "Post_isLocatedIn_Country":
                srcPrefix = "Post";
                dstPrefix = "Country";
                break;
            case "Post_hasCreator_Person":
                srcPrefix = "Post";
                dstPrefix = "Person";
                break;
            case "Post_hasTag_Tag":
                srcPrefix = "Post";
                dstPrefix = "Tag";
                break;
            case "Person_likes_Post":
                srcPrefix = "Person";
                dstPrefix = "Post";
                break;
            case "Person_likes_Comment":
                srcPrefix = "Person";
                dstPrefix = "Comment";
                break;
            case "Forum_containerOf_Post":
                srcPrefix = "Forum";
                dstPrefix = "Post";
                break;
            case "Tag_hasType_TagClass":
                srcPrefix = "Tag";
                dstPrefix = "TagClass";
                break;
            case "Place_isPartOf_Place":
                srcPrefix = "Place";
                dstPrefix = "Place";
                break;
            default:
                srcPrefix = "";
                dstPrefix = "";
                break;
        }
        return new Edge(e.getLabel(), srcPrefix + e.getSrcId(),
                dstPrefix + e.getDstId(), e.getValues());
    }

    @Override
    public String prompt(GraphSchema graphSchema) {
        return "allVerticesType:[" + graphSchema.getVertexSchemaList()
                .stream().map(VertexSchema::getLabel).collect(Collectors.joining(",")) + "]\n"
                + "allRelations:[" + graphSchema.getEdgeSchemaList()
                .stream().map(EdgeSchema::getLabel).collect(Collectors.joining(",")) + "]\n";
    }

    @Override
    public String prompt(GraphVertex entity) {
        if (entity == null) {
            return "Empty Vertex.";
        } else if (entity.getVertex() == null) {
            return "The Vertex.";
        }
        Vertex obj;
        Edge edge;
        switch (entity.getLabel()) {
            case "Person":
                obj = ((GraphVertex) entity).getVertex();
                return String.format("A Person, %s, register at %s, id is %s, name is %s %s, birthday is %s, " +
                                "ip is %s, use browser is %s, use language are %s, email address is %s",
                        obj.getValues().get(4), obj.getValues().get(0), obj.getId(),
                        obj.getValues().get(2), obj.getValues().get(3), obj.getValues().get(5),
                        obj.getValues().get(6), obj.getValues().get(7), obj.getValues().get(8), obj.getValues().get(9));
            case "Forum":
                obj = ((GraphVertex) entity).getVertex();
                return String.format("A Forum, name is %s, id is %s, created at %s",
                        obj.getValues().get(2), obj.getId(), obj.getValues().get(0));
            case "TagClass":
                obj = ((GraphVertex) entity).getVertex();
                return String.format("A TagClass, name is %s, id is %s, url %s",
                        obj.getValues().get(1), obj.getId(), obj.getValues().get(2));
            case "Tag":
                obj = ((GraphVertex) entity).getVertex();
                return String.format("A tag, name is %s, id is %s, url %s",
                        obj.getValues().get(1), obj.getId(), obj.getValues().get(2));
            case "Comment":
                obj = ((GraphVertex) entity).getVertex();
                return String.format("A comment, id is %s, created at %s, user ip is %s, " +
                                "user use browser is %s, content is %s",
                        obj.getId(), obj.getValues().get(0), obj.getValues().get(2), obj.getValues().get(3), obj.getValues().get(4));
            case "Post":
                obj = ((GraphVertex) entity).getVertex();
                return String.format("A post, id is %s, created at %s, user ip is %s, " +
                                "user use browser is %s, use language is %s, content is %s",
                        obj.getId(), obj.getValues().get(0), obj.getValues().get(3), obj.getValues().get(4), obj.getValues().get(5), obj.getValues().get(6));
            case "Organisation":
                obj = ((GraphVertex) entity).getVertex();
                return String.format("An organisation of %s, name is %s, id is %s, url %s",
                        obj.getValues().get(1), obj.getValues().get(2), obj.getId(), obj.getValues().get(3));
            case "Place":
                obj = ((GraphVertex) entity).getVertex();
                return String.format("A place of %s, name is %s, id is %s, url %s",
                        obj.getValues().get(3), obj.getValues().get(1), obj.getId(), obj.getValues().get(2));
            default:
                return entity.toString();
        }
    }

    @Override
    public String prompt(GraphEdge entity, GraphVertex start, GraphVertex end) {
        if (entity == null) {
            return "Empty Edge.";
        } else if (entity.getEdge() == null) {
            return "The Edge.";
        }
        StringBuilder builder = new StringBuilder();
        builder.append(promptEdge(entity));
        builder.append("\n  startVertex:");
        builder.append(prompt(start));
        builder.append("\n  endVertex:");
        builder.append(prompt(end));
        return builder.toString();
    }

    public String promptEdge(GraphEdge entity) {
        if (entity == null || entity.getEdge() == null) {
            return "Empty Edge.";
        }
        Edge edge;
        switch (entity.getLabel()) {
            case "Forum_hasMember_Person":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Forum_hasMember_Person, Forum id %s has member id %s, register at %s",
                        edge.getSrcId(), edge.getDstId(), edge.getValues().get(0));
            case "Person_workAt_Company":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Person_workAt_Company, Person id %s work at company id %s, start from %s year",
                        edge.getSrcId(), edge.getDstId(), edge.getValues().get(3));
            case "Organisation_isLocatedIn_Place":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Organisation_isLocatedIn_Place, Organisation id %s is located in place id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Person_hasInterest_Tag":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Person_hasInterest_Tag, Person id %s interest at tag id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Forum_hasTag_Tag":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Forum_hasTag_Tag, Forum id %s has tag id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Forum_hasModerator_Person":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Forum_hasModerator_Person, Forum id %s has moderator id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Forum_containerOf_Post":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Forum_containerOf_Post, Forum id %s contain of the post id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Tag_hasType_TagClass":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Tag_hasType_TagClass, Tag id %s has type of tag class id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Place_isPartOf_Place":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Place_isPartOf_Place, The place id %s is part of place id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Person_isLocatedIn_City":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Person_isLocatedIn_City, The person id %s is located in city id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Person_knows_Person":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Person_knows_Person, The person id %s knows person id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Person_studyAt_University":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Person_studyAt_University, The person id %s study at university id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Comment_hasCreator_Person":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Comment_hasCreator_Person, The comment id %s has creator person id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Post_hasCreator_Person":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Post_hasCreator_Person, The post id %s has creator person id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Person_likes_Post":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Person_likes_Post, The person id %s likes person id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Person_likes_Comment":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Person_likes_Comment, The person id %s likes comment id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Comment_replyOf_Post":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Comment_replyOf_Post, The comment id %s reply of the post id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Post_isLocatedIn_Country":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Post_isLocatedIn_Country, The post id %s is located in country id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Post_hasTag_Tag":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Post_hasTag_Tag, The post id %s has tag id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Comment_hasTag_Tag":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Comment_hasTag_Tag, The comment id %s has tag id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Comment_isLocatedIn_Country":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Comment_isLocatedIn_Country, The comment id %s is located in country id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Comment_replyOf_Comment":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type Comment_replyOf_Comment, The comment id %s is reply of comment id %s",
                        edge.getSrcId(), edge.getDstId());
            case "TagClass_isSubclassOf_TagClass":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("One edge of Type TagClass_isSubclassOf_TagClass, The tag class id %s is subclass of the tag class id %s",
                        edge.getSrcId(), edge.getDstId());
            default:
                return entity.toString();
        }
    }
}
