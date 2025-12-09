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
import org.apache.geaflow.ai.graph.io.Edge;
import org.apache.geaflow.ai.graph.io.Vertex;
import org.apache.geaflow.ai.verbalization.PromptFormatter;

public class LdbcPromptFormatter implements PromptFormatter {

    @Override
    public String prompt(GraphVertex entity) {
        if (entity == null || entity.getVertex() == null) {
            return "Empty Vertex.";
        }
        Vertex obj;
        Edge edge;
        switch (entity.getLabel()) {
            case "Person":
                obj = ((GraphVertex) entity).getVertex();
                return String.format("A Person, %s, register at %s, id is %s, name is %s %s, birthday is %s, " +
                                "ip is %s, use browser is %s, use language are %s, email address is %s",
                        obj.getValues().get(4), obj.getValues().get(0), obj.getValues().get(1),
                        obj.getValues().get(2), obj.getValues().get(3), obj.getValues().get(5),
                        obj.getValues().get(6), obj.getValues().get(7), obj.getValues().get(8), obj.getValues().get(9));
            case "Forum":
                obj = ((GraphVertex) entity).getVertex();
                return String.format("A Forum, name is %s, id is %s, created at %s",
                        obj.getValues().get(2), obj.getId(), obj.getValues().get(0));
            case "TagClass":
                obj = ((GraphVertex) entity).getVertex();
                return String.format("A tag class, name is %s, id is %s, url %s",
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
                return String.format("A organisation of %s, name is %s, id is %s, url %s",
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
        if (entity == null || entity.getEdge() == null) {
            return "Empty Edge.";
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
                return String.format("Forum id %s has member id %s, register at %s",
                        edge.getSrcId(), edge.getDstId(), edge.getValues().get(0));
            case "Person_workAt_Company":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("Person id %s work at company id %s, start from %s year",
                        edge.getSrcId(), edge.getDstId(), edge.getValues().get(3));
            case "Organisation_isLocatedIn_Place":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("Organisation id %s is located in place id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Person_hasInterest_Tag":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("Person id %s interest at tag id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Forum_hasTag_Tag":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("Forum id %s has tag id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Forum_hasModerator_Person":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("Forum id %s has moderator id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Forum_containerOf_Post":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("Forum id %s contain of the post id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Tag_hasType_TagClass":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("Tag id %s has type of tag class id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Place_isPartOf_Place":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("The place id %s is part of place id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Person_isLocatedIn_City":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("The person id %s is located in city id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Person_knows_Person":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("The person id %s knows person id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Person_studyAt_University":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("The person id %s study at university id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Comment_hasCreator_Person":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("The comment id %s has creator person id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Post_hasCreator_Person":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("The post id %s has creator person id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Person_likes_Post":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("The person id %s likes person id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Person_likes_Comment":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("The person id %s likes comment id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Comment_replyOf_Post":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("The comment id %s reply of the post id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Post_isLocatedIn_Country":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("The post id %s is located in country id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Post_hasTag_Tag":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("The post id %s has tag id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Comment_hasTag_Tag":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("The comment id %s has tag id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Comment_isLocatedIn_Country":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("The comment id %s is located in country id %s",
                        edge.getSrcId(), edge.getDstId());
            case "Comment_replyOf_Comment":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("The comment id %s is reply of comment id %s",
                        edge.getSrcId(), edge.getDstId());
            case "TagClass_isSubclassOf_TagClass":
                edge = ((GraphEdge) entity).getEdge();
                return String.format("The tag class id %s is subclass of the tag class id %s",
                        edge.getSrcId(), edge.getDstId());
            default:
                return entity.toString();
        }
    }
}
