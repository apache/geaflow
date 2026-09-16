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

package org.apache.geaflow.ai.common.model;

import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.ai.common.config.Constants;
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;

public class ModelUtils {

    public static List<String> splitLongText(int maxChunkSize, String... textList) {
        List<String> chunks = new ArrayList<>();
        for (String text : textList) {
            for (int i = 0; i < text.length(); i += maxChunkSize) {
                int end = Math.min(i + maxChunkSize, text.length());
                chunks.add(text.substring(i, end));
            }
        }
        return chunks;
    }

    /**
     * Separates the length of a key component from the component itself.
     */
    private static final char LENGTH_SEPARATOR = ':';

    /**
     * Stands in for a null component. Not a digit, so it cannot be confused with a length.
     */
    private static final String NULL_COMPONENT = "-:";

    /**
     * A key identifying an entity, unique across the graph.
     *
     * <p>Components are length prefixed rather than plainly concatenated, because the result is used
     * as a primary key: callers delete and replace documents by this exact value, so two different
     * entities mapping to the same key would make one silently destroy the other. Plain
     * concatenation is not injective, {@code (id="a", label="bc")} and {@code (id="ab", label="c")}
     * both yield {@code Vabc}. With lengths the encoding can be parsed back unambiguously, which is
     * what makes it collision free.
     */
    public static String getGraphEntityKey(GraphEntity entity) {
        if (entity instanceof GraphVertex) {
            return Constants.PREFIX_V
                    + encodeKeyComponent(((GraphVertex) entity).getVertex().getId())
                    + encodeKeyComponent(entity.getLabel());
        } else if (entity instanceof GraphEdge) {
            return Constants.PREFIX_E
                    + encodeKeyComponent(((GraphEdge) entity).getEdge().getSrcId())
                    + encodeKeyComponent(entity.getLabel())
                    + encodeKeyComponent(((GraphEdge) entity).getEdge().getDstId());
        }
        return "";
    }

    private static String encodeKeyComponent(String component) {
        if (component == null) {
            return NULL_COMPONENT;
        }
        return component.length() + String.valueOf(LENGTH_SEPARATOR) + component;
    }

    /**
     * The plainly concatenated key used before {@link #getGraphEntityKey} became collision free.
     *
     * <p>Only for reading persisted state written by an older version, such as an
     * {@code EmbeddingIndexStore} index file. It is not injective, so a caller must handle the case
     * where two entities produce the same value rather than trusting a lookup by it.
     */
    public static String getLegacyGraphEntityKey(GraphEntity entity) {
        if (entity instanceof GraphVertex) {
            return Constants.PREFIX_V + ((GraphVertex) entity).getVertex().getId() + entity.getLabel();
        } else if (entity instanceof GraphEdge) {
            return Constants.PREFIX_E + ((GraphEdge) entity).getEdge().getSrcId()
                    + entity.getLabel() + ((GraphEdge) entity).getEdge().getDstId();
        }
        return "";
    }
}
