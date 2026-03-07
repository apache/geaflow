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

package org.apache.geaflow.dsl.common.function;

import java.util.Objects;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;

/**
 * Utility class providing static methods for ISO-GQL source/destination predicates.
 *
 * <p>Implements ISO-GQL Section 19.10: &lt;source/destination predicate&gt;
 *
 * <p>These static methods are called via reflection by the corresponding runtime
 * Expression classes (IsSourceOfExpression, IsDestinationOfExpression) for better
 * distributed execution safety.
 *
 * <p>ISO-GQL General Rules:
 * <ul>
 *   <li>If node or edge is null, result is Unknown (null)</li>
 *   <li>If edge is undirected, result is False</li>
 *   <li>If node matches edge endpoint (source/destination), result is True</li>
 *   <li>Otherwise, result is False</li>
 * </ul>
 */
public class SourceDestinationFunctions {

    /**
     * Implements IS_SOURCE_OF predicate.
     *
     * @param nodeValue vertex/node object to check
     * @param edgeValue edge object to check
     * @return Boolean: true if node is source of edge, false if not, null if either is null
     */
    public static Boolean isSourceOf(Object nodeValue, Object edgeValue) {
        // ISO-GQL Rule 1: If node or edge is null, result is Unknown (null)
        if (nodeValue == null || edgeValue == null) {
            return null;  // Three-valued logic: Unknown
        }

        // Validate types
        if (!(nodeValue instanceof RowVertex)) {
            throw new IllegalArgumentException(
                "First operand of IS_SOURCE_OF must be a vertex/node, got: "
                + nodeValue.getClass().getName());
        }
        if (!(edgeValue instanceof RowEdge)) {
            throw new IllegalArgumentException(
                "Second operand of IS_SOURCE_OF must be an edge, got: "
                + edgeValue.getClass().getName());
        }

        RowVertex node = (RowVertex) nodeValue;
        RowEdge edge = (RowEdge) edgeValue;

        // ISO-GQL Rule 2: If edge is undirected, result is False
        // Note: In GeaFlow, BOTH direction means undirected
        if (edge.getDirect() == org.apache.geaflow.model.graph.edge.EdgeDirection.BOTH) {
            return false;
        }

        // ISO-GQL Rule 3: Check if node is source of edge
        // Compare node ID with edge source ID
        Object nodeId = node.getId();
        Object edgeSrcId = edge.getSrcId();

        return Objects.equals(nodeId, edgeSrcId);
    }

    /**
     * Implements IS_NOT_SOURCE_OF predicate.
     *
     * @param nodeValue vertex/node object to check
     * @param edgeValue edge object to check
     * @return Boolean: true if node is NOT source of edge, false if it is, null if either is null
     */
    public static Boolean isNotSourceOf(Object nodeValue, Object edgeValue) {
        Boolean result = isSourceOf(nodeValue, edgeValue);
        // Three-valued logic: NOT Unknown = Unknown (null remains null)
        return result == null ? null : !result;
    }

    /**
     * Implements IS_DESTINATION_OF predicate.
     *
     * @param nodeValue vertex/node object to check
     * @param edgeValue edge object to check
     * @return Boolean: true if node is destination of edge, false if not, null if either is null
     */
    public static Boolean isDestinationOf(Object nodeValue, Object edgeValue) {
        // ISO-GQL Rule 1: If node or edge is null, result is Unknown (null)
        if (nodeValue == null || edgeValue == null) {
            return null;  // Three-valued logic: Unknown
        }

        // Validate types
        if (!(nodeValue instanceof RowVertex)) {
            throw new IllegalArgumentException(
                "First operand of IS_DESTINATION_OF must be a vertex/node, got: "
                + nodeValue.getClass().getName());
        }
        if (!(edgeValue instanceof RowEdge)) {
            throw new IllegalArgumentException(
                "Second operand of IS_DESTINATION_OF must be an edge, got: "
                + edgeValue.getClass().getName());
        }

        RowVertex node = (RowVertex) nodeValue;
        RowEdge edge = (RowEdge) edgeValue;

        // ISO-GQL Rule 2: If edge is undirected, result is False
        // Note: In GeaFlow, BOTH direction means undirected
        if (edge.getDirect() == org.apache.geaflow.model.graph.edge.EdgeDirection.BOTH) {
            return false;
        }

        // ISO-GQL Rule 3: Check if node is destination of edge
        // Compare node ID with edge target ID
        Object nodeId = node.getId();
        Object edgeTargetId = edge.getTargetId();

        return Objects.equals(nodeId, edgeTargetId);
    }

    /**
     * Implements IS_NOT_DESTINATION_OF predicate.
     *
     * @param nodeValue vertex/node object to check
     * @param edgeValue edge object to check
     * @return Boolean: true if node is NOT destination of edge, false if it is, null if either is null
     */
    public static Boolean isNotDestinationOf(Object nodeValue, Object edgeValue) {
        Boolean result = isDestinationOf(nodeValue, edgeValue);
        // Three-valued logic: NOT Unknown = Unknown (null remains null)
        return result == null ? null : !result;
    }
}
