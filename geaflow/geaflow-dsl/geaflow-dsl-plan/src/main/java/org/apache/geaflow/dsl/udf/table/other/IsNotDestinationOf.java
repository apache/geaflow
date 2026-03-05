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

package org.apache.geaflow.dsl.udf.table.other;

import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.SourceDestinationFunctions;
import org.apache.geaflow.dsl.common.function.UDF;

/**
 * UDF implementation for ISO-GQL IS NOT DESTINATION OF predicate.
 *
 * <p>Implements ISO-GQL Section 19.10: &lt;source/destination predicate&gt;
 *
 * <p><b>Syntax:</b></p>
 * <pre>
 *   IS_NOT_DESTINATION_OF(node, edge)
 * </pre>
 *
 * <p><b>Semantics:</b></p>
 * Returns TRUE if the node is NOT the destination of the edge, FALSE if it is, or NULL if either operand is NULL.
 *
 * <p><b>ISO-GQL Rules:</b></p>
 * <ul>
 *   <li>If node or edge is null, result is Unknown (null)</li>
 *   <li>Otherwise, returns the negation of IS_DESTINATION_OF result</li>
 * </ul>
 *
 * <p><b>Example:</b></p>
 * <pre>
 * MATCH (a) -[e]-> (b)
 * WHERE IS_NOT_DESTINATION_OF(a, e)  -- Always TRUE for this pattern
 * RETURN a, e, b
 * </pre>
 */
@Description(
    name = "is_not_destination_of",
    description = "ISO-GQL Destination Predicate: Returns TRUE if node is NOT the destination of edge, "
        + "FALSE if it is, NULL if either operand is NULL. Follows ISO-GQL three-valued logic."
)
public class IsNotDestinationOf extends UDF {

    /**
     * Evaluates IS NOT DESTINATION OF predicate.
     *
     * @param nodeValue vertex/node to check (should be RowVertex)
     * @param edgeValue edge to check (should be RowEdge)
     * @return Boolean: true if node is NOT destination of edge, false if it is, null if either is null
     */
    public Boolean eval(Object nodeValue, Object edgeValue) {
        return SourceDestinationFunctions.isNotDestinationOf(nodeValue, edgeValue);
    }

    /**
     * Type-specific overload for better type checking.
     */
    public Boolean eval(RowVertex node, RowEdge edge) {
        return SourceDestinationFunctions.isNotDestinationOf(node, edge);
    }
}
