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
 * UDF implementation for ISO-GQL IS SOURCE OF predicate.
 *
 * <p>Implements ISO-GQL Section 19.10: &lt;source/destination predicate&gt;
 *
 * <p><b>Syntax:</b></p>
 * <pre>
 *   IS_SOURCE_OF(node, edge)
 * </pre>
 *
 * <p><b>Semantics:</b></p>
 * Returns TRUE if the node is the source of the edge, FALSE otherwise, or NULL if either operand is NULL.
 *
 * <p><b>ISO-GQL Rules:</b></p>
 * <ul>
 *   <li>If node or edge is null, result is Unknown (null)</li>
 *   <li>If edge is undirected, result is False</li>
 *   <li>If node.id equals edge.srcId, result is True</li>
 *   <li>Otherwise, result is False</li>
 * </ul>
 *
 * <p><b>Example:</b></p>
 * <pre>
 * MATCH (a) -[e]-> (b)
 * WHERE IS_SOURCE_OF(a, e)
 * RETURN a, e, b
 * </pre>
 */
@Description(
    name = "is_source_of",
    description = "ISO-GQL Source Predicate: Returns TRUE if node is the source of edge, "
        + "FALSE if not, NULL if either operand is NULL. Follows ISO-GQL three-valued logic."
)
public class IsSourceOf extends UDF {

    /**
     * Evaluates IS SOURCE OF predicate.
     *
     * @param nodeValue vertex/node to check (should be RowVertex)
     * @param edgeValue edge to check (should be RowEdge)
     * @return Boolean: true if node is source of edge, false if not, null if either is null
     */
    public Boolean eval(Object nodeValue, Object edgeValue) {
        return SourceDestinationFunctions.isSourceOf(nodeValue, edgeValue);
    }

    /**
     * Type-specific overload for better type checking.
     */
    public Boolean eval(RowVertex node, RowEdge edge) {
        return SourceDestinationFunctions.isSourceOf(node, edge);
    }
}
