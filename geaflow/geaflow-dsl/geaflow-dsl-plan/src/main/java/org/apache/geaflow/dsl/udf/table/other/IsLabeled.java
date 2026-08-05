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
import org.apache.geaflow.dsl.common.function.UDF;

/**
 * UDF implementation for the ISO-GQL IS LABELED predicate.
 *
 * <p>Implements ISO-GQL Section 19.9: &lt;labeled predicate&gt;, which tests whether a
 * graph element (vertex or edge) has a given label.
 *
 * <p><b>Syntax:</b></p>
 * <pre>
 *   IS_LABELED(element, label)
 * </pre>
 *
 * <p><b>Semantics (ISO-GQL three-valued logic):</b></p>
 * <ul>
 *   <li>If the element or the label is null, the result is Unknown (null).</li>
 *   <li>If the element's label equals the given label, the result is True.</li>
 *   <li>Otherwise, the result is False.</li>
 * </ul>
 *
 * <p><b>Example:</b></p>
 * <pre>
 * MATCH (a) -[e]-> (b)
 * WHERE IS_LABELED(a, 'person')
 * RETURN a, e, b
 * </pre>
 */
@Description(
    name = "is_labeled",
    description = "ISO-GQL Labeled Predicate: Returns TRUE if the vertex or edge has the given "
        + "label, FALSE if not, NULL if either operand is NULL. Follows ISO-GQL three-valued logic."
)
public class IsLabeled extends UDF {

    /**
     * Evaluates the IS LABELED predicate.
     *
     * @param elementValue vertex or edge to check
     * @param labelValue label name to test for
     * @return Boolean: true if the element has the given label, false if not, null if either
     *         operand is null
     */
    public Boolean eval(Object elementValue, Object labelValue) {
        // ISO-GQL Rule: If element or label is null, result is Unknown (null).
        if (elementValue == null || labelValue == null) {
            return null;
        }
        String elementLabel = getLabel(elementValue);
        return elementLabel != null && elementLabel.equals(labelValue.toString());
    }

    /**
     * Type-specific overload for vertices.
     */
    public Boolean eval(RowVertex vertex, String label) {
        return eval((Object) vertex, label);
    }

    /**
     * Type-specific overload for edges.
     */
    public Boolean eval(RowEdge edge, String label) {
        return eval((Object) edge, label);
    }

    private static String getLabel(Object elementValue) {
        if (elementValue instanceof RowVertex) {
            return ((RowVertex) elementValue).getLabel();
        }
        if (elementValue instanceof RowEdge) {
            return ((RowEdge) elementValue).getLabel();
        }
        throw new IllegalArgumentException(
            "First operand of labeled predicate must be a vertex or an edge, got: "
                + elementValue.getClass().getName());
    }
}
