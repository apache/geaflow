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
 * UDF implementation for the ISO-GQL IS NOT LABELED predicate.
 *
 * <p>Implements ISO-GQL Section 19.9: &lt;labeled predicate&gt;. This is the negation of
 * {@link IsLabeled} and delegates to it so the label-extraction logic lives in one place.
 *
 * <p><b>Syntax:</b></p>
 * <pre>
 *   IS_NOT_LABELED(element, label)
 * </pre>
 *
 * <p><b>Semantics (ISO-GQL three-valued logic):</b></p>
 * Returns TRUE if the vertex or edge does NOT have the given label, FALSE if it does, or NULL
 * if either operand is NULL (NOT Unknown = Unknown).
 *
 * <p><b>Example:</b></p>
 * <pre>
 * MATCH (a) -[e]-> (b)
 * WHERE IS_NOT_LABELED(a, 'software')
 * RETURN a, e, b
 * </pre>
 */
@Description(
    name = "is_not_labeled",
    description = "ISO-GQL Labeled Predicate: Returns TRUE if the vertex or edge does NOT have "
        + "the given label, FALSE if it does, NULL if either operand is NULL. Follows ISO-GQL "
        + "three-valued logic."
)
public class IsNotLabeled extends UDF {

    private final IsLabeled isLabeled = new IsLabeled();

    /**
     * Evaluates the IS NOT LABELED predicate.
     *
     * @param elementValue vertex or edge to check
     * @param labelValue label name to test for
     * @return Boolean: true if the element does NOT have the given label, false if it does, null
     *         if either operand is null
     */
    public Boolean eval(Object elementValue, Object labelValue) {
        Boolean result = isLabeled.eval(elementValue, labelValue);
        // Three-valued logic: NOT Unknown = Unknown (null remains null).
        return result == null ? null : !result;
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
}
