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

import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;

/**
 * Utility class providing static methods for the ISO-GQL labeled predicate.
 *
 * <p>Implements ISO-GQL Section 19.9: &lt;labeled predicate&gt;, which tests whether a
 * graph element (vertex or edge) has a given label.
 *
 * <p>ISO-GQL General Rules (three-valued logic):
 * <ul>
 *   <li>If the element or the label is null, the result is Unknown (null).</li>
 *   <li>If the element's label equals the given label, the result is True.</li>
 *   <li>Otherwise, the result is False.</li>
 * </ul>
 */
public class LabeledPredicateFunctions {

    /**
     * Implements the IS LABELED predicate.
     *
     * @param elementValue the vertex or edge to check
     * @param labelValue the label name to test for
     * @return Boolean: true if the element has the given label, false if not, null if either
     *         operand is null
     */
    public static Boolean isLabeled(Object elementValue, Object labelValue) {
        // ISO-GQL Rule: If element or label is null, result is Unknown (null).
        if (elementValue == null || labelValue == null) {
            return null;
        }

        String elementLabel = getLabel(elementValue);
        return elementLabel != null && elementLabel.equals(labelValue.toString());
    }

    /**
     * Implements the IS NOT LABELED predicate.
     *
     * @param elementValue the vertex or edge to check
     * @param labelValue the label name to test for
     * @return Boolean: true if the element does NOT have the given label, false if it does, null
     *         if either operand is null
     */
    public static Boolean isNotLabeled(Object elementValue, Object labelValue) {
        Boolean result = isLabeled(elementValue, labelValue);
        // Three-valued logic: NOT Unknown = Unknown (null remains null).
        return result == null ? null : !result;
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
