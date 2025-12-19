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

import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.PropertyExistsFunctions;
import org.apache.geaflow.dsl.common.function.UDF;

/**
 * UDF implementation for ISO-GQL PROPERTY_EXISTS predicate.
 *
 * <p>Implements ISO-GQL Section 19.13: &lt;property_exists predicate&gt;
 *
 * <p><b>Syntax:</b></p>
 * <pre>
 *   PROPERTY_EXISTS(element, property_name)
 * </pre>
 *
 * <p><b>Semantics:</b></p>
 * Returns TRUE if the graph element has the specified property, FALSE otherwise, or NULL if the element is NULL.
 *
 * <p><b>ISO-GQL Rules:</b></p>
 * <ul>
 *   <li>If element is null, result is Unknown (null)</li>
 *   <li>If element has a property with the specified name, result is True</li>
 *   <li>Otherwise, result is False</li>
 * </ul>
 *
 * <p><b>Example:</b></p>
 * <pre>
 * MATCH (p:Person)
 * WHERE PROPERTY_EXISTS(p, 'email')
 * RETURN p.name, p.email
 * </pre>
 */
@Description(
    name = "property_exists",
    description = "ISO-GQL Property Exists Predicate: Returns TRUE if the graph element has "
        + "the specified property, FALSE if not, NULL if element is NULL. "
        + "Follows ISO-GQL three-valued logic."
)
public class PropertyExists extends UDF {

    /**
     * Evaluates PROPERTY_EXISTS predicate for any graph element.
     *
     * <p>This implementation follows the established GeaFlow pattern for ISO-GQL predicates,
     * delegating to {@link PropertyExistsFunctions} utility class for consistent validation
     * and error handling across the framework.
     *
     * <p><b>Implementation Strategy:</b>
     * Property existence validation relies on compile-time checking through GeaFlow's SQL
     * optimizer and type system (StructType). At runtime, this function validates argument
     * types and provides meaningful error messages.
     *
     * <p>This approach is consistent with:
     * <ul>
     *   <li>Other ISO-GQL predicates (IS_SOURCE_OF, IS_DESTINATION_OF)</li>
     *   <li>GeaFlow's Row interface design (indexed access only)</li>
     *   <li>Three-layer architecture: UDF → Utility → Business Logic</li>
     * </ul>
     *
     * @param element graph element to check (Row, RowVertex, or RowEdge)
     * @param propertyName name of property to check
     * @return Boolean: null if element is null, true if property exists, false otherwise
     * @throws IllegalArgumentException if element is not a valid graph element type
     * @throws IllegalArgumentException if propertyName is null or empty
     */
    public Boolean eval(Object element, String propertyName) {
        return PropertyExistsFunctions.propertyExists(element, propertyName);
    }

    /**
     * Type-specific overload for RowVertex.
     *
     * <p>Provides better type inference and error messages when called with vertex elements.
     *
     * @param vertex vertex to check
     * @param propertyName name of property to check
     * @return Boolean: null if vertex is null, true if property exists, false otherwise
     */
    public Boolean eval(RowVertex vertex, String propertyName) {
        return PropertyExistsFunctions.propertyExists(vertex, propertyName);
    }

    /**
     * Type-specific overload for RowEdge.
     *
     * <p>Provides better type inference and error messages when called with edge elements.
     *
     * @param edge edge to check
     * @param propertyName name of property to check
     * @return Boolean: null if edge is null, true if property exists, false otherwise
     */
    public Boolean eval(RowEdge edge, String propertyName) {
        return PropertyExistsFunctions.propertyExists(edge, propertyName);
    }

    /**
     * Type-specific overload for Row.
     *
     * <p>Provides better type inference and error messages when called with row elements.
     *
     * @param row row to check
     * @param propertyName name of property to check
     * @return Boolean: null if row is null, true if property exists, false otherwise
     */
    public Boolean eval(Row row, String propertyName) {
        return PropertyExistsFunctions.propertyExists(row, propertyName);
    }
}
