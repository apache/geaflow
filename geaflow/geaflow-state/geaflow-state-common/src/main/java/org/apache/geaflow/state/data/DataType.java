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

package org.apache.geaflow.state.data;

public enum DataType {
    /**
     * VType.
     */
    V,
    /**
     * EType.
     */
    E,
    /**
     * VEType.
     */
    VE,
    /**
     * VID Type.
     */
    VID,
    /**
     * Vertex topology type.
     */
    V_TOPO,
    /**
     * Edge topology type.
     */
    E_TOPO,
    /**
     * Vertex and Edge topology type.
     */
    VE_TOPO,
    /**
     * Project field.
     */
    PROJECT_FIELD,
    /**
     * Unknown type.
     */
    OTHER;
}
