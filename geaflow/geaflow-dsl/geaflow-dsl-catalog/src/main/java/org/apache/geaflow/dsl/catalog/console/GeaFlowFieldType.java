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

package org.apache.geaflow.dsl.catalog.console;

import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;

/**
 * Basic data type supported on the console platform.
 */
public enum GeaFlowFieldType {
    /**
     * Boolean type.
     */
    BOOLEAN,
    /**
     * Int type.
     */
    INT,
    /**
     * Bigint type.
     */
    BIGINT,
    /**
     * Double type.
     */
    DOUBLE,
    /**
     * Varchar type.
     */
    VARCHAR,
    /**
     * Timestamp type.
     */
    TIMESTAMP;

    public static GeaFlowFieldType getFieldType(IType type) {
        switch (type.getName()) {
            case Types.TYPE_NAME_STRING:
            case Types.TYPE_NAME_BINARY_STRING:
                return VARCHAR;
            case Types.TYPE_NAME_LONG:
                return BIGINT;
            case Types.TYPE_NAME_INTEGER:
                return INT;
            default:
                for (GeaFlowFieldType t : values()) {
                    if (t.name().equalsIgnoreCase(type.getName())) {
                        return t;
                    }
                }
                break;
        }
        throw new GeaflowRuntimeException("can not find relate field type: " + type.getName());
    }
}
