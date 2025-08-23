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

package org.apache.geaflow.console.core.model.config;

import com.google.common.collect.Sets;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.console.common.util.exception.GeaflowException;

public enum GeaflowConfigType {

    BOOLEAN(Boolean.class),

    LONG(Long.class, Integer.class, Short.class),

    DOUBLE(BigDecimal.class, Double.class, Float.class),

    STRING(String.class, Enum.class),

    CONFIG(GeaflowConfigClass.class, Map.class);

    private final Set<Class<?>> javaClasses;

    GeaflowConfigType(Class<?>... javaClasses) {
        this.javaClasses = Sets.newHashSet(javaClasses);
    }

    public static GeaflowConfigType of(Class<?> clazz) {
        for (GeaflowConfigType value : values()) {
            for (Class<?> javaClass : value.javaClasses) {
                if (javaClass.isAssignableFrom(clazz)) {
                    return value;
                }
            }
        }
        throw new GeaflowException("Unsupported config type {}", clazz.getSimpleName());
    }
}
