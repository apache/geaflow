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

package org.apache.geaflow.analytics.service.client.jdbc.property;

import static java.lang.String.format;

import java.util.Optional;
import java.util.Properties;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;

/**
 * This class is an adaptation of Presto's com.facebook.presto.jdbc.ConnectionProperty.
 */
public interface ConnectProperty<T> {

    String getPropertyKey();

    Optional<String> getDefault();

    boolean enableRequired(Properties properties);

    boolean enableAllowed(Properties properties);

    Optional<T> getValue(Properties properties) throws GeaflowRuntimeException;

    void validate(Properties properties) throws GeaflowRuntimeException;

    default T getRequiredValue(Properties properties) throws GeaflowRuntimeException {
        return getValue(properties).orElseThrow(() ->
            new GeaflowRuntimeException(format("Connect property '%s' is required", getPropertyKey())));
    }
}
