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

package org.apache.geaflow.dsl.connector.api.serde;

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.dsl.connector.api.serde.impl.JsonDeserializer;
import org.apache.geaflow.dsl.connector.api.serde.impl.RowTableDeserializer;
import org.apache.geaflow.dsl.connector.api.serde.impl.TextDeserializer;
import org.apache.geaflow.dsl.connector.api.util.ConnectorConstants;

public class DeserializerFactory {

    public static <IN> TableDeserializer<IN> loadDeserializer(Configuration conf) {
        String connectorFormat = conf.getString(ConnectorConfigKeys.GEAFLOW_DSL_CONNECTOR_FORMAT,
            (String) ConnectorConfigKeys.GEAFLOW_DSL_CONNECTOR_FORMAT.getDefaultValue());
        if (connectorFormat.equals(ConnectorConstants.CONNECTOR_FORMAT_JSON)) {
            return (TableDeserializer<IN>) new JsonDeserializer();
        } else {
            return (TableDeserializer<IN>) new TextDeserializer();
        }
    }

    public static <IN> TableDeserializer<IN> loadRowTableDeserializer() {
        return (TableDeserializer<IN>) new RowTableDeserializer();
    }

    public static <IN> TableDeserializer<IN> loadTextDeserializer() {
        return (TableDeserializer<IN>) new TextDeserializer();
    }

}
