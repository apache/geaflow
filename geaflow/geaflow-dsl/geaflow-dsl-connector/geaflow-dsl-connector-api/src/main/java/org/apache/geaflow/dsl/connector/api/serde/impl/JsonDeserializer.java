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

package org.apache.geaflow.dsl.connector.api.serde.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.util.TypeCastUtil;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;

public class JsonDeserializer implements TableDeserializer<String> {

    private StructType schema;

    private ObjectMapper mapper;

    private boolean ignoreParseError;

    private boolean failOnMissingField;


    @Override
    public void init(Configuration conf, StructType schema) {
        this.schema = Objects.requireNonNull(schema);
        this.mapper = new ObjectMapper();
        this.ignoreParseError = conf.getBoolean(ConnectorConfigKeys.GEAFLOW_DSL_CONNECTOR_FORMAT_JSON_IGNORE_PARSE_ERROR);
        this.failOnMissingField = conf.getBoolean(ConnectorConfigKeys.GEAFLOW_DSL_CONNECTOR_FORMAT_JSON_FAIL_ON_MISSING_FIELD);

    }

    @Override
    public List<Row> deserialize(String record) {
        if (record == null || record.isEmpty()) {
            return Collections.emptyList();
        }
        Object[] values = new Object[schema.size()];
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(record);
        } catch (JsonProcessingException e) {
            // handle exception according to configuration
            if (ignoreParseError) {
                // return empty list
                return Collections.emptyList();
            } else {
                throw new GeaflowRuntimeException("fail to deserialize record " + record, e);
            }
        }
        // if json node is null
        for (int i = 0; i < schema.size(); i++) {
            String fieldName = schema.getFieldNames().get(i);
            if (failOnMissingField) {
                if (!jsonNode.has(fieldName)) {
                    throw new GeaflowRuntimeException("fail to deserialize record " + record + " due to  missing field " + fieldName);
                }
            }
            JsonNode value = jsonNode.get(fieldName);
            IType<?> type = schema.getType(i);
            // cast the value to the type defined in the schema.
            if (value != null) {
                values[i] = TypeCastUtil.cast(value.asText(), type);
            } else {
                values[i] = null;
            }

        }
        return Collections.singletonList(ObjectRow.create(values));
    }

}
