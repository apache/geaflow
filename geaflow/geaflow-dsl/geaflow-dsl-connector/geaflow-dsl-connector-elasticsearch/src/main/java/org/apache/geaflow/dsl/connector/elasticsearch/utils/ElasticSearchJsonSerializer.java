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

package org.apache.geaflow.dsl.connector.elasticsearch.utils;

import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.StructType;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticSearchJsonSerializer {
    private final StructType schema;
    private final String timestampField;

    public ElasticSearchJsonSerializer(StructType schema, String timestampField) {
        this.schema = schema;
        this.timestampField = timestampField;
    }

    public Map<String, Object> convert(Row row) {
        Map<String, Object> document = new HashMap<>();
        List<String> fieldNames = schema.getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            Object fieldValue = row.getField(i, schema.getType(i));
            document.put(fieldName, fieldValue);
        }

        // add timestamp field
        if (!document.containsKey(timestampField)) {
            document.put(timestampField, System.currentTimeMillis());
        }

        return document;
    }
}