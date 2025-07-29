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

package org.apache.geaflow.common.schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaImpl implements ISchema {

    private final int id;
    private final String name;
    private final List<Field> fields;
    private final Map<String, Field> fieldMap;

    public SchemaImpl(String name, List<Field> fields) {
        this(0, name, fields);
    }

    public SchemaImpl(int id, String name, List<Field> fields) {
        this.id = id;
        this.name = name;
        this.fields = Collections.unmodifiableList(fields);
        this.fieldMap = Collections.unmodifiableMap(generateFieldMap(fields));
    }

    private static Map<String, Field> generateFieldMap(List<Field> fields) {
        Map<String, Field> map = new HashMap<>(fields.size());
        for (Field field : fields) {
            map.put(field.getName(), field);
        }
        return map;
    }

    @Override
    public int getSchemaId() {
        return this.id;
    }

    @Override
    public String getSchemaName() {
        return this.name;
    }

    @Override
    public Field getField(int index) {
        return this.fields.get(index);
    }

    @Override
    public Field getField(String fieldName) {
        return this.fieldMap.get(fieldName);
    }

    @Override
    public List<Field> getFields() {
        return this.fields;
    }

}
