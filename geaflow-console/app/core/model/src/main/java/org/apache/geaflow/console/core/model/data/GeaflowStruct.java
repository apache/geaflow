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

package org.apache.geaflow.console.core.model.data;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.type.GeaflowStructType;

@Getter
@Setter
public abstract class GeaflowStruct extends GeaflowData {

    protected final Map<String, GeaflowField> fields = new LinkedHashMap<>();
    protected GeaflowStructType type;

    public GeaflowStruct(GeaflowStructType type) {
        this.type = type;
    }

    public void addField(GeaflowField field) {
        fields.put(field.getName(), field);
    }

    public void addFields(List<GeaflowField> fields) {
        for (GeaflowField field : fields) {
            String fieldName = field.getName();
            if (this.fields.containsKey(fieldName)) {
                throw new GeaflowException("Field name {} duplicated", fieldName);
            }
            this.fields.put(fieldName, field);
        }
    }

    public void removeField(String name) {
        this.fields.remove(name);
    }

    public void removeFields(List<String> names) {
        for (String name : names) {
            this.fields.remove(name);
        }
    }
}
