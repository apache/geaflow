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

package org.apache.geaflow.ai.graph.io;

import java.util.List;

public class VertexSchema implements Schema {

    private final String label;
    private final List<String> fields;
    private final String idField;

    public VertexSchema(String label, String idField, List<String> fields) {
        this.idField = idField;
        this.fields = fields;
        this.label = label;
    }

    @Override
    public List<String> getFields() {
        return fields;
    }

    public String getLabel() {
        return label;
    }

    public String getIdField() {
        return idField;
    }

    @Override
    public String getName() {
        return label;
    }
}
