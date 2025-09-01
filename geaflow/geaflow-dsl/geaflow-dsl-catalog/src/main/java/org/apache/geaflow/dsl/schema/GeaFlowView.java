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

package org.apache.geaflow.dsl.schema;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.commons.lang3.StringUtils;

public class GeaFlowView extends ViewTable implements Serializable {

    private final String instanceName;

    private final String name;

    private final List<String> fields;

    private final boolean ifNotExists;

    public GeaFlowView(String instanceName, String name, List<String> fields, RelDataType rowType,
                       String viewSql, boolean ifNotExists) {
        super(null, t -> rowType, viewSql, Collections.emptyList(), Collections.emptyList());
        this.instanceName = instanceName;
        this.name = Objects.requireNonNull(name, "name is null");
        this.fields = Objects.requireNonNull(fields, "fields is null");
        this.ifNotExists = ifNotExists;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public String getName() {
        return name;
    }

    public List<String> getFields() {
        return fields;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    @Override
    public String toString() {
        return "Create View " + name + "("
            + StringUtils.join(fields, ",")
            + ") AS"
            + getViewSql();
    }
}
