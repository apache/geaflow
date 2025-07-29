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

package org.apache.geaflow.dsl.catalog;

import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.*;

/**
 * A bridge between GeaFlow's instance catalog to calcite schema.
 */
public class InstanceCalciteSchema implements Schema {

    private final String instanceName;

    private final Catalog catalog;

    public InstanceCalciteSchema(String instanceName, Catalog catalog) {
        this.instanceName = Objects.requireNonNull(instanceName);
        this.catalog = Objects.requireNonNull(catalog);
    }

    @Override
    public Table getTable(String name) {
        //At present, Calcite only has one Table data model.
        // The Graph data model inherits from the Table data model.
        // During validator inference, it is impossible to distinguish whether it is a graph or a
        // table based on identifier. It is necessary to read the catalog separately.
        Table table;
        try {
            table = catalog.getTable(instanceName, name);
        } catch (Exception e) {
            table = null;
        }
        if (table != null) {
            return table;
        }
        return catalog.getGraph(instanceName, name);
    }

    @Override
    public Set<String> getTableNames() {
        return catalog.listGraphAndTable(instanceName);
    }

    @Override
    public RelProtoDataType getType(String name) {
        return null;
    }

    @Override
    public Set<String> getTypeNames() {
        return Sets.newHashSet();
    }

    @Override
    public Collection<Function> getFunctions(String name) {
        return Collections.emptyList();
    }

    @Override
    public Set<String> getFunctionNames() {
        return Sets.newHashSet();
    }

    @Override
    public SchemaPlus getSubSchema(String name) {
        return null;
    }

    @Override
    public Set<String> getSubSchemaNames() {
        return null;
    }

    @Override
    public Expression getExpression(SchemaPlus parentSchema, String name) {
        return Schemas.subSchemaExpression(parentSchema, name, getClass());
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
        return this;
    }
}
