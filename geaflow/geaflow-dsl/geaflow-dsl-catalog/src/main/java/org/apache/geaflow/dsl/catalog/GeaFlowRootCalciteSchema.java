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
import java.util.Set;
import org.apache.calcite.jdbc.SimpleCalciteSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.*;
import org.apache.geaflow.dsl.catalog.exception.ObjectNotExistException;

public class GeaFlowRootCalciteSchema implements Schema {

    private final Catalog catalog;

    public GeaFlowRootCalciteSchema(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public Table getTable(String name) {
        return null;
    }

    @Override
    public Set<String> getTableNames() {
        return Sets.newHashSet();
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
        return Sets.newHashSet();
    }

    @Override
    public Set<String> getFunctionNames() {
        return Sets.newHashSet();
    }

    @Override
    public Schema getSubSchema(String name) {
        if (catalog.isInstanceExists(name)) {
            return new InstanceCalciteSchema(name, catalog);
        }
        throw new ObjectNotExistException("Instance '" + name + "' is not exists.");
    }

    @Override
    public Set<String> getSubSchemaNames() {
        return catalog.listInstances();
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

    public SchemaPlus plus() {
        return new SimpleCalciteSchema(null, this, "").plus();
    }
}
