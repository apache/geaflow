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

package org.apache.geaflow.dsl.rex;

import java.util.Objects;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBiVisitor;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.type.SqlTypeName;

public class RexSystemVariable extends RexSlot {

    public RexSystemVariable(String name, int index, RelDataType type) {
        super(name, index, type);
    }

    @Override
    public <R> R accept(RexVisitor<R> visitor) {
        return visitor.visitOther(this);
    }

    @Override
    public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
        return visitor.visitOther(this, arg);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RexSystemVariable)) {
            return false;
        }
        RexSystemVariable systemVariable = (RexSystemVariable) obj;
        return Objects.equals(name, systemVariable.name) && index == systemVariable.index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, index);
    }

    public enum SystemVariable {
        LOOP_COUNTER("loopCounter", 0, SqlTypeName.INTEGER),
        ;

        private final String name;

        private final int index;

        private final SqlTypeName typeName;

        SystemVariable(String name, int index, SqlTypeName typeName) {
            this.name = name;
            this.index = index;
            this.typeName = typeName;
        }

        public String getName() {
            return name;
        }

        public int getIndex() {
            return index;
        }

        public SqlTypeName getTypeName() {
            return typeName;
        }

        public RexSystemVariable toRexNode(RelDataTypeFactory typeFactory) {
            return new RexSystemVariable(name, index, typeFactory.createSqlType(typeName));
        }

        public static SystemVariable of(String name) {
            if (name.equalsIgnoreCase(LOOP_COUNTER.getName())) {
                return LOOP_COUNTER;
            }
            throw new IllegalArgumentException("Not support system variable: " + name);
        }
    }
}
