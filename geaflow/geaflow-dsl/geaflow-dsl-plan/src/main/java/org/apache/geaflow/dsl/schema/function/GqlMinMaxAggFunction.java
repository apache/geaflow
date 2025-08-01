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

package org.apache.geaflow.dsl.schema.function;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.geaflow.dsl.calcite.MetaFieldType;

public class GqlMinMaxAggFunction extends SqlMinMaxAggFunction {

    public GqlMinMaxAggFunction(SqlKind kind) {
        super(kind);
    }

    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        RelDataType superType = super.inferReturnType(opBinding);
        if (superType instanceof MetaFieldType) {
            return ((MetaFieldType) superType).getType();
        }
        return superType;
    }
}
