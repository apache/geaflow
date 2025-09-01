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

package org.apache.geaflow.dsl.runtime.function.table;

import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.ParameterizedRow;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowKey;
import org.apache.geaflow.dsl.common.data.impl.DefaultRowKeyWithRequestId;
import org.apache.geaflow.dsl.common.data.impl.ObjectRowKey;

public class GroupByFunctionImpl implements GroupByFunction {

    private final int[] keyFieldIndices;

    private final IType<?>[] keyFieldTypes;

    public GroupByFunctionImpl(int[] keyFieldIndices, IType<?>[] keyFieldTypes) {
        assert keyFieldIndices.length == keyFieldTypes.length;

        this.keyFieldIndices = keyFieldIndices;
        this.keyFieldTypes = keyFieldTypes;
    }

    @Override
    public RowKey getRowKey(Row row) {
        Object[] keys = new Object[keyFieldIndices.length];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = row.getField(keyFieldIndices[i], keyFieldTypes[i]);
        }
        RowKey key = ObjectRowKey.of(keys);
        if (row instanceof ParameterizedRow) {
            return new DefaultRowKeyWithRequestId(((ParameterizedRow) row).getRequestId(), key);
        }
        return key;
    }

    @Override
    public IType<?>[] getFieldTypes() {
        return keyFieldTypes;
    }

    @Override
    public int[] getKeyFieldIndices() {
        return keyFieldIndices;
    }
}
