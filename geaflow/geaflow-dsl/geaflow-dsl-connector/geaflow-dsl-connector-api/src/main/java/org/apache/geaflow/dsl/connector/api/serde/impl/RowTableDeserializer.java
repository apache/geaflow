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

package org.apache.geaflow.dsl.connector.api.serde.impl;

import java.util.Collections;
import java.util.List;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.ObjectType;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.util.TypeCastUtil;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;

public class RowTableDeserializer implements TableDeserializer<Row> {

    private StructType schema;

    @Override
    public void init(Configuration conf, StructType schema) {
        this.schema = schema;
    }

    @Override
    public List<Row> deserialize(Row record) {
        Object[] values = new Object[schema.size()];

        for (int i = 0; i < schema.size(); i++) {
            IType<?> type = schema.getType(i);
            // cast the value to the type defined in the schema.
            values[i] = TypeCastUtil.cast(record.getField(i, ObjectType.INSTANCE), type);
        }
        return Collections.singletonList(ObjectRow.create(values));
    }
}
