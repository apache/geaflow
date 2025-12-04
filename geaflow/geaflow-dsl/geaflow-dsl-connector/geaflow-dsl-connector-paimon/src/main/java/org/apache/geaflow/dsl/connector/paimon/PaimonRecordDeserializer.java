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

package org.apache.geaflow.dsl.connector.paimon;

import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.paimon.data.InternalRow;

public class PaimonRecordDeserializer {

    private StructType schema;

    public void init(StructType schema) {
        TableSchema tableSchema = (TableSchema) schema;
        this.schema = tableSchema.getDataSchema();
    }

    public Row deserialize(Object record) {
        if (record == null) {
            return null;
        }
        InternalRow internalRow = (InternalRow) record;
        assert internalRow.getFieldCount() == schema.size();
        Object[] values = new Object[schema.size()];
        for (int i = 0; i < schema.size(); i++) {
            TableField field = this.schema.getField(i);
            switch (field.getType().getName()) {
                case Types.TYPE_NAME_BOOLEAN:
                    values[i] = internalRow.getBoolean(i);
                    break;
                case Types.TYPE_NAME_BYTE:
                    values[i] = internalRow.getByte(i);
                    break;
                case Types.TYPE_NAME_DOUBLE:
                    values[i] = internalRow.getDouble(i);
                    break;
                case Types.TYPE_NAME_FLOAT:
                    values[i] = internalRow.getFloat(i);
                    break;
                case Types.TYPE_NAME_INTEGER:
                    values[i] = internalRow.getInt(i);
                    break;
                case Types.TYPE_NAME_LONG:
                    values[i] = internalRow.getLong(i);
                    break;
                case Types.TYPE_NAME_STRING:
                case Types.TYPE_NAME_BINARY_STRING:
                    values[i] = internalRow.getString(i);
                    break;
                default:
                    throw new GeaFlowDSLException("Type: {} not support",
                        field.getType().getName());
            }
        }
        return ObjectRow.create(values);
    }
}
