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

package org.apache.geaflow.dsl.connector.odps.utils;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.Record;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.util.TypeCastUtil;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;

public class OdpsRecordDeserializer implements TableDeserializer<OdpsRecordWithPartitionSpec> {

    private StructType schema;

    private Map<String, Integer> columnName2Index;

    @Override
    public void init(Configuration conf, StructType schema) {
        this.schema = Objects.requireNonNull(schema);
        columnName2Index = new HashMap<>();
        for (int i = 0; i < schema.size(); i++) {
            columnName2Index.put(schema.getField(i).getName(), i);
        }
    }

    @Override
    public List<Row> deserialize(OdpsRecordWithPartitionSpec recordWithPartitionSpec) {
        if (recordWithPartitionSpec == null || recordWithPartitionSpec.record == null) {
            return Collections.emptyList();
        }
        Record item = recordWithPartitionSpec.record;
        Object[] objects = new Object[this.schema.size()];
        Column[] columns = item.getColumns();
        int colIndex = 0;
        for (Column col : columns) {
            String colName = col.getName();
            Integer index = columnName2Index.get(colName);
            if (index != null) {
                objects[index] = TypeCastUtil.cast(item.get(colIndex), this.schema.getType(colIndex));
            }
            colIndex++;
        }
        PartitionSpec spec = recordWithPartitionSpec.spec;
        if (spec != null) {
            for (String colName : spec.keys()) {
                Integer index = columnName2Index.get(colName);
                if (index != null) {
                    objects[index] = TypeCastUtil.cast(spec.get(colName), this.schema.getType(index));
                }
                colIndex++;
            }
        }
        return Collections.singletonList(ObjectRow.create(objects));
    }
}