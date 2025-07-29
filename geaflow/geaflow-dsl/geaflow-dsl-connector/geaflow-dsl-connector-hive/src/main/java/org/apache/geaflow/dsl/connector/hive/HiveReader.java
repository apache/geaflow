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

package org.apache.geaflow.dsl.connector.hive;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.geaflow.common.utils.ClassUtil;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.hive.HiveTableSource.HiveOffset;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

public class HiveReader {

    private final RecordReader<Writable, Writable> recordReader;
    private final StructType readSchema;
    private final Deserializer deserializer;


    public HiveReader(RecordReader<Writable, Writable> recordReader, StructType readSchema,
                      StorageDescriptor sd, Properties tableProps) {
        this.recordReader = recordReader;
        this.readSchema = new StructType(readSchema.getFields().stream().map(
            f -> new TableField(f.getName().toLowerCase(Locale.ROOT), f.getType(), f.isNullable()))
            .collect(Collectors.toList()));
        this.deserializer = ClassUtil.newInstance(sd.getSerdeInfo().getSerializationLib());
        try {
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            SerDeUtils.initializeSerDe(deserializer, conf, tableProps, null);

        } catch (SerDeException e) {
            throw new GeaFlowDSLException(e);
        }
    }

    public FetchData<Row> read(long windowSize, String[] partitionValues) {
        if (windowSize == Long.MAX_VALUE) {
            Iterator<Row> hiveIterator = new HiveIterator(recordReader, deserializer, partitionValues, readSchema);
            return FetchData.createBatchFetch(hiveIterator, new HiveOffset(-1L));
        } else {
            throw new GeaFlowDSLException("Cannot support stream read for hive");
        }
    }

    private static class HiveIterator implements Iterator<Row> {

        private final RecordReader<Writable, Writable> recordReader;
        private final Deserializer deserializer;
        private final String[] partitionValues;
        private final StructType readSchema;

        private final Map<String, StructField> name2Fields = new HashMap<>();

        private final Writable key;
        private final Writable value;


        public HiveIterator(RecordReader<Writable, Writable> recordReader,
                            Deserializer deserializer,
                            String[] partitionValues,
                            StructType readSchema) {
            this.recordReader = recordReader;
            this.deserializer = deserializer;
            this.partitionValues = partitionValues;
            this.readSchema = readSchema;
            key = recordReader.createKey();
            value = recordReader.createValue();

            try {
                StructObjectInspector structObjectInspector = (StructObjectInspector) deserializer.getObjectInspector();
                for (StructField field : structObjectInspector.getAllStructFieldRefs()) {
                    name2Fields.put(field.getFieldName(), field);
                }
            } catch (Exception e) {
                throw new GeaFlowDSLException(e);
            }
        }

        @Override
        public boolean hasNext() {
            try {
                return recordReader.next(key, value);
            } catch (IOException e) {
                throw new GeaFlowDSLException(e);
            }
        }

        @Override
        public Row next() {
            try {
                Object hiveRowStruct = deserializer.deserialize(value);
                StructObjectInspector structObjectInspector = (StructObjectInspector) deserializer.getObjectInspector();
                Object[] values = convertHiveStructToRow(hiveRowStruct, structObjectInspector);
                if (partitionValues.length > 0) { // append partition values.
                    Object[] valueWithPartitions = new Object[values.length + partitionValues.length];
                    System.arraycopy(values, 0, valueWithPartitions, 0, values.length);
                    System.arraycopy(partitionValues, 0, valueWithPartitions,
                        values.length, partitionValues.length);
                    values = valueWithPartitions;
                }
                return ObjectRow.create(values);
            } catch (Exception e) {
                throw new GeaFlowDSLException(e);
            }
        }

        private Object[] convertHiveStructToRow(Object hiveRowStruct, StructObjectInspector structObjectInspector) {
            Object[] values = new Object[readSchema.size()];
            for (int i = 0; i < values.length; i++) {
                String fieldName = readSchema.getField(i).getName();
                StructField field = name2Fields.get(fieldName);
                if (field != null) {
                    values[i] = toSqlValue(
                        structObjectInspector.getStructFieldData(hiveRowStruct, field),
                        field.getFieldObjectInspector());
                } else {
                    values[i] = null;
                }
            }
            return values;
        }

        private Object toSqlValue(Object hiveValue, ObjectInspector fieldInspector) {
            if (fieldInspector instanceof PrimitiveObjectInspector) {
                PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) fieldInspector;
                return primitiveObjectInspector.getPrimitiveJavaObject(hiveValue);
            }
            throw new GeaFlowDSLException("Complex type:{} have not support", fieldInspector.getTypeName());
        }
    }
}
