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

package org.apache.geaflow.dsl.common.binary;

import static org.apache.geaflow.dsl.common.binary.BinaryLayoutHelper.getArrayFieldOffset;
import static org.apache.geaflow.dsl.common.binary.BinaryLayoutHelper.getBitSetBytes;
import static org.apache.geaflow.dsl.common.binary.BinaryLayoutHelper.isSet;

import java.lang.reflect.Array;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Locale;
import org.apache.geaflow.common.binary.BinaryOperations;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.common.binary.IBinaryObject;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.ArrayType;
import org.apache.geaflow.dsl.common.util.FunctionCallUtils;

public class FieldReaderFactory {

    public interface PropertyFieldReader<V> {

        V read(IBinaryObject baseObject, long offset);
    }

    public static PropertyFieldReader<?> getPropertyFieldReader(IType<?> type) {
        String typeName = type.getName().toUpperCase(Locale.ROOT);
        switch (typeName) {
            case Types.TYPE_NAME_INTEGER:
                return BinaryOperations::getInt;
            case Types.TYPE_NAME_LONG:
                return BinaryOperations::getLong;
            case Types.TYPE_NAME_SHORT:
                return BinaryOperations::getShort;
            case Types.TYPE_NAME_DOUBLE:
                return BinaryOperations::getDouble;
            case Types.TYPE_NAME_BINARY_STRING:
                return (baseObject, offset) -> {
                    int size = BinaryOperations.getInt(baseObject, offset);
                    long stringOffset = getContentOffset(baseObject, offset);
                    return new BinaryString(baseObject, stringOffset, size);
                };
            case Types.TYPE_NAME_BOOLEAN:
                return (baseObject, offset) -> BinaryOperations.getInt(baseObject, offset) == 1;
            case Types.TYPE_NAME_TIMESTAMP:
                return (baseObject, offset) -> new Timestamp(BinaryOperations.getLong(baseObject,
                    offset));
            case Types.TYPE_NAME_DATE:
                return (baseObject, offset) -> new Date(BinaryOperations.getLong(baseObject,
                    offset));
            case Types.TYPE_NAME_OBJECT:
            case Types.TYPE_NAME_VERTEX:
            case Types.TYPE_NAME_EDGE:
                return (baseObject, offset) -> {
                    int size = BinaryOperations.getInt(baseObject, offset);
                    long bytesOffset = getContentOffset(baseObject, offset);
                    byte[] objectBytes = new byte[size];
                    BinaryOperations.copyMemory(baseObject, bytesOffset, objectBytes, 0, size);
                    return SerializerFactory.getKryoSerializer().deserialize(objectBytes);
                };
            case Types.TYPE_NAME_ARRAY:
                return (baseObject, offset) -> {
                    int arraySize = BinaryOperations.getInt(baseObject, offset);
                    long arrayOffset = getContentOffset(baseObject, offset);

                    IType componentType = ((ArrayType) type).getComponentType();
                    Object[] array =
                        (Object[]) Array.newInstance(
                            FunctionCallUtils.typeClass(componentType.getTypeClass(), true), arraySize);

                    for (int i = 0; i < arraySize; i++) {
                        if (isSet(baseObject, arrayOffset, i)) {
                            array[i] = null;
                        } else {
                            long elementOffset = arrayOffset + getArrayFieldOffset(getBitSetBytes(arraySize), i);

                            PropertyFieldReader elementReader = getPropertyFieldReader(componentType);
                            array[i] = elementReader.read(baseObject, elementOffset);
                        }
                    }
                    return array;
                };
            default:
                throw new GeaFlowDSLException("field type not supported: " + typeName);
        }
    }

    private static long getContentOffset(IBinaryObject baseObject, long headOffset) {
        return BinaryOperations.getInt(baseObject, headOffset + 4);
    }
}
