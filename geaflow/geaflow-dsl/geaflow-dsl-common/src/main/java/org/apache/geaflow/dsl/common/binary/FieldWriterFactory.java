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

import static org.apache.geaflow.dsl.common.binary.BinaryLayoutHelper.zeroBytes;

import java.util.Locale;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.ArrayType;

public class FieldWriterFactory {

    public interface PropertyFieldWriter<V> {

        void write(WriterBuffer writerBuffer, long nullBitsOffset, int index, V value);
    }

    public static PropertyFieldWriter<?> getPropertyFieldWriter(IType<?> type) {
        String typeName = type.getName().toUpperCase(Locale.ROOT);
        switch (typeName) {
            case Types.TYPE_NAME_INTEGER:
                return (PropertyFieldWriter<Integer>) (writerBuffer, nullBitsOffset, index, value) -> {
                    if (value == null) {
                        writerBuffer.setNullAt(nullBitsOffset, index);
                    } else {
                        writerBuffer.writeIntAlign(value);
                    }
                };
            case Types.TYPE_NAME_LONG:
                return (PropertyFieldWriter<Long>) (writerBuffer, nullBitsOffset, index, value) -> {
                    if (value == null) {
                        writerBuffer.setNullAt(nullBitsOffset, index);
                    } else {
                        writerBuffer.writeLong(value);
                    }
                };
            case Types.TYPE_NAME_SHORT:
                return (PropertyFieldWriter<Short>) (writerBuffer, nullBitsOffset, index, value) -> {
                    if (value == null) {
                        writerBuffer.setNullAt(nullBitsOffset, index);
                    } else {
                        writerBuffer.writeShortAlign(value);
                    }
                };
            case Types.TYPE_NAME_DOUBLE:
                return (PropertyFieldWriter<Double>) (writerBuffer, nullBitsOffset, index, value) -> {
                    if (value == null) {
                        writerBuffer.setNullAt(nullBitsOffset, index);
                    } else {
                        writerBuffer.writeDouble(value);
                    }
                };
            case Types.TYPE_NAME_BINARY_STRING:
                return (PropertyFieldWriter<BinaryString>) (writerBuffer, nullBitsOffset, index, value) -> {
                    writeString(writerBuffer, nullBitsOffset, index, value);
                };
            case Types.TYPE_NAME_BOOLEAN:
                return (PropertyFieldWriter<Boolean>) (writerBuffer, nullBitsOffset, index,
                                                       value) -> {
                    if (value == null) {
                        writerBuffer.setNullAt(nullBitsOffset, index);
                    } else {
                        writerBuffer.writeIntAlign(value ? 1 : 0);
                    }
                };
            case Types.TYPE_NAME_TIMESTAMP:
            case Types.TYPE_NAME_DATE:
                return (PropertyFieldWriter<java.util.Date>) (writerBuffer, nullBitsOffset, index, value) -> {
                    if (value == null) {
                        writerBuffer.setNullAt(nullBitsOffset, index);
                    } else {
                        writerBuffer.writeLong(value.getTime());
                    }
                };
            case Types.TYPE_NAME_OBJECT:
            case Types.TYPE_NAME_VERTEX:
            case Types.TYPE_NAME_EDGE:
                return (PropertyFieldWriter<Object>) (writerBuffer, nullBitsOffset, index, value) -> {
                    byte[] bytes = null;
                    if (value != null) {
                        bytes = SerializerFactory.getKryoSerializer().serialize(value);
                    }
                    writeBytes(writerBuffer, nullBitsOffset, index, bytes);
                };
            case Types.TYPE_NAME_ARRAY:
                ArrayType arrayType = (ArrayType) type;
                return (PropertyFieldWriter<Object>) (writerBuffer, nullBitsOffset, index, value) -> {
                    if (value == null) {
                        writerBuffer.setNullAt(nullBitsOffset, index);
                    } else {
                        Object[] array = (Object[]) value;
                        int currentCursor = writerBuffer.getCursor();
                        final int arrayStartOffset = writerBuffer.getExtendPoint();
                        writerBuffer.moveToExtend();

                        writeArray(writerBuffer, array, arrayType.getComponentType());
                        writerBuffer.setCursor(currentCursor);
                        // write array length
                        writerBuffer.writeInt(array.length);
                        // write array content offset
                        writerBuffer.writeInt(arrayStartOffset);
                    }
                };
            default:
                throw new GeaFlowDSLException("field type not supported: " + typeName);
        }
    }

    public static void writeArray(WriterBuffer writerBuffer, Object[] array, IType componentType) {
        int startCursor = writerBuffer.getCursor();
        byte[] nullBitSet = new byte[BinaryLayoutHelper.getBitSetBytes(array.length)];
        // nullBits length + array-length * 8
        int baseSizeNeed = nullBitSet.length + array.length * 8;
        writerBuffer.grow(baseSizeNeed);
        writerBuffer.setExtendPoint(startCursor + baseSizeNeed);

        long nullBitOffset = writerBuffer.getCursor();
        // clear null bits
        zeroBytes(nullBitSet, 0, nullBitSet.length);
        // write null bits
        writerBuffer.writeBytes(nullBitSet);

        for (int i = 0; i < array.length; i++) {
            PropertyFieldWriter writer = getPropertyFieldWriter(componentType);
            writer.write(writerBuffer, nullBitOffset, i, array[i]);
        }
    }

    public static void writeString(WriterBuffer writerBuffer, long baseOffset, int index,
                                   BinaryString string) {
        if (string == null) {
            writerBuffer.setNullAt(baseOffset, index);
        } else {
            int bytesLength = string.getNumBytes();
            // write bytes size
            writerBuffer.writeInt(bytesLength);
            // write bytes offset
            writerBuffer.writeInt(writerBuffer.getExtendPoint());
            // save current cursor
            int currentCursor = writerBuffer.getCursor();
            // move cursor to extend region
            writerBuffer.moveToExtend();
            // grow buffer
            writerBuffer.growTo(writerBuffer.getExtendPoint() + bytesLength);
            writerBuffer.writeBytes(string.getBinaryObject(), string.getOffset(), bytesLength);
            // set new extend point
            writerBuffer.setExtendPoint(writerBuffer.getCursor());
            // reset cursor
            writerBuffer.setCursor(currentCursor);
        }
    }

    public static void writeBytes(WriterBuffer writerBuffer, long baseOffset, int index, byte[] bytes) {
        if (bytes == null) {
            writerBuffer.setNullAt(baseOffset, index);
        } else {
            // write bytes size
            writerBuffer.writeInt(bytes.length);
            // write bytes offset
            writerBuffer.writeInt(writerBuffer.getExtendPoint());
            // save current cursor
            int currentCursor = writerBuffer.getCursor();
            // move cursor to extend region
            writerBuffer.moveToExtend();
            // grow buffer
            writerBuffer.growTo(writerBuffer.getExtendPoint() + bytes.length);
            writerBuffer.writeBytes(bytes);
            // set new extend point
            writerBuffer.setExtendPoint(writerBuffer.getCursor());
            // reset cursor
            writerBuffer.setCursor(currentCursor);
        }
    }
}
