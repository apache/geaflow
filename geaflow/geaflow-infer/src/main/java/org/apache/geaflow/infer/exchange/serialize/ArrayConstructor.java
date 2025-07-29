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

package org.apache.geaflow.infer.exchange.serialize;

import java.util.ArrayList;

public class ArrayConstructor implements IObjectConstructor {

    public Object construct(Object[] args) throws PickleException {
        if (args.length == 4) {
            ArrayConstructor constructor = (ArrayConstructor) args[0];
            char typeCode = ((String) args[1]).charAt(0);
            int machineCodeType = (Integer) args[2];
            byte[] data = (byte[]) args[3];
            return constructor.construct(typeCode, machineCodeType, data);
        }
        if (args.length != 2) {
            throw new PickleException(
                "invalid pickle data for array; expected 2 args, got " + args.length);
        }

        String typeCode = (String) args[0];
        if (args[1] instanceof String) {
            throw new PickleException("unsupported Python 2.6 array pickle format");
        }
        ArrayList<Object> values = (ArrayList<Object>) args[1];

        switch (typeCode.charAt(0)) {
            case 'c':
            case 'u': {
                char[] result = new char[values.size()];
                int i = 0;
                for (Object c : values) {
                    result[i++] = ((String) c).charAt(0);
                }
                return result;
            }
            case 'b': {
                byte[] result = new byte[values.size()];
                int i = 0;
                for (Object c : values) {
                    result[i++] = ((Number) c).byteValue();
                }
                return result;
            }
            case 'B':
            case 'h': {
                short[] result = new short[values.size()];
                int i = 0;
                for (Object c : values) {
                    result[i++] = ((Number) c).shortValue();
                }
                return result;
            }
            // 列表元素为int类型
            case 'H':
            case 'i':
            case 'l': {
                int[] result = new int[values.size()];
                int i = 0;
                for (Object c : values) {
                    result[i++] = ((Number) c).intValue();
                }
                return result;
            }
            case 'I':
            case 'L': {
                long[] result = new long[values.size()];
                int i = 0;
                for (Object c : values) {
                    result[i++] = ((Number) c).longValue();
                }
                return result;
            }
            case 'f': {
                float[] result = new float[values.size()];
                int i = 0;
                for (Object c : values) {
                    result[i++] = ((Number) c).floatValue();
                }
                return result;
            }

            case 'd': {
                double[] result = new double[values.size()];
                int i = 0;
                for (Object c : values) {
                    result[i++] = ((Number) c).doubleValue();
                }
                return result;
            }
            default:
                throw new PickleException("invalid array typecode: " + typeCode);
        }
    }


    public Object construct(char typeCode, int machineCode, byte[] data) throws PickleException {
        if (machineCode < 0) {
            throw new PickleException("unknown machine type format");
        }
        switch (typeCode) {
            case 'c':
            case 'u': {
                if (machineCode != 18 && machineCode != 19 && machineCode != 20
                    && machineCode != 21) {
                    throw new PickleException("for c/u type must be 18/19/20/21");
                }
                if (machineCode == 18 || machineCode == 19) {
                    if (data.length % 2 != 0) {
                        throw new PickleException("data size alignment error");
                    }
                    return constructCharArrayUTF16(machineCode, data);
                } else {
                    if (data.length % 4 != 0) {
                        throw new PickleException("data size alignment error");
                    }
                    return constructCharArrayUTF32(machineCode, data);
                }
            }
            case 'b': {
                if (machineCode != 1) {
                    throw new PickleException("for b type must be 1");
                }
                return data;
            }
            case 'B': {
                if (machineCode != 0) {
                    throw new PickleException("for B type must be 0");
                }
                return constructShortArrayFromUByte(data);
            }
            case 'h': {
                if (machineCode != 4 && machineCode != 5) {
                    throw new PickleException("for h type must be 4/5");
                }
                if (data.length % 2 != 0) {
                    throw new PickleException("data size alignment error");
                }
                return constructShortArraySigned(machineCode, data);
            }
            case 'H': {
                if (machineCode != 2 && machineCode != 3) {
                    throw new PickleException("for H type must be 2/3");
                }
                if (data.length % 2 != 0) {
                    throw new PickleException("data size alignment error");
                }
                return constructIntArrayFromUShort(machineCode, data);
            }
            case 'i': {
                if (machineCode != 8 && machineCode != 9) {
                    throw new PickleException("for i type must be 8/9");
                }
                if (data.length % 4 != 0) {
                    throw new PickleException("data size alignment error");
                }
                return constructIntArrayFromInt32(machineCode, data);
            }
            case 'l': {
                if (machineCode != 8 && machineCode != 9 && machineCode != 12
                    && machineCode != 13) {
                    throw new PickleException("for l type must be 8/9/12/13");
                }
                if ((machineCode == 8 || machineCode == 9) && (data.length % 4 != 0)) {
                    throw new PickleException("data size alignment error");
                }
                if ((machineCode == 12 || machineCode == 13) && (data.length % 8 != 0)) {
                    throw new PickleException("data size alignment error");
                }
                if (machineCode == 8 || machineCode == 9) {
                    return constructIntArrayFromInt32(machineCode, data);
                } else {
                    return constructLongArrayFromInt64(machineCode, data);
                }
            }
            case 'I': {
                if (machineCode != 6 && machineCode != 7) {
                    throw new PickleException("for I type must be 6/7");
                }
                if (data.length % 4 != 0) {
                    throw new PickleException("data size alignment error");
                }
                return constructLongArrayFromUInt32(machineCode, data);
            }
            case 'L': {
                if (machineCode != 6 && machineCode != 7 && machineCode != 10
                    && machineCode != 11) {
                    throw new PickleException("for L type must be 6/7/10/11");
                }
                if ((machineCode == 6 || machineCode == 7) && (data.length % 4 != 0)) {
                    throw new PickleException("data size alignment error");
                }
                if ((machineCode == 10 || machineCode == 11) && (data.length % 8 != 0)) {
                    throw new PickleException("data size alignment error");
                }
                if (machineCode == 6 || machineCode == 7) {
                    // 32 bits
                    return constructLongArrayFromUInt32(machineCode, data);
                } else {
                    // 64 bits
                    return constructLongArrayFromUInt64(machineCode, data);
                }
            }
            case 'f': {
                if (machineCode != 14 && machineCode != 15) {
                    throw new PickleException("for f type must be 14/15");
                }
                if (data.length % 4 != 0) {
                    throw new PickleException("data size alignment error");
                }
                return constructFloatArray(machineCode, data);
            }
            case 'd': {
                if (machineCode != 16 && machineCode != 17) {
                    throw new PickleException("for d type must be 16/17");
                }
                if (data.length % 8 != 0) {
                    throw new PickleException("data size alignment error");
                }
                return constructDoubleArray(machineCode, data);
            }
            default:
                throw new PickleException("invalid array typecode: " + typeCode);
        }
    }

    protected int[] constructIntArrayFromInt32(int machineCode, byte[] data) {
        int[] result = new int[data.length / 4];
        byte[] bigEnd = new byte[4];
        for (int i = 0; i < data.length / 4; i++) {
            if (machineCode == 8) {
                result[i] = PickleUtils.bytes2Integer(data, i * 4, 4);
            } else {
                bigEnd[0] = data[3 + i * 4];
                bigEnd[1] = data[2 + i * 4];
                bigEnd[2] = data[1 + i * 4];
                bigEnd[3] = data[i * 4];
                result[i] = PickleUtils.bytes2Integer(bigEnd);
            }
        }
        return result;
    }

    protected long[] constructLongArrayFromUInt32(int machineCode, byte[] data) {
        long[] result = new long[data.length / 4];
        byte[] bigEnd = new byte[4];
        for (int i = 0; i < data.length / 4; i++) {
            if (machineCode == 6) {
                result[i] = PickleUtils.bytes2Uint(data, i * 4);
            } else {
                bigEnd[0] = data[3 + i * 4];
                bigEnd[1] = data[2 + i * 4];
                bigEnd[2] = data[1 + i * 4];
                bigEnd[3] = data[i * 4];
                result[i] = PickleUtils.bytes2Uint(bigEnd, 0);
            }
        }
        return result;
    }

    protected long[] constructLongArrayFromUInt64(int machineCode, byte[] data) {
        throw new PickleException("unsupported datatype: 64-bits unsigned long");
    }

    protected long[] constructLongArrayFromInt64(int machineCode, byte[] data) {
        long[] result = new long[data.length / 8];
        byte[] bigEnd = new byte[8];
        for (int i = 0; i < data.length / 8; i++) {
            if (machineCode == 12) {
                result[i] = PickleUtils.bytes2Long(data, i * 8);
            } else {
                bigEnd[0] = data[7 + i * 8];
                bigEnd[1] = data[6 + i * 8];
                bigEnd[2] = data[5 + i * 8];
                bigEnd[3] = data[4 + i * 8];
                bigEnd[4] = data[3 + i * 8];
                bigEnd[5] = data[2 + i * 8];
                bigEnd[6] = data[1 + i * 8];
                bigEnd[7] = data[i * 8];
                result[i] = PickleUtils.bytes2Long(bigEnd, 0);
            }
        }
        return result;
    }

    protected double[] constructDoubleArray(int machineCode, byte[] data) {
        double[] result = new double[data.length / 8];
        byte[] bigEnd = new byte[8];
        for (int i = 0; i < data.length / 8; ++i) {
            if (machineCode == 17) {
                result[i] = PickleUtils.bytes2Double(data, i * 8);
            } else {
                bigEnd[0] = data[7 + i * 8];
                bigEnd[1] = data[6 + i * 8];
                bigEnd[2] = data[5 + i * 8];
                bigEnd[3] = data[4 + i * 8];
                bigEnd[4] = data[3 + i * 8];
                bigEnd[5] = data[2 + i * 8];
                bigEnd[6] = data[1 + i * 8];
                bigEnd[7] = data[i * 8];
                result[i] = PickleUtils.bytes2Double(bigEnd, 0);
            }
        }
        return result;
    }

    protected float[] constructFloatArray(int machineCode, byte[] data) {
        float[] result = new float[data.length / 4];
        byte[] bigEnd = new byte[4];
        for (int i = 0; i < data.length / 4; ++i) {
            if (machineCode == 15) {
                result[i] = PickleUtils.bytes2Float(data, i * 4);
            } else {
                bigEnd[0] = data[3 + i * 4];
                bigEnd[1] = data[2 + i * 4];
                bigEnd[2] = data[1 + i * 4];
                bigEnd[3] = data[i * 4];
                result[i] = PickleUtils.bytes2Float(bigEnd, 0);
            }
        }
        return result;
    }

    protected int[] constructIntArrayFromUShort(int machineCode, byte[] data) {
        int[] result = new int[data.length / 2];
        for (int i = 0; i < data.length / 2; ++i) {
            int b1 = data[i * 2] & 0xff;
            int b2 = data[1 + i * 2] & 0xff;
            if (machineCode == 2) {
                result[i] = (b2 << 8) | b1;
            } else {
                result[i] = (b1 << 8) | b2;
            }
        }
        return result;
    }

    protected short[] constructShortArraySigned(int machineCode, byte[] data) {
        short[] result = new short[data.length / 2];
        for (int i = 0; i < data.length / 2; ++i) {
            byte b1 = data[i * 2];
            byte b2 = data[1 + i * 2];
            if (machineCode == 4) {
                result[i] = (short) ((b2 << 8) | (b1 & 0xff));
            } else {
                result[i] = (short) ((b1 << 8) | (b2 & 0xff));
            }
        }
        return result;
    }

    protected short[] constructShortArrayFromUByte(byte[] data) {
        short[] result = new short[data.length];
        for (int i = 0; i < data.length; ++i) {
            result[i] = (short) (data[i] & 0xff);
        }
        return result;
    }

    protected char[] constructCharArrayUTF32(int machineCode, byte[] data) {
        char[] result = new char[data.length / 4];
        byte[] bigEndian = new byte[4];
        for (int index = 0; index < data.length / 4; ++index) {
            if (machineCode == 20) {
                int codepoint = PickleUtils.bytes2Integer(data, index * 4, 4);
                char[] cc = Character.toChars(codepoint);
                if (cc.length > 1) {
                    throw new PickleException(
                        "cannot process UTF-32 character codepoint " + codepoint);
                }
                result[index] = cc[0];
            } else {
                bigEndian[0] = data[3 + index * 4];
                bigEndian[1] = data[2 + index * 4];
                bigEndian[2] = data[1 + index * 4];
                bigEndian[3] = data[index * 4];
                int codepoint = PickleUtils.bytes2Integer(bigEndian);
                char[] cc = Character.toChars(codepoint);
                if (cc.length > 1) {
                    throw new PickleException(
                        "cannot process UTF-32 character codepoint " + codepoint);
                }
                result[index] = cc[0];
            }
        }
        return result;
    }

    protected char[] constructCharArrayUTF16(int machineCode, byte[] data) {
        char[] result = new char[data.length / 2];
        byte[] bigEndian = new byte[2];
        for (int index = 0; index < data.length / 2; ++index) {
            if (machineCode == 18) {
                result[index] = (char) PickleUtils.bytes2Integer(data, index * 2, 2);
            } else {
                bigEndian[0] = data[1 + index * 2];
                bigEndian[1] = data[index * 2];
                result[index] = (char) PickleUtils.bytes2Integer(bigEndian);
            }
        }
        return result;
    }
}
