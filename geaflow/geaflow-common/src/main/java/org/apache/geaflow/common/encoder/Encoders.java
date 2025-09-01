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

package org.apache.geaflow.common.encoder;

import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.common.encoder.impl.BooleanArrEncoder;
import org.apache.geaflow.common.encoder.impl.BooleanEncoder;
import org.apache.geaflow.common.encoder.impl.ByteArrEncoder;
import org.apache.geaflow.common.encoder.impl.ByteEncoder;
import org.apache.geaflow.common.encoder.impl.CharacterArrEncoder;
import org.apache.geaflow.common.encoder.impl.CharacterEncoder;
import org.apache.geaflow.common.encoder.impl.DoubleArrEncoder;
import org.apache.geaflow.common.encoder.impl.DoubleEncoder;
import org.apache.geaflow.common.encoder.impl.FloatArrEncoder;
import org.apache.geaflow.common.encoder.impl.FloatEncoder;
import org.apache.geaflow.common.encoder.impl.GenericArrayEncoder;
import org.apache.geaflow.common.encoder.impl.IntegerArrEncoder;
import org.apache.geaflow.common.encoder.impl.IntegerEncoder;
import org.apache.geaflow.common.encoder.impl.LongArrEncoder;
import org.apache.geaflow.common.encoder.impl.LongEncoder;
import org.apache.geaflow.common.encoder.impl.ShortArrEncoder;
import org.apache.geaflow.common.encoder.impl.ShortEncoder;
import org.apache.geaflow.common.encoder.impl.StringEncoder;
import org.apache.geaflow.common.encoder.impl.TripleEncoder;
import org.apache.geaflow.common.encoder.impl.TupleEncoder;
import org.apache.geaflow.common.tuple.Triple;
import org.apache.geaflow.common.tuple.Tuple;

public class Encoders {

    public static final IEncoder<Boolean> BOOLEAN = BooleanEncoder.INSTANCE;
    public static final IEncoder<Byte> BYTE = ByteEncoder.INSTANCE;
    public static final IEncoder<Short> SHORT = ShortEncoder.INSTANCE;
    public static final IEncoder<Integer> INTEGER = IntegerEncoder.INSTANCE;
    public static final IEncoder<Long> LONG = LongEncoder.INSTANCE;
    public static final IEncoder<Float> FLOAT = FloatEncoder.INSTANCE;
    public static final IEncoder<Double> DOUBLE = DoubleEncoder.INSTANCE;
    public static final IEncoder<Character> CHARACTER = CharacterEncoder.INSTANCE;
    public static final IEncoder<String> STRING = StringEncoder.INSTANCE;

    public static final IEncoder<boolean[]> BOOLEAN_ARR = BooleanArrEncoder.INSTANCE;
    public static final IEncoder<byte[]> BYTE_ARR = ByteArrEncoder.INSTANCE;
    public static final IEncoder<short[]> SHORT_ARR = ShortArrEncoder.INSTANCE;
    public static final IEncoder<int[]> INTEGER_ARR = IntegerArrEncoder.INSTANCE;
    public static final IEncoder<long[]> LONG_ARR = LongArrEncoder.INSTANCE;
    public static final IEncoder<float[]> FLOAT_ARR = FloatArrEncoder.INSTANCE;
    public static final IEncoder<double[]> DOUBLE_ARR = DoubleArrEncoder.INSTANCE;
    public static final IEncoder<char[]> CHARACTER_ARR = CharacterArrEncoder.INSTANCE;

    public static final Map<Class<?>, IEncoder<?>> PRIMITIVE_ENCODER_MAP = new HashMap<>();

    static {
        PRIMITIVE_ENCODER_MAP.put(boolean.class, BOOLEAN);
        PRIMITIVE_ENCODER_MAP.put(Boolean.class, BOOLEAN);
        PRIMITIVE_ENCODER_MAP.put(byte.class, BYTE);
        PRIMITIVE_ENCODER_MAP.put(Byte.class, BYTE);
        PRIMITIVE_ENCODER_MAP.put(short.class, SHORT);
        PRIMITIVE_ENCODER_MAP.put(Short.class, SHORT);
        PRIMITIVE_ENCODER_MAP.put(int.class, INTEGER);
        PRIMITIVE_ENCODER_MAP.put(Integer.class, INTEGER);
        PRIMITIVE_ENCODER_MAP.put(long.class, LONG);
        PRIMITIVE_ENCODER_MAP.put(Long.class, LONG);
        PRIMITIVE_ENCODER_MAP.put(float.class, FLOAT);
        PRIMITIVE_ENCODER_MAP.put(Float.class, FLOAT);
        PRIMITIVE_ENCODER_MAP.put(double.class, DOUBLE);
        PRIMITIVE_ENCODER_MAP.put(Double.class, DOUBLE);
        PRIMITIVE_ENCODER_MAP.put(char.class, CHARACTER);
        PRIMITIVE_ENCODER_MAP.put(Character.class, CHARACTER);
        PRIMITIVE_ENCODER_MAP.put(String.class, STRING);
    }

    public static final Map<Class<?>, IEncoder<?>> PRIMITIVE_ARR_ENCODER_MAP = new HashMap<>();

    static {
        PRIMITIVE_ARR_ENCODER_MAP.put(boolean[].class, BOOLEAN_ARR);
        PRIMITIVE_ARR_ENCODER_MAP.put(Boolean[].class, new GenericArrayEncoder<>(BOOLEAN, Boolean[]::new));
        PRIMITIVE_ARR_ENCODER_MAP.put(byte[].class, BYTE_ARR);
        PRIMITIVE_ARR_ENCODER_MAP.put(Byte[].class, new GenericArrayEncoder<>(BYTE, Byte[]::new));
        PRIMITIVE_ARR_ENCODER_MAP.put(short[].class, SHORT_ARR);
        PRIMITIVE_ARR_ENCODER_MAP.put(Short[].class, new GenericArrayEncoder<>(SHORT, Short[]::new));
        PRIMITIVE_ARR_ENCODER_MAP.put(int[].class, INTEGER_ARR);
        PRIMITIVE_ARR_ENCODER_MAP.put(Integer[].class, new GenericArrayEncoder<>(INTEGER, Integer[]::new));
        PRIMITIVE_ARR_ENCODER_MAP.put(long[].class, LONG_ARR);
        PRIMITIVE_ARR_ENCODER_MAP.put(Long[].class, new GenericArrayEncoder<>(LONG, Long[]::new));
        PRIMITIVE_ARR_ENCODER_MAP.put(float[].class, FLOAT_ARR);
        PRIMITIVE_ARR_ENCODER_MAP.put(Float[].class, new GenericArrayEncoder<>(FLOAT, Float[]::new));
        PRIMITIVE_ARR_ENCODER_MAP.put(double[].class, DOUBLE_ARR);
        PRIMITIVE_ARR_ENCODER_MAP.put(Double[].class, new GenericArrayEncoder<>(DOUBLE, Double[]::new));
        PRIMITIVE_ARR_ENCODER_MAP.put(char[].class, CHARACTER_ARR);
        PRIMITIVE_ARR_ENCODER_MAP.put(Character[].class, new GenericArrayEncoder<>(CHARACTER, Character[]::new));
        PRIMITIVE_ARR_ENCODER_MAP.put(String[].class, new GenericArrayEncoder<>(STRING, String[]::new));
    }

    public static final Map<Class<?>, Class<?>> PRIMITIVE_WRAPPER_MAP = new HashMap<>();

    static {
        PRIMITIVE_WRAPPER_MAP.put(Boolean.TYPE, Boolean.class);
        PRIMITIVE_WRAPPER_MAP.put(Byte.TYPE, Byte.class);
        PRIMITIVE_WRAPPER_MAP.put(Character.TYPE, Character.class);
        PRIMITIVE_WRAPPER_MAP.put(Short.TYPE, Short.class);
        PRIMITIVE_WRAPPER_MAP.put(Integer.TYPE, Integer.class);
        PRIMITIVE_WRAPPER_MAP.put(Long.TYPE, Long.class);
        PRIMITIVE_WRAPPER_MAP.put(Double.TYPE, Double.class);
        PRIMITIVE_WRAPPER_MAP.put(Float.TYPE, Float.class);
        PRIMITIVE_WRAPPER_MAP.put(Void.TYPE, Void.TYPE);
    }

    public static <T0, T1> IEncoder<Tuple<T0, T1>> tuple(IEncoder<T0> encoder0, IEncoder<T1> encoder1) {
        return new TupleEncoder<>(encoder0, encoder1);
    }

    public static <T0, T1, T2> IEncoder<Triple<T0, T1, T2>> triple(
        IEncoder<T0> encoder0, IEncoder<T1> encoder1, IEncoder<T2> encoder2) {
        return new TripleEncoder<>(encoder0, encoder1, encoder2);
    }

}
