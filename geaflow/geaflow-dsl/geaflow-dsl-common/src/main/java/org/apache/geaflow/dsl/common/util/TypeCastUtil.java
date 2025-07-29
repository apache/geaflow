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

package org.apache.geaflow.dsl.common.util;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.common.type.IType;

public class TypeCastUtil {

    @SuppressWarnings("unchecked")
    private static final Map<Tuple<Class, Class>, ITypeCast> typeCasts =
        (Map) ImmutableMap.builder()
            .put(Tuple.of(Integer.class, Long.class), new Int2Long())
            .put(Tuple.of(Integer.class, Double.class), new Int2Double())
            .put(Tuple.of(Integer.class, BigDecimal.class), new Int2Decimal())
            .put(Tuple.of(Integer.class, String.class), new Int2String())
            .put(Tuple.of(Integer.class, BinaryString.class), new Int2BinaryString())
            .put(Tuple.of(Integer.class, Timestamp.class), new Int2Timestamp())
            .put(Tuple.of(Long.class, Double.class), new Long2Double())
            .put(Tuple.of(Long.class, Integer.class), new Long2Int())
            .put(Tuple.of(Long.class, BigDecimal.class), new Long2Decimal())
            .put(Tuple.of(Long.class, String.class), new Long2String())
            .put(Tuple.of(Long.class, BinaryString.class), new Long2BinaryString())
            .put(Tuple.of(Long.class, Timestamp.class), new Long2Timestamp())
            .put(Tuple.of(String.class, Long.class), new String2Long())
            .put(Tuple.of(String.class, Integer.class), new String2Int())
            .put(Tuple.of(String.class, Double.class), new String2Double())
            .put(Tuple.of(String.class, BigDecimal.class), new String2Decimal())
            .put(Tuple.of(String.class, Boolean.class), new String2Boolean())
            .put(Tuple.of(String.class, BinaryString.class), new String2Binary())
            .put(Tuple.of(String.class, Timestamp.class), new String2Timestamp())
            .put(Tuple.of(String.class, Date.class), new String2Date())
            .put(Tuple.of(BinaryString.class, Long.class), new BinaryString2Long())
            .put(Tuple.of(BinaryString.class, Integer.class), new BinaryString2Int())
            .put(Tuple.of(BinaryString.class, Double.class), new BinaryString2Double())
            .put(Tuple.of(BinaryString.class, BigDecimal.class), new BinaryString2Decimal())
            .put(Tuple.of(BinaryString.class, Boolean.class), new BinaryString2Boolean())
            .put(Tuple.of(BinaryString.class, String.class), new BinaryString2String())
            .put(Tuple.of(BinaryString.class, Timestamp.class), new BinaryString2Timestamp())
            .put(Tuple.of(BinaryString.class, Date.class), new BinaryString2Date())
            .put(Tuple.of(Double.class, Long.class), new Double2Long())
            .put(Tuple.of(Double.class, Integer.class), new Double2Int())
            .put(Tuple.of(Double.class, BigDecimal.class), new Double2Decimal())
            .put(Tuple.of(Double.class, String.class), new Double2String())
            .put(Tuple.of(Double.class, BinaryString.class), new Double2BinaryString())
            .put(Tuple.of(Boolean.class, String.class), new Boolean2String())
            .put(Tuple.of(Boolean.class, BinaryString.class), new Boolean2BinaryString())
            .put(Tuple.of(BigDecimal.class, Double.class), new Decimal2Double())
            .put(Tuple.of(BigDecimal.class, Integer.class), new Decimal2Int())
            .put(Tuple.of(BigDecimal.class, Long.class), new Decimal2Long())
            .put(Tuple.of(BigDecimal.class, String.class), new Decimal2String())
            .put(Tuple.of(BigDecimal.class, BinaryString.class), new Decimal2BinaryString())
            .put(Tuple.of(byte[].class, BinaryString.class), new Bytes2BinaryString())
            .build();

    private static final ITypeCast identityCast = new IdentityCast();

    public static <S, T> ITypeCast<S, T> getTypeCast(IType<?> sourceType, IType<?> targetType) {
        return getTypeCast(sourceType.getTypeClass(), targetType.getTypeClass());
    }

    @SuppressWarnings("unchecked")
    public static <S, T> ITypeCast<S, T> getTypeCast(Class<?> sourceType, Class<?> targetType) {
        sourceType = FunctionCallUtils.getBoxType(sourceType);
        targetType = FunctionCallUtils.getBoxType(targetType);
        if (sourceType == targetType) {
            return identityCast;
        }
        if (sourceType.isArray() && targetType.isArray()) {
            ITypeCast componentTypeCast = getTypeCast(sourceType.getComponentType(), targetType.getComponentType());
            return (ITypeCast<S, T>) new ArrayCast(componentTypeCast, targetType.getComponentType());
        }
        ITypeCast<S, T> typeCast = typeCasts.get(Tuple.of(sourceType, targetType));
        if (typeCast == null) {
            throw new IllegalArgumentException("Cannot cast from: " + sourceType + " to " + targetType);
        }
        return typeCast;
    }

    public static Object cast(Object o, IType<?> type) {
        return cast(o, type.getTypeClass());
    }

    public static Object cast(Object o, Class<?> targetType) {
        if (o == null) {
            return null;
        }
        if (targetType.isAssignableFrom(o.getClass())) {
            return o;
        }
        if (targetType == Object.class) {
            return o;
        }
        return getTypeCast(o.getClass(), targetType).castTo(o);
    }

    public static boolean isInteger(String s) {
        if (s == null) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isInteger(BinaryString s) {
        if (s == null) {
            return false;
        }
        for (int i = 0; i < s.getLength(); i++) {
            if (!Character.isDigit(s.getByte(i))) {
                return false;
            }
        }
        return true;
    }

    public interface ITypeCast<S, T> {
        T castTo(S s);
    }

    private static class IdentityCast<S> implements ITypeCast<S, S> {

        @Override
        public S castTo(S s) {
            return s;
        }
    }

    private static class ArrayCast implements ITypeCast<Object, Object> {

        private final ITypeCast<Object, Object> componentTypeCast;

        private final Class targetType;

        public ArrayCast(
            ITypeCast<Object, Object> componentTypeCast, Class<?> targetType) {
            this.componentTypeCast = componentTypeCast;
            this.targetType = targetType;
        }

        @Override
        public Object castTo(Object objects) {
            int length = Array.getLength(objects);
            Object castArray = Array.newInstance(targetType, length);
            for (int i = 0; i < length; i++) {
                Object castObject = componentTypeCast.castTo(Array.get(objects, i));
                Array.set(castArray, i, castObject);
            }
            return castArray;
        }
    }

    private static class Int2Long implements ITypeCast<Integer, Long> {

        @Override
        public Long castTo(Integer o) {
            if (o == null) {
                return null;
            }
            return o.longValue();
        }
    }

    private static class Int2Double implements ITypeCast<Integer, Double> {

        @Override
        public Double castTo(Integer o) {
            if (o == null) {
                return null;
            }
            return o.doubleValue();
        }
    }

    private static class Int2Decimal implements ITypeCast<Integer, BigDecimal> {

        @Override
        public BigDecimal castTo(Integer o) {
            if (o == null) {
                return null;
            }
            return new BigDecimal(o);
        }
    }

    private static class Int2String implements ITypeCast<Integer, String> {

        @Override
        public String castTo(Integer o) {
            if (o == null) {
                return null;
            }
            return String.valueOf(o);
        }
    }

    private static class Int2BinaryString implements ITypeCast<Integer, BinaryString> {

        @Override
        public BinaryString castTo(Integer o) {
            if (o == null) {
                return null;
            }
            return BinaryString.fromString(String.valueOf(o));
        }
    }

    private static class Int2Timestamp implements ITypeCast<Integer, Timestamp> {

        @Override
        public Timestamp castTo(Integer o) {
            if (o == null) {
                return null;
            }
            return new Timestamp(o);
        }
    }

    private static class Long2Double implements ITypeCast<Long, Double> {

        @Override
        public Double castTo(Long o) {
            if (o == null) {
                return null;
            }
            return o.doubleValue();
        }
    }

    private static class Long2Int implements ITypeCast<Long, Integer> {

        @Override
        public Integer castTo(Long o) {
            if (o == null) {
                return null;
            }
            return o.intValue();
        }
    }

    private static class Long2Decimal implements ITypeCast<Long, BigDecimal> {

        @Override
        public BigDecimal castTo(Long o) {
            if (o == null) {
                return null;
            }
            return new BigDecimal(o);
        }
    }

    private static class Long2String implements ITypeCast<Long, String> {

        @Override
        public String castTo(Long o) {
            if (o == null) {
                return null;
            }
            return String.valueOf(o);
        }
    }

    private static class Long2BinaryString implements ITypeCast<Long, BinaryString> {

        @Override
        public BinaryString castTo(Long o) {
            if (o == null) {
                return null;
            }
            return BinaryString.fromString(String.valueOf(o));
        }
    }

    private static class Long2Timestamp implements ITypeCast<Long, Timestamp> {

        @Override
        public Timestamp castTo(Long o) {
            if (o == null) {
                return null;
            }
            return new Timestamp(o);
        }
    }

    private static class String2Long implements ITypeCast<String, Long> {

        @Override
        public Long castTo(String o) {
            if (o == null) {
                return null;
            }
            return Long.parseLong(o);
        }
    }

    private static class String2Int implements ITypeCast<String, Integer> {

        @Override
        public Integer castTo(String o) {
            if (o == null) {
                return null;
            }
            return Integer.parseInt(o);
        }
    }

    private static class String2Double implements ITypeCast<String, Double> {

        @Override
        public Double castTo(String o) {
            if (o == null) {
                return null;
            }
            return Double.valueOf(o);
        }
    }

    private static class String2Decimal implements ITypeCast<String, BigDecimal> {

        @Override
        public BigDecimal castTo(String o) {
            if (o == null) {
                return null;
            }
            return new BigDecimal(o);
        }
    }

    private static class String2Boolean implements ITypeCast<String, Boolean> {

        @Override
        public Boolean castTo(String o) {
            if (o == null) {
                return null;
            }
            return Boolean.valueOf(o);
        }
    }

    private static class String2Binary implements ITypeCast<String, BinaryString> {

        @Override
        public BinaryString castTo(String o) {
            if (o == null) {
                return null;
            }
            return BinaryString.fromString(o);
        }
    }

    private static class String2Timestamp implements ITypeCast<String, Timestamp> {

        @Override
        public Timestamp castTo(String o) {
            if (o == null) {
                return null;
            }
            if (isInteger(o)) {
                return new Timestamp(Long.parseLong(o));
            }
            return Timestamp.valueOf(o);
        }
    }

    private static class String2Date implements ITypeCast<String, Date> {

        @Override
        public Date castTo(String o) {
            if (o == null) {
                return null;
            }
            return Date.valueOf(o);
        }
    }

    private static class BinaryString2Long implements ITypeCast<BinaryString, Long> {

        @Override
        public Long castTo(BinaryString o) {
            if (o == null) {
                return null;
            }
            return Long.parseLong(o.toString());
        }
    }

    private static class BinaryString2Int implements ITypeCast<BinaryString, Integer> {

        @Override
        public Integer castTo(BinaryString o) {
            if (o == null) {
                return null;
            }
            return Integer.parseInt(o.toString());
        }
    }

    private static class BinaryString2Double implements ITypeCast<BinaryString, Double> {

        @Override
        public Double castTo(BinaryString o) {
            if (o == null) {
                return null;
            }
            return Double.valueOf(o.toString());
        }
    }

    private static class BinaryString2Decimal implements ITypeCast<BinaryString, BigDecimal> {

        @Override
        public BigDecimal castTo(BinaryString o) {
            if (o == null) {
                return null;
            }
            return new BigDecimal(o.toString());
        }
    }

    private static class BinaryString2Boolean implements ITypeCast<BinaryString, Boolean> {

        @Override
        public Boolean castTo(BinaryString o) {
            if (o == null) {
                return null;
            }
            return Boolean.valueOf(o.toString());
        }
    }

    private static class BinaryString2String implements ITypeCast<BinaryString, String> {

        @Override
        public String castTo(BinaryString o) {
            if (o == null) {
                return null;
            }
            return o.toString();
        }
    }

    private static class BinaryString2Timestamp implements ITypeCast<BinaryString, Timestamp> {

        @Override
        public Timestamp castTo(BinaryString o) {
            if (o == null) {
                return null;
            }
            if (isInteger(o)) {
                return new Timestamp(Long.parseLong(o.toString()));
            }
            return Timestamp.valueOf(o.toString());
        }
    }

    private static class BinaryString2Date implements ITypeCast<BinaryString, Date> {

        @Override
        public Date castTo(BinaryString o) {
            if (o == null) {
                return null;
            }
            return Date.valueOf(o.toString());
        }
    }

    private static class Double2Long implements ITypeCast<Double, Long> {

        @Override
        public Long castTo(Double o) {
            if (o == null) {
                return null;
            }
            return o.longValue();
        }
    }

    private static class Double2Int implements ITypeCast<Double, Integer> {

        @Override
        public Integer castTo(Double o) {
            if (o == null) {
                return null;
            }
            return o.intValue();
        }
    }

    private static class Double2Decimal implements ITypeCast<Double, BigDecimal> {

        @Override
        public BigDecimal castTo(Double o) {
            if (o == null) {
                return null;
            }
            return new BigDecimal(o);
        }
    }

    private static class Double2String implements ITypeCast<Double, String> {

        @Override
        public String castTo(Double o) {
            if (o == null) {
                return null;
            }
            return o.toString();
        }
    }

    private static class Double2BinaryString implements ITypeCast<Double, BinaryString> {

        @Override
        public BinaryString castTo(Double o) {
            if (o == null) {
                return null;
            }
            return BinaryString.fromString(o.toString());
        }
    }

    private static class Boolean2String implements ITypeCast<Boolean, String> {

        @Override
        public String castTo(Boolean o) {
            if (o == null) {
                return null;
            }
            return o.toString();
        }
    }

    private static class Boolean2BinaryString implements ITypeCast<Boolean, BinaryString> {

        @Override
        public BinaryString castTo(Boolean o) {
            if (o == null) {
                return null;
            }
            return BinaryString.fromString(o.toString());
        }
    }

    private static class Decimal2Double implements ITypeCast<BigDecimal, Double> {

        @Override
        public Double castTo(BigDecimal o) {
            if (o == null) {
                return null;
            }
            return o.doubleValue();
        }
    }

    private static class Decimal2Long implements ITypeCast<BigDecimal, Long> {

        @Override
        public Long castTo(BigDecimal o) {
            if (o == null) {
                return null;
            }
            return o.longValue();
        }
    }

    private static class Decimal2Int implements ITypeCast<BigDecimal, Integer> {

        @Override
        public Integer castTo(BigDecimal o) {
            if (o == null) {
                return null;
            }
            return o.intValue();
        }
    }

    private static class Decimal2String implements ITypeCast<BigDecimal, String> {

        @Override
        public String castTo(BigDecimal o) {
            if (o == null) {
                return null;
            }
            return o.toString();
        }
    }

    private static class Decimal2BinaryString implements ITypeCast<BigDecimal, BinaryString> {

        @Override
        public BinaryString castTo(BigDecimal o) {
            if (o == null) {
                return null;
            }
            return BinaryString.fromString(o.toString());
        }
    }

    private static class Bytes2BinaryString implements ITypeCast<byte[], BinaryString> {
        @Override
        public BinaryString castTo(byte[] o) {
            if (o == null) {
                return null;
            }
            return BinaryString.fromString(new String(o, StandardCharsets.UTF_8));
        }
    }
}
