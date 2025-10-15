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

package org.apache.geaflow.dsl.schema.function;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.geaflow.common.binary.BinaryString;

public final class GeaFlowBuiltinFunctions {

    public static Long plus(Long a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Integer plus(Integer a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Integer plus(Short a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Integer plus(Byte a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Double plus(Double a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static BigDecimal plus(BigDecimal a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return a.add(b);
    }

    public static Long plus(Long a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Long plus(Long a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Long plus(Long a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Double plus(Long a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static BigDecimal plus(Long a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).add(b);
    }

    public static Long plus(Integer a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Integer plus(Integer a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Integer plus(Integer a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Double plus(Integer a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static BigDecimal plus(Integer a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).add(b);
    }

    public static Long plus(Short a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Integer plus(Short a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Integer plus(Short a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Double plus(Short a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static BigDecimal plus(Short a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).add(b);
    }

    public static Long plus(Byte a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Integer plus(Byte a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Integer plus(Byte a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Double plus(Byte a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static BigDecimal plus(Byte a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).add(b);
    }

    public static Double plus(Double a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Double plus(Double a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Double plus(Double a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Double plus(Double a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static BigDecimal plus(Double a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).add(b);
    }

    public static BigDecimal plus(BigDecimal a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a.add(new BigDecimal(b));
    }

    public static BigDecimal plus(BigDecimal a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a.add(new BigDecimal(b));
    }

    public static BigDecimal plus(BigDecimal a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a.add(new BigDecimal(b));
    }

    public static BigDecimal plus(BigDecimal a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a.add(new BigDecimal(b));
    }

    public static BigDecimal plus(BigDecimal a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a.add(new BigDecimal(b));
    }

    public static Long minus(Long a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static Integer minus(Integer a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static Integer minus(Short a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static Integer minus(Byte a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static Double minus(Double a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static BigDecimal minus(BigDecimal a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return a.subtract(b);
    }

    public static Long minus(Long a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static Long minus(Long a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static Long minus(Long a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static Double minus(Long a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static BigDecimal minus(Long a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).subtract(b);
    }

    public static Long minus(Integer a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static Integer minus(Integer a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static Integer minus(Integer a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static Double minus(Integer a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static BigDecimal minus(Integer a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).subtract(b);
    }

    public static Long minus(Short a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static Integer minus(Short a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static Integer minus(Short a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static Double minus(Short a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static BigDecimal minus(Short a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).subtract(b);
    }

    public static Long minus(Byte a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static Integer minus(Byte a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static Integer minus(Byte a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static Double minus(Byte a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static BigDecimal minus(Byte a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).subtract(b);
    }

    public static Double minus(Double a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static Double minus(Double a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static Double minus(Double a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static Double minus(Double a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a - b;
    }

    public static BigDecimal minus(Double a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).subtract(b);
    }

    public static BigDecimal minus(BigDecimal a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a.subtract(new BigDecimal(b));
    }

    public static BigDecimal minus(BigDecimal a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a.subtract(new BigDecimal(b));
    }

    public static BigDecimal minus(BigDecimal a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a.subtract(new BigDecimal(b));
    }

    public static BigDecimal minus(BigDecimal a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a.subtract(new BigDecimal(b));
    }

    public static BigDecimal minus(BigDecimal a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a.subtract(new BigDecimal(b));
    }

    public static Long times(Long a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static Long times(Integer a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return (long) a * (long) b;
    }

    public static Integer times(Short a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static Integer times(Byte a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static Double times(Double a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static BigDecimal times(BigDecimal a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return a.multiply(b);
    }

    public static Long times(Long a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static Long times(Long a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static Long times(Long a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static Double times(Long a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static BigDecimal times(Long a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).multiply(b);
    }

    public static Long times(Integer a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static Integer times(Integer a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static Integer times(Integer a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static Double times(Integer a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static BigDecimal times(Integer a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).multiply(b);
    }

    public static Long times(Short a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static Integer times(Short a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static Integer times(Short a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static Double times(Short a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static BigDecimal times(Short a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).multiply(b);
    }

    public static Long times(Byte a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static Integer times(Byte a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static Integer times(Byte a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static Double times(Byte a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static BigDecimal times(Byte a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).multiply(b);
    }

    public static Double times(Double a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static Double times(Double a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static Double times(Double a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static Double times(Double a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a * b;
    }

    public static BigDecimal times(Double a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).multiply(b);
    }

    public static BigDecimal times(BigDecimal a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a.multiply(new BigDecimal(b));
    }

    public static BigDecimal times(BigDecimal a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a.multiply(new BigDecimal(b));
    }

    public static BigDecimal times(BigDecimal a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a.multiply(new BigDecimal(b));
    }

    public static BigDecimal times(BigDecimal a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a.multiply(new BigDecimal(b));
    }

    public static BigDecimal times(BigDecimal a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a.multiply(new BigDecimal(b));
    }

    public static Long divide(Long a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static Integer divide(Integer a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static Integer divide(Short a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static Integer divide(Byte a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static Double divide(Double a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static BigDecimal divide(BigDecimal a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return a.divide(b);
    }

    public static Long divide(Long a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static Long divide(Long a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static Long divide(Long a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static Double divide(Long a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static BigDecimal divide(Long a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).divide(b);
    }

    public static Long divide(Integer a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static Integer divide(Integer a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static Integer divide(Integer a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static Double divide(Integer a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static BigDecimal divide(Integer a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).divide(b);
    }

    public static Long divide(Short a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static Integer divide(Short a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static Integer divide(Short a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static Double divide(Short a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static BigDecimal divide(Short a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).divide(b);
    }

    public static Long divide(Byte a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static Integer divide(Byte a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static Integer divide(Byte a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static Double divide(Byte a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static BigDecimal divide(Byte a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).divide(b);
    }

    public static Double divide(Double a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static Double divide(Double a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static Double divide(Double a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static Double divide(Double a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a / b;
    }

    public static BigDecimal divide(Double a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return new BigDecimal(a).divide(b);
    }

    public static BigDecimal divide(BigDecimal a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a.divide(new BigDecimal(b));
    }

    public static BigDecimal divide(BigDecimal a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a.divide(new BigDecimal(b));
    }

    public static BigDecimal divide(BigDecimal a, Short b) {
        if (a == null || b == null) {
            return null;
        }
        return a.divide(new BigDecimal(b));
    }

    public static BigDecimal divide(BigDecimal a, Byte b) {
        if (a == null || b == null) {
            return null;
        }
        return a.divide(new BigDecimal(b));
    }

    public static BigDecimal divide(BigDecimal a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a.divide(new BigDecimal(b));
    }

    public static Long mod(Long a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        if (b == 0) {
            return null;
        }
        return a % b;
    }

    public static Double mod(Double a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a % b;
    }

    public static Integer mod(Integer a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        if (b == 0) {
            return null;
        }
        return a % b;
    }

    public static Double power(Double a, Double b) {
        if (a == null || b == null) {
            return null;
        }

        return Math.pow(a, b);
    }

    public static Long abs(Long a) {
        if (a == null) {
            return null;
        }
        return Math.abs(a);
    }

    public static Double abs(Double a) {
        if (a == null) {
            return null;
        }
        return Math.abs(a);
    }

    public static Integer abs(Integer a) {
        if (a == null) {
            return null;
        }
        return Math.abs(a);
    }

    public static Integer abs(Short a) {
        if (a == null) {
            return null;
        }
        return Math.abs(a);
    }

    public static Integer abs(Byte a) {
        if (a == null) {
            return null;
        }
        return Math.abs(a);
    }

    public static BigDecimal abs(BigDecimal a) {
        if (a == null) {
            return null;
        }
        return a.abs();
    }

    public static Double asin(Double a) {
        if (a == null) {
            return null;
        }
        return Math.asin(a);
    }

    public static Double acos(Double a) {
        if (a == null) {
            return null;
        }
        return Math.acos(a);
    }

    public static Double atan(Double a) {
        if (a == null) {
            return null;
        }
        return Math.atan(a);
    }

    public static Double ceil(Double a) {
        if (a == null) {
            return null;
        }
        return Math.ceil(a);
    }

    public static Long ceil(Long a) {
        if (a == null) {
            return null;
        }
        return a;
    }

    public static Integer ceil(Integer a) {
        return a;
    }

    public static Double cot(Double a) {
        if (a == null) {
            return null;
        }
        return 1.0d / Math.tan(a);
    }

    public static Double cos(Double a) {
        if (a == null) {
            return null;
        }
        return Math.cos(a);
    }

    public static Double degrees(Double a) {
        if (a == null) {
            return null;
        }
        return Math.toDegrees(a);
    }

    public static Double radians(Double a) {
        if (a == null) {
            return null;
        }
        return Math.toRadians(a);
    }

    public static Double exp(Double a) {
        if (a == null) {
            return null;
        }
        return Math.exp(a);
    }

    public static Double floor(Double a) {
        if (a == null) {
            return null;
        }
        return Math.floor(a);
    }

    public static Long floor(Long a) {
        return a;
    }

    public static Integer floor(Integer a) {
        return a;
    }

    public static Double ln(Double a) {
        if (a == null) {
            return null;
        }
        return Math.log(a);
    }

    public static Double log10(Double a) {
        if (a == null) {
            return null;
        }
        return Math.log10(a);
    }

    public static Long minusPrefix(Long a) {
        if (a == null) {
            return null;
        }
        return -a;
    }

    public static Double minusPrefix(Double a) {
        if (a == null) {
            return null;
        }
        return -a;
    }

    public static Integer minusPrefix(Integer a) {
        if (a == null) {
            return null;
        }
        return -a;
    }

    public static Integer minusPrefix(Short a) {
        if (a == null) {
            return null;
        }
        return -a;
    }

    public static Integer minusPrefix(Byte a) {
        if (a == null) {
            return null;
        }
        return -a;
    }

    public static BigDecimal minusPrefix(BigDecimal a) {
        if (a == null) {
            return null;
        }
        return a.negate();
    }

    public static Double rand() {
        return rand(null);
    }

    public static Double rand(Long seed) {
        Random random = seed == null ? new Random() : new Random(seed);
        return random.nextDouble();
    }

    public static int rand(Long seed, Integer bound) {
        Random random = seed == null ? new Random() : new Random(seed);
        return random.nextInt(bound);
    }

    public static Integer randInt(Integer bound) {
        Random random = new Random();
        return random.nextInt(bound);
    }

    public static Integer randInt(Long seed, Integer bound) {
        return rand(seed, bound);
    }

    public static Double sign(Double a) {
        if (a == null) {
            return null;
        }
        return Math.signum(a);
    }

    public static Double sin(Double a) {
        if (a == null) {
            return null;
        }
        return Math.sin(a);
    }

    public static Double tan(Double a) {
        if (a == null) {
            return null;
        }
        return Math.tan(a);
    }

    public static Double round(Double a, Integer n) {
        if (a == null || n == null) {
            return null;
        }

        if (Double.isNaN(a) || Double.isInfinite(a)) {
            return a;
        } else {
            return BigDecimal.valueOf(a).setScale(n, RoundingMode.HALF_UP)
                .doubleValue();
        }
    }

    public static Boolean equal(Long a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a.longValue() == b.longValue();
    }

    public static Boolean equal(Double a, Double b) {
        if (a == null || b == null) {
            return null;
        }

        return a.doubleValue() == b.doubleValue();
    }

    public static Boolean equal(BigDecimal a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }

        return a.compareTo(b) == 0;
    }

    public static Boolean equal(String a, String b) {
        if (a == null || b == null) {
            return null;
        }
        return a.equals(b);
    }

    public static Boolean equal(BinaryString a, BinaryString b) {
        if (a == null || b == null) {
            return null;
        }
        return a.equals(b);
    }

    public static Boolean equal(Boolean a, Boolean b) {
        if (a == null || b == null) {
            return null;
        }
        return a.equals(b);
    }

    public static Boolean equal(String a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        int dot = a.indexOf('.');
        if (dot >= 0) {
            for (int i = dot + 1; i < a.length(); i++) {
                if (a.charAt(i) != '0') {
                    return false;
                }
            }
        }
        return Integer.valueOf(a).equals(b);
    }

    public static Boolean equal(Integer a, String b) {
        return equal(b, a);
    }

    public static Boolean equal(String a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return Double.valueOf(a).equals(b);
    }

    public static Boolean equal(Double a, String b) {
        return equal(b, a);
    }

    public static Boolean equal(String a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        int dot = a.indexOf('.');
        if (dot >= 0) {
            for (int i = dot + 1; i < a.length(); i++) {
                if (a.charAt(i) != '0') {
                    return false;
                }
            }
            a = a.substring(0, dot);
        }
        return Long.valueOf(a).equals(b);
    }

    public static Boolean equal(Long a, String b) {
        return equal(b, a);
    }

    public static Boolean equal(String a, Boolean b) {
        if (a == null || b == null) {
            return null;
        }
        return Boolean.valueOf(a).equals(b);
    }

    public static Boolean equal(Boolean a, String b) {
        return equal(b, a);
    }

    public static Boolean equal(Object a, Object b) {
        if (a == null || b == null) {
            return null;
        }
        return a.equals(b);
    }

    public static Boolean unequal(Long a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a.longValue() != b.longValue();
    }

    public static Boolean unequal(Double a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a.doubleValue() != b.doubleValue();
    }

    public static Boolean unequal(BigDecimal a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return a.compareTo(b) != 0;
    }

    public static Boolean unequal(String a, String b) {
        if (a == null || b == null) {
            return null;
        }
        return !a.equals(b);
    }

    public static Boolean unequal(Boolean a, Boolean b) {
        if (a == null || b == null) {
            return null;
        }
        return !a.equals(b);
    }

    public static Boolean unequal(String a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        int dot = a.indexOf('.');
        if (dot >= 0) {
            for (int i = dot + 1; i < a.length(); i++) {
                if (a.charAt(i) != '0') {
                    return true;
                }
            }
        }
        return !Integer.valueOf(a).equals(b);
    }

    public static Boolean unequal(Integer a, String b) {
        Boolean equals = equal(b, a);
        return equals != null ? !equals : null;
    }

    public static Boolean unequal(String a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return !Double.valueOf(a).equals(b);
    }

    public static Boolean unequal(Double a, String b) {
        if (a == null || b == null) {
            return null;
        }
        Boolean equals = equal(b, a);
        return equals != null ? !equals : null;
    }

    public static Boolean unequal(String a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        int dot = a.indexOf('.');
        if (dot >= 0) {
            for (int i = dot + 1; i < a.length(); i++) {
                if (a.charAt(i) != '0') {
                    return true;
                }
            }
            a = a.substring(0, dot);
        }
        return !Long.valueOf(a).equals(b);
    }

    public static Boolean unequal(Long a, String b) {
        if (a == null || b == null) {
            return null;
        }
        Boolean equals = equal(b, a);
        return equals != null ? !equals : null;
    }

    public static Boolean unequal(String a, Boolean b) {
        if (a == null || b == null) {
            return null;
        }
        return !Boolean.valueOf(a).equals(b);
    }

    public static Boolean unequal(Boolean a, String b) {
        if (a == null || b == null) {
            return null;
        }
        Boolean equals = equal(b, a);
        return equals != null ? !equals : null;
    }

    public static Boolean unequal(Object a, Object b) {
        if (a == null || b == null) {
            return null;
        }
        Boolean equals = equal(a, b);
        return equals != null ? !equals : null;
    }

    public static Boolean lessThan(Long a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a < b;
    }

    public static Boolean lessThan(Double a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a < b;
    }

    public static Boolean lessThan(BigDecimal a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return a.compareTo(b) < 0;
    }

    public static Boolean lessThan(String a, String b) {
        if (a == null || b == null) {
            return null;
        }
        return a.compareTo(b) < 0;
    }

    public static Boolean greaterThanEq(Long a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a >= b;
    }

    public static Boolean greaterThanEq(Double a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a >= b;
    }

    public static Boolean greaterThanEq(BigDecimal a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return a.compareTo(b) >= 0;
    }

    public static Boolean greaterThanEq(String a, String b) {
        if (a == null || b == null) {
            return null;
        }
        return a.compareTo(b) >= 0;
    }

    public static Boolean lessThanEq(Long a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a <= b;
    }

    public static Boolean lessThanEq(Double a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a <= b;
    }

    public static Boolean lessThanEq(BigDecimal a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return a.compareTo(b) <= 0;
    }

    public static Boolean lessThanEq(String a, String b) {
        if (a == null || b == null) {
            return null;
        }
        return a.compareTo(b) <= 0;
    }

    public static Boolean greaterThan(Long a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        return a > b;
    }

    public static Boolean greaterThan(Double a, Double b) {
        if (a == null || b == null) {
            return null;
        }
        return a > b;
    }

    public static Boolean greaterThan(BigDecimal a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return a.compareTo(b) > 0;
    }

    public static Boolean greaterThan(String a, String b) {
        if (a == null || b == null) {
            return null;
        }
        return a.compareTo(b) > 0;
    }

    public static Timestamp timestampCeil(Timestamp b0, Long b1) {
        if (b1 == 1000) { // second
            return new Timestamp(DateUtils.ceiling(b0, Calendar.SECOND).getTime());
        } else if (b1 == 60000) { // minute
            return new Timestamp(DateUtils.ceiling(b0, Calendar.MINUTE).getTime());
        } else if (b1 == 3600000) { // hour
            return new Timestamp(DateUtils.ceiling(b0, Calendar.HOUR).getTime());
        } else if (b1 == 86400000) { // day
            return new Timestamp(DateUtils.ceiling(b0, Calendar.DATE).getTime());
        } else {
            throw new RuntimeException();
        }
    }

    public static Timestamp timestampTumble(Timestamp b0, Long b1) {
        if (b1 % 86400000 == 0) {
            long base = DateUtils.truncate(b0, Calendar.DAY_OF_YEAR).getTime();
            long day = b0.getDay() * 86400000;
            long interval = ((day / b1) + 1) * b1 - day;
            return new Timestamp(base + interval);
        } else if (b1 % 3600000 == 0) {
            long base = DateUtils.truncate(b0, Calendar.HOUR).getTime();
            long hour = b0.getHours() * 3600000;
            long interval = ((hour / b1) + 1) * b1 - hour;
            return new Timestamp(base + interval);
        } else if (b1 % 60000 == 0) {
            long base = DateUtils.truncate(b0, Calendar.MINUTE).getTime();
            long minutes = b0.getMinutes() * 60000;
            long interval = ((minutes / b1) + 1) * b1 - minutes;
            return new Timestamp(base + interval);
        } else if (b1 % 1000 == 0) {
            long base = DateUtils.truncate(b0, Calendar.SECOND).getTime();
            long second = b0.getSeconds() * 1000;
            long interval = ((second / b1) + 1) * b1 - second;
            return new Timestamp(base + interval);
        } else {
            throw new RuntimeException();
        }
    }

    public static Timestamp timestampFloor(Timestamp b0, Long b1) {
        if (b1 == 1000) { // second
            return new Timestamp(DateUtils.truncate(b0, Calendar.SECOND).getTime());
        } else if (b1 == 60000) { // minute
            return new Timestamp(DateUtils.truncate(b0, Calendar.MINUTE).getTime());
        } else if (b1 == 3600000) { // hour
            return new Timestamp(DateUtils.truncate(b0, Calendar.HOUR).getTime());
        } else if (b1 == 86400000) { // day
            return new Timestamp(DateUtils.truncate(b0, Calendar.DATE).getTime());
        } else {
            throw new RuntimeException();
        }
    }

    public static Timestamp plus(Timestamp d, Long b0) {
        if (d == null || b0 == null) {
            return null;
        }
        return new Timestamp(d.getTime() + b0);
    }

    public static Timestamp minus(Timestamp d, Long b0) {
        if (d == null || b0 == null) {
            return null;
        }
        return new Timestamp(d.getTime() - b0);
    }

    public static Long minus(Timestamp d1, Timestamp d2) {
        if (d1 == null || d2 == null) {
            return null;
        }
        return d1.getTime() - d2.getTime();
    }

    public static Timestamp currentTimestamp() {
        return new Timestamp(System.currentTimeMillis());
    }

    public static final int TRIM_BOTH = 0;
    public static final int TRIM_LEFT = 1;
    public static final int TRIM_RIGHT = 2;

    public static String concat(String a, String b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }

    public static Integer length(String s) {
        if (s == null) {
            return null;
        }
        return s.length();
    }

    public static String lower(String s) {
        if (s == null) {
            return null;
        }
        return s.toLowerCase();
    }

    public static String upper(String s) {
        if (s == null) {
            return null;
        }
        return s.toUpperCase();
    }

    public static String overlay(String s, String r, int start) {
        if (s == null || r == null) {
            return null;
        }
        return s.substring(0, start - 1)
            + r
            + s.substring(start - 1 + r.length());
    }

    public static String overlay(String s, String r, int start, int length) {
        if (s == null || r == null) {
            return null;
        }
        return s.substring(0, start - 1)
            + r
            + s.substring(start - 1 + length);
    }

    public static Integer position(String seek, String s) {
        if (seek == null || s == null) {
            return null;
        }
        return s.indexOf(seek) + 1;
    }

    public static Integer position(String seek, String s, Integer from) {
        if (seek == null
            || s == null || from == null) {
            return null;
        }
        final int from0 = from - 1;
        if (from0 > s.length() || from0 < 0) {
            return 0;
        }
        return s.indexOf(seek, from0) + 1;
    }

    public static String substring(String c, Integer s, Integer l) {
        if (c == null
            || s == null || l == null) {
            return null;
        }
        int lc = c.length();
        if (s < 0) {
            s += lc + 1;
        }
        int e = s + l;
        if (e < s) {
            return null;
        }
        if (s > lc || e < 1) {
            return "";
        }
        int s1 = Math.max(s, 1);
        int e1 = Math.min(e, lc + 1);
        return c.substring(s1 - 1, e1 - 1);
    }

    public static String substring(String c, Integer s) {
        if (c == null || s == null) {
            return null;
        }
        return substring(c, s, c.length() + 1);
    }

    public static String trim(Integer flag,
                              String removeStr, String str) {
        if (flag == null || removeStr == null || str == null) {
            return null;
        }
        switch (flag) {
            case TRIM_BOTH:
                while (str.startsWith(removeStr)) {
                    str = str.substring(removeStr.length());
                }
                while (str.endsWith(removeStr)) {
                    str = str.substring(0, str.length() - removeStr.length());
                }
                return str;
            case TRIM_LEFT:
                while (str.startsWith(removeStr)) {
                    str = str.substring(removeStr.length());
                }
                return str;
            case TRIM_RIGHT:
                while (str.endsWith(removeStr)) {
                    str = str.substring(0, str.length() - removeStr.length());
                }
                return str;
            default:
                throw new UnsupportedOperationException("Not support trim flag: " + flag);
        }
    }

    public static BinaryString trim(Integer flag,
                                    BinaryString removeStr, BinaryString str) {
        if (flag == null || removeStr == null || str == null) {
            return null;
        }
        switch (flag) {
            case TRIM_BOTH:
                while (str.startsWith(removeStr)) {
                    str = str.substring(removeStr.getLength());
                }
                while (str.endsWith(removeStr)) {
                    str = str.substring(0, str.getLength() - removeStr.getLength());
                }
                return str;
            case TRIM_LEFT:
                while (str.startsWith(removeStr)) {
                    str = str.substring(removeStr.getLength());
                }
                return str;
            case TRIM_RIGHT:
                while (str.endsWith(removeStr)) {
                    str = str.substring(0, str.getLength() - removeStr.getLength());
                }
                return str;
            default:
                throw new UnsupportedOperationException("Not support trim flag: " + flag);
        }
    }
}
