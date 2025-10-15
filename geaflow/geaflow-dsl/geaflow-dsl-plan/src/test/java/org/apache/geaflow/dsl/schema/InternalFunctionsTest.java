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

package org.apache.geaflow.dsl.schema;

import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.abs;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.acos;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.asin;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.atan;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.ceil;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.cos;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.cot;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.degrees;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.divide;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.equal;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.exp;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.floor;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.greaterThan;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.greaterThanEq;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.lessThan;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.lessThanEq;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.ln;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.log10;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.minus;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.minusPrefix;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.mod;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.plus;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.power;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.radians;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.rand;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.randInt;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.round;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.sign;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.sin;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.tan;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.times;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.timestampCeil;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.timestampFloor;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.timestampTumble;
import static org.apache.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions.unequal;
import static org.testng.AssertJUnit.assertEquals;

import com.google.common.collect.Lists;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import org.apache.geaflow.dsl.common.function.UDTF;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.schema.function.GeaFlowUserDefinedTableFunction;
import org.testng.Assert;
import org.testng.annotations.Test;

public class InternalFunctionsTest {
    Byte byte1 = Byte.valueOf("1");
    Short short1 = Short.valueOf("1");
    Integer int1 = Integer.valueOf("1");
    Long long1 = Long.valueOf("1");
    Double double1 = Double.valueOf("1.0");
    BigDecimal decimal1 = BigDecimal.valueOf(1);
    Byte byteNull = null;
    Short shortNull = null;
    Integer intNull = null;
    Long longNull = null;
    Double doubleNull = null;
    BigDecimal decimalNull = null;

    @Test
    public void testPlus() {
        assertEquals((long) plus(long1, long1), 2);
        assertEquals((int) plus(int1, int1), 2);
        assertEquals((int) plus(short1, short1), 2);
        assertEquals((int) plus(byte1, byte1), 2);
        assertEquals(plus(double1, double1), 2.0);
        assertEquals(plus(decimal1, decimal1), BigDecimal.valueOf(2));
        assertEquals((long) plus(long1, int1), 2);
        assertEquals((long) plus(long1, short1), 2);
        assertEquals((long) plus(long1, byte1), 2);
        assertEquals(plus(long1, double1), 2.0);
        assertEquals(plus(long1, decimal1), BigDecimal.valueOf(2));
        assertEquals((long) plus(int1, long1), 2);
        assertEquals((int) plus(int1, short1), 2);
        assertEquals((int) plus(int1, byte1), 2);
        assertEquals(plus(int1, double1), 2.0);
        assertEquals(plus(int1, decimal1), BigDecimal.valueOf(2));
        assertEquals((long) plus(short1, long1), 2);
        assertEquals((int) plus(short1, int1), 2);
        assertEquals((int) plus(short1, byte1), 2);
        assertEquals(plus(short1, double1), 2.0);
        assertEquals(plus(short1, decimal1), BigDecimal.valueOf(2));
        assertEquals((long) plus(byte1, long1), 2);
        assertEquals((int) plus(byte1, int1), 2);
        assertEquals((int) plus(byte1, short1), 2);
        assertEquals(plus(byte1, double1), 2.0);
        assertEquals(plus(byte1, decimal1), BigDecimal.valueOf(2));
        assertEquals(plus(double1, long1), 2.0);
        assertEquals(plus(double1, int1), 2.0);
        assertEquals(plus(double1, short1), 2.0);
        assertEquals(plus(double1, byte1), 2.0);
        assertEquals(plus(double1, decimal1), BigDecimal.valueOf(2));
        assertEquals(plus(decimal1, long1), BigDecimal.valueOf(2));
        assertEquals(plus(decimal1, int1), BigDecimal.valueOf(2));
        assertEquals(plus(decimal1, short1), BigDecimal.valueOf(2));
        assertEquals(plus(decimal1, byte1), BigDecimal.valueOf(2));
        assertEquals(plus(decimal1, double1), BigDecimal.valueOf(2));

        Assert.assertNull(plus(longNull, long1));
        Assert.assertNull(plus(intNull, int1));
        Assert.assertNull(plus(shortNull, short1));
        Assert.assertNull(plus(byteNull, byte1));
        Assert.assertNull(plus(doubleNull, double1));
        Assert.assertNull(plus(decimalNull, decimal1));
        Assert.assertNull(plus(longNull, int1));
        Assert.assertNull(plus(longNull, short1));
        Assert.assertNull(plus(longNull, byte1));
        Assert.assertNull(plus(longNull, double1));
        Assert.assertNull(plus(longNull, decimal1));
        Assert.assertNull(plus(intNull, long1));
        Assert.assertNull(plus(intNull, short1));
        Assert.assertNull(plus(intNull, byte1));
        Assert.assertNull(plus(intNull, double1));
        Assert.assertNull(plus(intNull, decimal1));
        Assert.assertNull(plus(shortNull, long1));
        Assert.assertNull(plus(shortNull, int1));
        Assert.assertNull(plus(shortNull, byte1));
        Assert.assertNull(plus(shortNull, double1));
        Assert.assertNull(plus(shortNull, decimal1));
        Assert.assertNull(plus(byteNull, long1));
        Assert.assertNull(plus(byteNull, int1));
        Assert.assertNull(plus(byteNull, short1));
        Assert.assertNull(plus(byteNull, double1));
        Assert.assertNull(plus(byteNull, decimal1));
        Assert.assertNull(plus(doubleNull, long1));
        Assert.assertNull(plus(doubleNull, int1));
        Assert.assertNull(plus(doubleNull, short1));
        Assert.assertNull(plus(doubleNull, byte1));
        Assert.assertNull(plus(doubleNull, decimal1));
        Assert.assertNull(plus(decimalNull, long1));
        Assert.assertNull(plus(decimalNull, int1));
        Assert.assertNull(plus(decimalNull, short1));
        Assert.assertNull(plus(decimalNull, byte1));
        Assert.assertNull(plus(decimalNull, double1));
    }

    @Test
    public void testMinus() {
        assertEquals((long) minus(long1, long1), 0);
        assertEquals((int) minus(int1, int1), 0);
        assertEquals((int) minus(short1, short1), 0);
        assertEquals((int) minus(byte1, byte1), 0);
        assertEquals(minus(double1, double1), 0.0);
        assertEquals(minus(decimal1, decimal1), BigDecimal.valueOf(0));
        assertEquals((long) minus(long1, int1), 0);
        assertEquals((long) minus(long1, short1), 0);
        assertEquals((long) minus(long1, byte1), 0);
        assertEquals(minus(long1, double1), 0.0);
        assertEquals(minus(long1, decimal1), BigDecimal.valueOf(0));
        assertEquals((long) minus(int1, long1), 0);
        assertEquals((int) minus(int1, short1), 0);
        assertEquals((int) minus(int1, byte1), 0);
        assertEquals(minus(int1, double1), 0.0);
        assertEquals(minus(int1, decimal1), BigDecimal.valueOf(0));
        assertEquals((long) minus(short1, long1), 0);
        assertEquals((int) minus(short1, int1), 0);
        assertEquals((int) minus(short1, byte1), 0);
        assertEquals(minus(short1, double1), 0.0);
        assertEquals(minus(short1, decimal1), BigDecimal.valueOf(0));
        assertEquals((long) minus(byte1, long1), 0);
        assertEquals((int) minus(byte1, int1), 0);
        assertEquals((int) minus(byte1, short1), 0);
        assertEquals(minus(byte1, double1), 0.0);
        assertEquals(minus(byte1, decimal1), BigDecimal.valueOf(0));
        assertEquals(minus(double1, long1), 0.0);
        assertEquals(minus(double1, int1), 0.0);
        assertEquals(minus(double1, short1), 0.0);
        assertEquals(minus(double1, byte1), 0.0);
        assertEquals(minus(double1, decimal1), BigDecimal.valueOf(0));
        assertEquals(minus(decimal1, long1), BigDecimal.valueOf(0));
        assertEquals(minus(decimal1, int1), BigDecimal.valueOf(0));
        assertEquals(minus(decimal1, short1), BigDecimal.valueOf(0));
        assertEquals(minus(decimal1, byte1), BigDecimal.valueOf(0));
        assertEquals(minus(decimal1, double1), BigDecimal.valueOf(0));

        Assert.assertNull(minus(longNull, long1));
        Assert.assertNull(minus(intNull, int1));
        Assert.assertNull(minus(shortNull, short1));
        Assert.assertNull(minus(byteNull, byte1));
        Assert.assertNull(minus(doubleNull, double1));
        Assert.assertNull(minus(decimalNull, decimal1));
        Assert.assertNull(minus(longNull, int1));
        Assert.assertNull(minus(longNull, short1));
        Assert.assertNull(minus(longNull, byte1));
        Assert.assertNull(minus(longNull, double1));
        Assert.assertNull(minus(longNull, decimal1));
        Assert.assertNull(minus(intNull, long1));
        Assert.assertNull(minus(intNull, short1));
        Assert.assertNull(minus(intNull, byte1));
        Assert.assertNull(minus(intNull, double1));
        Assert.assertNull(minus(intNull, decimal1));
        Assert.assertNull(minus(shortNull, long1));
        Assert.assertNull(minus(shortNull, int1));
        Assert.assertNull(minus(shortNull, byte1));
        Assert.assertNull(minus(shortNull, double1));
        Assert.assertNull(minus(shortNull, decimal1));
        Assert.assertNull(minus(byteNull, long1));
        Assert.assertNull(minus(byteNull, int1));
        Assert.assertNull(minus(byteNull, short1));
        Assert.assertNull(minus(byteNull, double1));
        Assert.assertNull(minus(byteNull, decimal1));
        Assert.assertNull(minus(doubleNull, long1));
        Assert.assertNull(minus(doubleNull, int1));
        Assert.assertNull(minus(doubleNull, short1));
        Assert.assertNull(minus(doubleNull, byte1));
        Assert.assertNull(minus(doubleNull, decimal1));
        Assert.assertNull(minus(decimalNull, long1));
        Assert.assertNull(minus(decimalNull, int1));
        Assert.assertNull(minus(decimalNull, short1));
        Assert.assertNull(minus(decimalNull, byte1));
        Assert.assertNull(minus(decimalNull, double1));
    }

    @Test
    public void testTimes() {
        assertEquals((long) times(long1, long1), 1);
        assertEquals((long) times(int1, int1), 1);
        assertEquals((int) times(short1, short1), 1);
        assertEquals((int) times(byte1, byte1), 1);
        assertEquals(times(double1, double1), 1.0);
        assertEquals(times(decimal1, decimal1), BigDecimal.valueOf(1));
        assertEquals((long) times(long1, int1), 1);
        assertEquals((long) times(long1, short1), 1);
        assertEquals((long) times(long1, byte1), 1);
        assertEquals(times(long1, double1), 1.0);
        assertEquals(times(long1, decimal1), BigDecimal.valueOf(1));
        assertEquals((long) times(int1, long1), 1);
        assertEquals((int) times(int1, short1), 1);
        assertEquals((int) times(int1, byte1), 1);
        assertEquals(times(int1, double1), 1.0);
        assertEquals(times(int1, decimal1), BigDecimal.valueOf(1));
        assertEquals((long) times(short1, long1), 1);
        assertEquals((int) times(short1, int1), 1);
        assertEquals((int) times(short1, byte1), 1);
        assertEquals(times(short1, double1), 1.0);
        assertEquals(times(short1, decimal1), BigDecimal.valueOf(1));
        assertEquals((long) times(byte1, long1), 1);
        assertEquals((int) times(byte1, int1), 1);
        assertEquals((int) times(byte1, short1), 1);
        assertEquals(times(byte1, double1), 1.0);
        assertEquals(times(byte1, decimal1), BigDecimal.valueOf(1));
        assertEquals(times(double1, long1), 1.0);
        assertEquals(times(double1, int1), 1.0);
        assertEquals(times(double1, short1), 1.0);
        assertEquals(times(double1, byte1), 1.0);
        assertEquals(times(double1, decimal1), BigDecimal.valueOf(1));
        assertEquals(times(decimal1, long1), BigDecimal.valueOf(1));
        assertEquals(times(decimal1, int1), BigDecimal.valueOf(1));
        assertEquals(times(decimal1, short1), BigDecimal.valueOf(1));
        assertEquals(times(decimal1, byte1), BigDecimal.valueOf(1));
        assertEquals(times(decimal1, double1), BigDecimal.valueOf(1));

        Assert.assertNull(times(longNull, long1));
        Assert.assertNull(times(intNull, int1));
        Assert.assertNull(times(shortNull, short1));
        Assert.assertNull(times(byteNull, byte1));
        Assert.assertNull(times(doubleNull, double1));
        Assert.assertNull(times(decimalNull, decimal1));
        Assert.assertNull(times(longNull, int1));
        Assert.assertNull(times(longNull, short1));
        Assert.assertNull(times(longNull, byte1));
        Assert.assertNull(times(longNull, double1));
        Assert.assertNull(times(longNull, decimal1));
        Assert.assertNull(times(intNull, long1));
        Assert.assertNull(times(intNull, short1));
        Assert.assertNull(times(intNull, byte1));
        Assert.assertNull(times(intNull, double1));
        Assert.assertNull(times(intNull, decimal1));
        Assert.assertNull(times(shortNull, long1));
        Assert.assertNull(times(shortNull, int1));
        Assert.assertNull(times(shortNull, byte1));
        Assert.assertNull(times(shortNull, double1));
        Assert.assertNull(times(shortNull, decimal1));
        Assert.assertNull(times(byteNull, long1));
        Assert.assertNull(times(byteNull, int1));
        Assert.assertNull(times(byteNull, short1));
        Assert.assertNull(times(byteNull, double1));
        Assert.assertNull(times(byteNull, decimal1));
        Assert.assertNull(times(doubleNull, long1));
        Assert.assertNull(times(doubleNull, int1));
        Assert.assertNull(times(doubleNull, short1));
        Assert.assertNull(times(doubleNull, byte1));
        Assert.assertNull(times(doubleNull, decimal1));
        Assert.assertNull(times(decimalNull, long1));
        Assert.assertNull(times(decimalNull, int1));
        Assert.assertNull(times(decimalNull, short1));
        Assert.assertNull(times(decimalNull, byte1));
        Assert.assertNull(times(decimalNull, double1));
    }

    @Test
    public void testDivide() {
        assertEquals((long) divide(long1, long1), 1);
        assertEquals((int) divide(int1, int1), 1);
        assertEquals((int) divide(short1, short1), 1);
        assertEquals((int) divide(byte1, byte1), 1);
        assertEquals(divide(double1, double1), 1.0);
        assertEquals(divide(decimal1, decimal1), BigDecimal.valueOf(1));
        assertEquals((long) divide(long1, int1), 1);
        assertEquals((long) divide(long1, short1), 1);
        assertEquals((long) divide(long1, byte1), 1);
        assertEquals(divide(long1, double1), 1.0);
        assertEquals(divide(long1, decimal1), BigDecimal.valueOf(1));
        assertEquals((long) divide(int1, long1), 1);
        assertEquals((int) divide(int1, short1), 1);
        assertEquals((int) divide(int1, byte1), 1);
        assertEquals(divide(int1, double1), 1.0);
        assertEquals(divide(int1, decimal1), BigDecimal.valueOf(1));
        assertEquals((long) divide(short1, long1), 1);
        assertEquals((int) divide(short1, int1), 1);
        assertEquals((int) divide(short1, byte1), 1);
        assertEquals(divide(short1, double1), 1.0);
        assertEquals(divide(short1, decimal1), BigDecimal.valueOf(1));
        assertEquals((long) divide(byte1, long1), 1);
        assertEquals((int) divide(byte1, int1), 1);
        assertEquals((int) divide(byte1, short1), 1);
        assertEquals(divide(byte1, double1), 1.0);
        assertEquals(divide(byte1, decimal1), BigDecimal.valueOf(1));
        assertEquals(divide(double1, long1), 1.0);
        assertEquals(divide(double1, int1), 1.0);
        assertEquals(divide(double1, short1), 1.0);
        assertEquals(divide(double1, byte1), 1.0);
        assertEquals(divide(double1, decimal1), BigDecimal.valueOf(1));
        assertEquals(divide(decimal1, long1), BigDecimal.valueOf(1));
        assertEquals(divide(decimal1, int1), BigDecimal.valueOf(1));
        assertEquals(divide(decimal1, short1), BigDecimal.valueOf(1));
        assertEquals(divide(decimal1, byte1), BigDecimal.valueOf(1));
        assertEquals(divide(decimal1, double1), BigDecimal.valueOf(1));

        Assert.assertNull(divide(longNull, long1));
        Assert.assertNull(divide(intNull, int1));
        Assert.assertNull(divide(shortNull, short1));
        Assert.assertNull(divide(byteNull, byte1));
        Assert.assertNull(divide(doubleNull, double1));
        Assert.assertNull(divide(decimalNull, decimal1));
        Assert.assertNull(divide(longNull, int1));
        Assert.assertNull(divide(longNull, short1));
        Assert.assertNull(divide(longNull, byte1));
        Assert.assertNull(divide(longNull, double1));
        Assert.assertNull(divide(longNull, decimal1));
        Assert.assertNull(divide(intNull, long1));
        Assert.assertNull(divide(intNull, short1));
        Assert.assertNull(divide(intNull, byte1));
        Assert.assertNull(divide(intNull, double1));
        Assert.assertNull(divide(intNull, decimal1));
        Assert.assertNull(divide(shortNull, long1));
        Assert.assertNull(divide(shortNull, int1));
        Assert.assertNull(divide(shortNull, byte1));
        Assert.assertNull(divide(shortNull, double1));
        Assert.assertNull(divide(shortNull, decimal1));
        Assert.assertNull(divide(byteNull, long1));
        Assert.assertNull(divide(byteNull, int1));
        Assert.assertNull(divide(byteNull, short1));
        Assert.assertNull(divide(byteNull, double1));
        Assert.assertNull(divide(byteNull, decimal1));
        Assert.assertNull(divide(doubleNull, long1));
        Assert.assertNull(divide(doubleNull, int1));
        Assert.assertNull(divide(doubleNull, short1));
        Assert.assertNull(divide(doubleNull, byte1));
        Assert.assertNull(divide(doubleNull, decimal1));
        Assert.assertNull(divide(decimalNull, long1));
        Assert.assertNull(divide(decimalNull, int1));
        Assert.assertNull(divide(decimalNull, short1));
        Assert.assertNull(divide(decimalNull, byte1));
        Assert.assertNull(divide(decimalNull, double1));
    }

    @Test
    public void testMod() {
        assertEquals((long) mod(long1, long1), 0);
        assertEquals((int) mod(int1, int1), 0);
        assertEquals(mod(double1, double1), 0.0);

        Assert.assertNull(mod(longNull, long1));
        Assert.assertNull(mod(intNull, int1));
        Assert.assertNull(mod(doubleNull, double1));
    }

    @Test
    public void testPower() {
        assertEquals(power(double1, double1), 1.0);

        Assert.assertNull(power(doubleNull, double1));
    }

    @Test
    public void testAbs() {
        assertEquals((long) abs(long1), 1);
        assertEquals((int) abs(int1), 1);
        assertEquals((int) abs(short1), 1);
        assertEquals((int) abs(byte1), 1);
        assertEquals(abs(decimal1), BigDecimal.valueOf(1));
        assertEquals(abs(double1), 1.0);

        Assert.assertNull(abs(longNull));
        Assert.assertNull(abs(intNull));
        Assert.assertNull(abs(shortNull));
        Assert.assertNull(abs(byteNull));
        Assert.assertNull(abs(decimalNull));
        Assert.assertNull(abs(doubleNull));
    }

    @Test
    public void testTrigonometric() {
        assertEquals(asin(double1), 1.5707963267948966);
        assertEquals(acos(double1), 0.0);
        assertEquals(atan(double1), 0.7853981633974483);
        assertEquals(ceil(double1), 1.0);
        assertEquals(ceil(long1), long1);
        assertEquals(ceil(int1), int1);
        assertEquals(cot(double1), 0.6420926159343306);
        assertEquals(cos(double1), 0.5403023058681398);
        assertEquals(degrees(double1), 57.29577951308232);
        assertEquals(radians(double1), 0.017453292519943295);
        assertEquals(sign(double1), 1.0);
        assertEquals(sin(double1), 0.8414709848078965);
        assertEquals(tan(double1), 1.5574077246549023);

        Assert.assertNull(asin(doubleNull));
        Assert.assertNull(acos(doubleNull));
        Assert.assertNull(atan(doubleNull));
        Assert.assertNull(ceil(doubleNull));
        Assert.assertNull(ceil(longNull));
        Assert.assertNull(ceil(intNull));
        Assert.assertNull(cot(doubleNull));
        Assert.assertNull(cos(doubleNull));
        Assert.assertNull(degrees(doubleNull));
        Assert.assertNull(radians(doubleNull));
        Assert.assertNull(sign(doubleNull));
        Assert.assertNull(sin(doubleNull));
        Assert.assertNull(tan(doubleNull));
    }

    @Test
    public void testMath() {
        assertEquals(exp(double1), 2.718281828459045, 1e-15);
        assertEquals(floor(double1), 1.0);
        assertEquals((long) floor(long1), 1);
        assertEquals((int) floor(int1), 1);
        assertEquals(ln(double1), 0.0);
        assertEquals(log10(double1), 0.0);
        assertEquals(minusPrefix(double1), -1.0);
        assertEquals((long) minusPrefix(long1), -1);
        assertEquals((int) minusPrefix(int1), -1);
        assertEquals((int) minusPrefix(short1), -1);
        assertEquals((int) minusPrefix(byte1), -1);
        assertEquals(minusPrefix(decimal1), BigDecimal.valueOf(-1));

        System.out.println(rand());
        System.out.println(rand(long1));
        System.out.println(rand(long1, int1));
        System.out.println(randInt(int1));
        System.out.println(randInt(long1, int1));

        Assert.assertNull(exp(doubleNull));
        Assert.assertNull(floor(doubleNull));
        Assert.assertNull(floor(longNull));
        Assert.assertNull(floor(intNull));
        Assert.assertNull(ln(doubleNull));
        Assert.assertNull(log10(doubleNull));
        Assert.assertNull(minusPrefix(doubleNull));
        Assert.assertNull(minusPrefix(longNull));
        Assert.assertNull(minusPrefix(intNull));
        Assert.assertNull(minusPrefix(shortNull));
        Assert.assertNull(minusPrefix(byteNull));
        Assert.assertNull(minusPrefix(decimalNull));
    }

    @Test
    public void testEqual() {
        String string1 = "1";
        String stringNull = null;
        Boolean boolTrue = true;
        Boolean boolNull = null;

        Assert.assertTrue(equal(long1, long1));
        Assert.assertTrue(equal(double1, double1));
        Assert.assertTrue(equal(decimal1, decimal1));
        Assert.assertTrue(equal(string1, string1));
        Assert.assertTrue(equal(boolTrue, boolTrue));
        Assert.assertTrue(equal(string1, int1));
        Assert.assertTrue(equal(int1, string1));
        Assert.assertTrue(equal(string1, double1));
        Assert.assertTrue(equal(double1, string1));
        Assert.assertTrue(equal(string1, long1));
        Assert.assertTrue(equal(long1, string1));
        Assert.assertTrue(equal(string1, false));
        Assert.assertTrue(equal(false, string1));
        Assert.assertTrue(equal((Object) boolTrue, boolTrue));

        Assert.assertNull(equal(longNull, long1));
        Assert.assertNull(equal(doubleNull, double1));
        Assert.assertNull(equal(decimalNull, decimal1));
        Assert.assertNull(equal(stringNull, string1));
        Assert.assertNull(equal(boolNull, boolTrue));
        Assert.assertNull(equal(stringNull, int1));
        Assert.assertNull(equal(intNull, string1));
        Assert.assertNull(equal(stringNull, double1));
        Assert.assertNull(equal(doubleNull, string1));
        Assert.assertNull(equal(stringNull, long1));
        Assert.assertNull(equal(longNull, string1));
        Assert.assertNull(equal(stringNull, boolTrue));
        Assert.assertNull(equal(boolNull, string1));
        Assert.assertNull(equal(stringNull, boolTrue));
        Assert.assertNull(equal((Object) boolNull, boolTrue));
    }

    @Test
    public void testUnequal() {
        String string1 = "1";
        String stringNull = null;
        Boolean boolTrue = true;
        Boolean boolNull = null;

        Assert.assertFalse(unequal(long1, long1));
        Assert.assertFalse(unequal(double1, double1));
        Assert.assertFalse(unequal(decimal1, decimal1));
        Assert.assertFalse(unequal(string1, string1));
        Assert.assertFalse(unequal(boolTrue, boolTrue));
        Assert.assertFalse(unequal(string1, int1));
        Assert.assertFalse(unequal(int1, string1));
        Assert.assertFalse(unequal(string1, double1));
        Assert.assertFalse(unequal(double1, string1));
        Assert.assertFalse(unequal(string1, long1));
        Assert.assertFalse(unequal(long1, string1));
        Assert.assertFalse(unequal(string1, false));
        Assert.assertFalse(unequal(false, string1));
        Assert.assertFalse(unequal((Object) boolTrue, boolTrue));

        Assert.assertNull(unequal(longNull, long1));
        Assert.assertNull(unequal(doubleNull, double1));
        Assert.assertNull(unequal(decimalNull, decimal1));
        Assert.assertNull(unequal(stringNull, string1));
        Assert.assertNull(unequal(boolNull, boolTrue));
        Assert.assertNull(unequal(stringNull, int1));
        Assert.assertNull(unequal(intNull, string1));
        Assert.assertNull(unequal(stringNull, double1));
        Assert.assertNull(unequal(doubleNull, string1));
        Assert.assertNull(unequal(stringNull, long1));
        Assert.assertNull(unequal(longNull, string1));
        Assert.assertNull(unequal(stringNull, boolTrue));
        Assert.assertNull(unequal(boolNull, string1));
        Assert.assertNull(unequal(stringNull, boolTrue));
        Assert.assertFalse(unequal((Object) boolTrue, boolTrue));
    }

    @Test
    public void testCompare() {
        String string1 = "1";
        String stringNull = null;

        Assert.assertFalse(lessThan(long1, long1));
        Assert.assertFalse(lessThan(double1, double1));
        Assert.assertFalse(lessThan(decimal1, decimal1));
        Assert.assertFalse(lessThan(string1, string1));

        Assert.assertTrue(greaterThanEq(long1, long1));
        Assert.assertTrue(greaterThanEq(double1, double1));
        Assert.assertTrue(greaterThanEq(decimal1, decimal1));
        Assert.assertTrue(greaterThanEq(string1, string1));

        Assert.assertTrue(lessThanEq(long1, long1));
        Assert.assertTrue(lessThanEq(double1, double1));
        Assert.assertTrue(lessThanEq(decimal1, decimal1));
        Assert.assertTrue(lessThanEq(string1, string1));

        Assert.assertFalse(greaterThan(long1, long1));
        Assert.assertFalse(greaterThan(double1, double1));
        Assert.assertFalse(greaterThan(decimal1, decimal1));
        Assert.assertFalse(greaterThan(string1, string1));

        Assert.assertNull(lessThan(longNull, long1));
        Assert.assertNull(lessThan(doubleNull, double1));
        Assert.assertNull(lessThan(decimalNull, decimal1));
        Assert.assertNull(lessThan(stringNull, string1));

        Assert.assertNull(greaterThanEq(longNull, long1));
        Assert.assertNull(greaterThanEq(doubleNull, double1));
        Assert.assertNull(greaterThanEq(decimalNull, decimal1));
        Assert.assertNull(greaterThanEq(stringNull, string1));

        Assert.assertNull(lessThanEq(longNull, long1));
        Assert.assertNull(lessThanEq(doubleNull, double1));
        Assert.assertNull(lessThanEq(decimalNull, decimal1));
        Assert.assertNull(lessThanEq(stringNull, string1));

        Assert.assertNull(greaterThan(longNull, long1));
        Assert.assertNull(greaterThan(doubleNull, double1));
        Assert.assertNull(greaterThan(decimalNull, decimal1));
        Assert.assertNull(greaterThan(stringNull, string1));
    }

    @Test
    public void testTimestampUtil() {
        Timestamp ts = Timestamp.valueOf("1987-06-05 04:03:02");
        Assert.assertEquals(timestampCeil(ts, 1000L), Timestamp.valueOf("1987-06-05 04:03:03"));
        Assert.assertEquals(timestampCeil(ts, 60000L), Timestamp.valueOf("1987-06-05 04:04:00.0"));
        Assert.assertEquals(timestampCeil(ts, 3600000L), Timestamp.valueOf("1987-06-05 05:00:00.0"));
        Assert.assertEquals(timestampCeil(ts, 86400000L), Timestamp.valueOf("1987-06-06 00:00:00.0"));
        Assert.assertEquals(timestampTumble(ts, 1000L), Timestamp.valueOf("1987-06-05 04:03:03"));
        Assert.assertEquals(timestampTumble(ts, 60000L), Timestamp.valueOf("1987-06-05 04:04:00.0"));
        Assert.assertEquals(timestampTumble(ts, 3600000L), Timestamp.valueOf("1987-06-05 05:00:00.0"));
        Assert.assertEquals(timestampFloor(ts, 1000L), Timestamp.valueOf("1987-06-05 04:03:02.0"));
        Assert.assertEquals(timestampFloor(ts, 60000L), Timestamp.valueOf("1987-06-05 04:03:00.0"));
        Assert.assertEquals(timestampFloor(ts, 3600000L), Timestamp.valueOf("1987-06-05 04:00:00.0"));
        Assert.assertEquals(timestampFloor(ts, 86400000L), Timestamp.valueOf("1987-06-05 00:00:00.0"));

        Assert.assertEquals(plus(ts, 1L), Timestamp.valueOf("1987-06-05 04:03:02.001"));
        Assert.assertEquals(minus(ts, 1L), Timestamp.valueOf("1987-06-05 04:03:01.999"));
        Assert.assertEquals((long) minus(ts, Timestamp.valueOf("1987-06-05 00:00:00.0")), 14582000L);

        Assert.assertNull(plus((Timestamp) null, 1L));
        Assert.assertNull(minus((Timestamp) null, 1L));
        Assert.assertNull(minus((Timestamp) null, Timestamp.valueOf("1987-06-05 00:00:00.0")));

    }

    @Test
    public void testOtherFunction() {
        Assert.assertEquals(round(double1, 2), 1.0);

        Assert.assertNull(round(doubleNull, 2));
    }

    @Test
    public void testGeaFlowUserDefinedTableFunction() {
        GQLJavaTypeFactory typeFactory = GQLJavaTypeFactory.create();
        GeaFlowUserDefinedTableFunction.create("testFunction",
            UtUserDefinedTableFunction.class,
            typeFactory
        );
    }

    public static class UtUserDefinedTableFunction extends UDTF {

        public UtUserDefinedTableFunction() {
        }

        @Override
        public List<Class<?>> getReturnType(List<Class<?>> paramTypes, List<String> outFieldNames) {
            return Lists.newArrayList(String.class, Long.class);
        }
    }
}
