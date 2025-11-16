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

package org.apache.geaflow.dsl.runtime.expression;

import java.math.BigDecimal;
import java.util.Date;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.common.type.primitive.BinaryStringType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.runtime.expression.logic.IsNotTypedExpression;
import org.apache.geaflow.dsl.runtime.expression.logic.IsTypedExpression;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Comprehensive tests for ISO-GQL Value Type Predicate (IS TYPED / IS NOT TYPED).
 * Tests cover basic types, complex types, NULL handling, and performance under different data volumes.
 */
@Test
public class ValueTypePredicateTest {

    // ==================== Basic Type Tests ====================

    @Test
    public void testIsTypedInteger() {
        Expression input = new LiteralExpression(42, Types.INTEGER);
        IsTypedExpression expr = new IsTypedExpression(input, Types.INTEGER);
        ObjectRow row = ObjectRow.create();
        
        Assert.assertEquals(expr.evaluate(row), true);
    }

    @Test
    public void testIsTypedIntegerWithLongValue() {
        Expression input = new LiteralExpression(42L, Types.LONG);
        IsTypedExpression expr = new IsTypedExpression(input, Types.INTEGER);
        ObjectRow row = ObjectRow.create();
        
        // LONG is not INTEGER, should return false
        Assert.assertEquals(expr.evaluate(row), false);
    }

    @Test
    public void testIsTypedString() {
        Expression input = new LiteralExpression("hello", Types.STRING);
        IsTypedExpression expr = new IsTypedExpression(input, Types.STRING);
        ObjectRow row = ObjectRow.create();
        
        Assert.assertEquals(expr.evaluate(row), true);
    }

    @Test
    public void testIsTypedDouble() {
        Expression input = new LiteralExpression(3.14, Types.DOUBLE);
        IsTypedExpression expr = new IsTypedExpression(input, Types.DOUBLE);
        ObjectRow row = ObjectRow.create();
        
        Assert.assertEquals(expr.evaluate(row), true);
    }

    @Test
    public void testIsTypedBoolean() {
        Expression input = new LiteralExpression(true, Types.BOOLEAN);
        IsTypedExpression expr = new IsTypedExpression(input, Types.BOOLEAN);
        ObjectRow row = ObjectRow.create();
        
        Assert.assertEquals(expr.evaluate(row), true);
    }

    // ==================== NULL Handling Tests ====================

    @Test
    public void testIsTypedNull() {
        Expression input = new LiteralExpression(null, Types.INTEGER);
        IsTypedExpression expr = new IsTypedExpression(input, Types.INTEGER);
        ObjectRow row = ObjectRow.create();
        
        // NULL should return false for IS TYPED
        Assert.assertEquals(expr.evaluate(row), false);
    }

    @Test
    public void testIsNotTypedNull() {
        Expression input = new LiteralExpression(null, Types.INTEGER);
        IsNotTypedExpression expr = new IsNotTypedExpression(input, Types.INTEGER);
        ObjectRow row = ObjectRow.create();
        
        // NULL should return true for IS NOT TYPED
        Assert.assertEquals(expr.evaluate(row), true);
    }

    // ==================== IS NOT TYPED Tests ====================

    @Test
    public void testIsNotTypedInteger() {
        Expression input = new LiteralExpression(42, Types.INTEGER);
        IsNotTypedExpression expr = new IsNotTypedExpression(input, Types.STRING);
        ObjectRow row = ObjectRow.create();
        
        // 42 is not a STRING, should return true
        Assert.assertEquals(expr.evaluate(row), true);
    }

    @Test
    public void testIsNotTypedSameType() {
        Expression input = new LiteralExpression(42, Types.INTEGER);
        IsNotTypedExpression expr = new IsNotTypedExpression(input, Types.INTEGER);
        ObjectRow row = ObjectRow.create();
        
        // 42 is an INTEGER, should return false
        Assert.assertEquals(expr.evaluate(row), false);
    }

    // ==================== Numeric Type Hierarchy Tests ====================

    @Test
    public void testNumericTypeCompatibility_Byte() {
        Expression input = new LiteralExpression((byte) 1, Types.BYTE);
        IsTypedExpression expr = new IsTypedExpression(input, Types.BYTE);
        ObjectRow row = ObjectRow.create();
        
        Assert.assertEquals(expr.evaluate(row), true);
    }

    @Test
    public void testNumericTypeCompatibility_Short() {
        Expression input = new LiteralExpression((short) 100, Types.SHORT);
        IsTypedExpression expr = new IsTypedExpression(input, Types.SHORT);
        ObjectRow row = ObjectRow.create();
        
        Assert.assertEquals(expr.evaluate(row), true);
    }

    @Test
    public void testNumericTypeCompatibility_Long() {
        Expression input = new LiteralExpression(1000000L, Types.LONG);
        IsTypedExpression expr = new IsTypedExpression(input, Types.LONG);
        ObjectRow row = ObjectRow.create();
        
        Assert.assertEquals(expr.evaluate(row), true);
    }

    @Test
    public void testNumericTypeCompatibility_Float() {
        Expression input = new LiteralExpression(3.14f, Types.FLOAT);
        IsTypedExpression expr = new IsTypedExpression(input, Types.FLOAT);
        ObjectRow row = ObjectRow.create();
        
        Assert.assertEquals(expr.evaluate(row), true);
    }

    @Test
    public void testNumericTypeCompatibility_Double() {
        Expression input = new LiteralExpression(3.14159, Types.DOUBLE);
        IsTypedExpression expr = new IsTypedExpression(input, Types.DOUBLE);
        ObjectRow row = ObjectRow.create();
        
        Assert.assertEquals(expr.evaluate(row), true);
    }

    // ==================== Type Mismatch Tests ====================

    @Test
    public void testIsTypedTypeMismatch_IntegerVsString() {
        Expression input = new LiteralExpression(42, Types.INTEGER);
        IsTypedExpression expr = new IsTypedExpression(input, Types.STRING);
        ObjectRow row = ObjectRow.create();
        
        // 42 is not a STRING
        Assert.assertEquals(expr.evaluate(row), false);
    }

    @Test
    public void testIsTypedTypeMismatch_StringVsInteger() {
        Expression input = new LiteralExpression("42", Types.STRING);
        IsTypedExpression expr = new IsTypedExpression(input, Types.INTEGER);
        ObjectRow row = ObjectRow.create();
        
        // "42" is not an INTEGER
        Assert.assertEquals(expr.evaluate(row), false);
    }

    @Test
    public void testIsTypedTypeMismatch_BooleanVsInteger() {
        Expression input = new LiteralExpression(true, Types.BOOLEAN);
        IsTypedExpression expr = new IsTypedExpression(input, Types.INTEGER);
        ObjectRow row = ObjectRow.create();
        
        // true is not an INTEGER
        Assert.assertEquals(expr.evaluate(row), false);
    }

    // ==================== Large Data Volume Tests ====================

    @Test
    public void testLargeVolumeIntegerTypeCheck() {
        int volumeSize = 100000;
        int matchCount = 0;
        
        IsTypedExpression expr = new IsTypedExpression(
            new LiteralExpression(42, Types.INTEGER), 
            Types.INTEGER
        );
        ObjectRow row = ObjectRow.create();
        
        // Simulate checking 100K integer values
        for (int i = 0; i < volumeSize; i++) {
            if ((Boolean) expr.evaluate(row)) {
                matchCount++;
            }
        }
        
        Assert.assertEquals(matchCount, volumeSize);
    }

    @Test
    public void testLargeVolumeMixedTypeChecks() {
        int volumeSize = 50000;
        int intMatches = 0;
        int stringMatches = 0;
        int doubleMatches = 0;
        
        IsTypedExpression intExpr = new IsTypedExpression(
            new LiteralExpression(42, Types.INTEGER), 
            Types.INTEGER
        );
        IsTypedExpression stringExpr = new IsTypedExpression(
            new LiteralExpression("test", Types.STRING), 
            Types.STRING
        );
        IsTypedExpression doubleExpr = new IsTypedExpression(
            new LiteralExpression(3.14, Types.DOUBLE), 
            Types.DOUBLE
        );
        ObjectRow row = ObjectRow.create();
        
        // Simulate checking mixed type values
        for (int i = 0; i < volumeSize; i++) {
            if ((Boolean) intExpr.evaluate(row)) intMatches++;
            if ((Boolean) stringExpr.evaluate(row)) stringMatches++;
            if ((Boolean) doubleExpr.evaluate(row)) doubleMatches++;
        }
        
        Assert.assertEquals(intMatches, volumeSize);
        Assert.assertEquals(stringMatches, volumeSize);
        Assert.assertEquals(doubleMatches, volumeSize);
    }

    @Test
    public void testLargeVolumeNullHandling() {
        int volumeSize = 100000;
        int nullCount = 0;
        
        IsTypedExpression expr = new IsTypedExpression(
            new LiteralExpression(null, Types.INTEGER), 
            Types.INTEGER
        );
        ObjectRow row = ObjectRow.create();
        
        // Simulate checking 100K NULL values
        for (int i = 0; i < volumeSize; i++) {
            if (!(Boolean) expr.evaluate(row)) {
                nullCount++;
            }
        }
        
        Assert.assertEquals(nullCount, volumeSize);
    }

    // ==================== Performance Tests ====================

    @Test
    public void testPerformanceIsTypedIntegerChecks() {
        int iterations = 1000000;
        IsTypedExpression expr = new IsTypedExpression(
            new LiteralExpression(42, Types.INTEGER), 
            Types.INTEGER
        );
        ObjectRow row = ObjectRow.create();
        
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            expr.evaluate(row);
        }
        long endTime = System.nanoTime();
        
        long durationMs = (endTime - startTime) / 1_000_000;
        double opsPerSecond = (iterations * 1000.0) / durationMs;
        
        System.out.println("Performance Test: " + iterations + " operations in " + durationMs + "ms");
        System.out.println("Operations per second: " + opsPerSecond);
        
        // Assert that performance is acceptable (> 100K ops/sec)
        Assert.assertTrue(opsPerSecond > 100000, 
            "Performance below threshold: " + opsPerSecond + " ops/sec");
    }

    @Test
    public void testPerformanceIsNotTypedTypeChecks() {
        int iterations = 1000000;
        IsNotTypedExpression expr = new IsNotTypedExpression(
            new LiteralExpression("test", Types.STRING), 
            Types.INTEGER
        );
        ObjectRow row = ObjectRow.create();
        
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            expr.evaluate(row);
        }
        long endTime = System.nanoTime();
        
        long durationMs = (endTime - startTime) / 1_000_000;
        double opsPerSecond = (iterations * 1000.0) / durationMs;
        
        System.out.println("Performance Test: " + iterations + " operations in " + durationMs + "ms");
        System.out.println("Operations per second: " + opsPerSecond);
        
        // Assert that performance is acceptable (> 100K ops/sec)
        Assert.assertTrue(opsPerSecond > 100000, 
            "Performance below threshold: " + opsPerSecond + " ops/sec");
    }

    // ==================== Edge Cases ====================

    @Test
    public void testEdgeCase_ZeroInteger() {
        Expression input = new LiteralExpression(0, Types.INTEGER);
        IsTypedExpression expr = new IsTypedExpression(input, Types.INTEGER);
        ObjectRow row = ObjectRow.create();
        
        Assert.assertEquals(expr.evaluate(row), true);
    }

    @Test
    public void testEdgeCase_NegativeInteger() {
        Expression input = new LiteralExpression(-42, Types.INTEGER);
        IsTypedExpression expr = new IsTypedExpression(input, Types.INTEGER);
        ObjectRow row = ObjectRow.create();
        
        Assert.assertEquals(expr.evaluate(row), true);
    }

    @Test
    public void testEdgeCase_EmptyString() {
        Expression input = new LiteralExpression("", Types.STRING);
        IsTypedExpression expr = new IsTypedExpression(input, Types.STRING);
        ObjectRow row = ObjectRow.create();
        
        Assert.assertEquals(expr.evaluate(row), true);
    }

    @Test
    public void testEdgeCase_MaxLong() {
        Expression input = new LiteralExpression(Long.MAX_VALUE, Types.LONG);
        IsTypedExpression expr = new IsTypedExpression(input, Types.LONG);
        ObjectRow row = ObjectRow.create();
        
        Assert.assertEquals(expr.evaluate(row), true);
    }

    @Test
    public void testEdgeCase_MinLong() {
        Expression input = new LiteralExpression(Long.MIN_VALUE, Types.LONG);
        IsTypedExpression expr = new IsTypedExpression(input, Types.LONG);
        ObjectRow row = ObjectRow.create();
        
        Assert.assertEquals(expr.evaluate(row), true);
    }

    // ==================== Copy and Show Expression Tests ====================

    @Test
    public void testIsTypedCopy() {
        Expression input = new LiteralExpression(42, Types.INTEGER);
        IsTypedExpression expr = new IsTypedExpression(input, Types.INTEGER);
        
        Expression copied = expr.copy(expr.getInputs());
        Assert.assertNotNull(copied);
        Assert.assertTrue(copied instanceof IsTypedExpression);
    }

    @Test
    public void testIsTypedShowExpression() {
        Expression input = new LiteralExpression(42, Types.INTEGER);
        IsTypedExpression expr = new IsTypedExpression(input, Types.INTEGER);
        
        String shown = expr.showExpression();
        Assert.assertTrue(shown.contains("IS TYPED"));
        Assert.assertTrue(shown.contains("INTEGER"));
    }

    @Test
    public void testIsNotTypedShowExpression() {
        Expression input = new LiteralExpression(42, Types.INTEGER);
        IsNotTypedExpression expr = new IsNotTypedExpression(input, Types.STRING);
        
        String shown = expr.showExpression();
        Assert.assertTrue(shown.contains("IS NOT TYPED"));
        Assert.assertTrue(shown.contains("STRING"));
    }

    // ==================== Helper: LiteralExpression ====================

    /**
     * Simple literal expression for testing.
     */
    private static class LiteralExpression extends AbstractExpression {
        private final Object value;
        private final org.apache.geaflow.common.type.IType<?> outputType;

        public LiteralExpression(Object value, org.apache.geaflow.common.type.IType<?> outputType) {
            this.value = value;
            this.outputType = outputType;
        }

        @Override
        public Object evaluate(Row row) {
            return value;
        }

        @Override
        public org.apache.geaflow.common.type.IType<?> getOutputType() {
            return outputType;
        }

        @Override
        public String showExpression() {
            return String.valueOf(value);
        }

        @Override
        public Expression copy(java.util.List<Expression> inputs) {
            return this;
        }

        @Override
        public java.util.List<Expression> getInputs() {
            return java.util.Collections.emptyList();
        }
    }
}

