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

package org.apache.geaflow.dsl.runtime.plan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.geaflow.common.type.primitive.BooleanType;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.ArrayType;
import org.apache.geaflow.dsl.runtime.expression.DefaultExpressionBuilder;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.expression.ExpressionBuilder;
import org.testng.annotations.Test;

public class ExpressionTest {

    @Test
    public void testEqual() {
        ExpressionBuilder builder = new DefaultExpressionBuilder();
        Expression expression = builder.equal(
            builder.literal(1, IntegerType.INSTANCE),
            builder.literal(1, IntegerType.INSTANCE)
        );
        assertEquals(expression.showExpression(), "1 = 1");
        Expression newExpr = expression.copy(expression.getInputs());
        assertEquals(newExpr.evaluate(Row.EMPTY), true);
    }

    @Test
    public void testGTE() {
        ExpressionBuilder builder = new DefaultExpressionBuilder();
        Expression expression = builder.greaterEqThen(
            builder.literal(1, IntegerType.INSTANCE),
            builder.literal(1, IntegerType.INSTANCE)
        );
        assertEquals(expression.showExpression(), "1 >= 1");
        Expression newExpr = expression.copy(expression.getInputs());
        assertEquals(newExpr.evaluate(Row.EMPTY), true);
    }

    @Test
    public void testGT() {
        ExpressionBuilder builder = new DefaultExpressionBuilder();
        Expression expression = builder.greaterThan(
            builder.literal(1, IntegerType.INSTANCE),
            builder.literal(1, IntegerType.INSTANCE)
        );
        assertEquals(expression.showExpression(), "1 > 1");
        Expression newExpr = expression.copy(expression.getInputs());
        assertEquals(newExpr.evaluate(Row.EMPTY), false);
    }

    @Test
    public void testIsFalse() {
        ExpressionBuilder builder = new DefaultExpressionBuilder();
        Expression expression = builder.isFalse(
            builder.literal(false, BooleanType.INSTANCE)
        );
        assertEquals(expression.showExpression(), "false is false");
        Expression newExpr = expression.copy(expression.getInputs());
        assertEquals(newExpr.evaluate(Row.EMPTY), true);
    }

    @Test
    public void testIsNotFalse() {
        ExpressionBuilder builder = new DefaultExpressionBuilder();
        Expression expression = builder.isNotFalse(
            builder.literal(false, BooleanType.INSTANCE)
        );
        assertEquals(expression.showExpression(), "false is not false");
        Expression newExpr = expression.copy(expression.getInputs());
        assertEquals(newExpr.evaluate(Row.EMPTY), false);
    }

    @Test
    public void testIsNotNull() {
        ExpressionBuilder builder = new DefaultExpressionBuilder();
        Expression expression = builder.isNotNull(
            builder.literal(false, BooleanType.INSTANCE)
        );
        assertEquals(expression.showExpression(), "false is not null");
        Expression newExpr = expression.copy(expression.getInputs());
        assertEquals(newExpr.evaluate(Row.EMPTY), true);
    }

    @Test
    public void testIsNotTrue() {
        ExpressionBuilder builder = new DefaultExpressionBuilder();
        Expression expression = builder.isNotTrue(
            builder.literal(false, BooleanType.INSTANCE)
        );
        assertEquals(expression.showExpression(), "false is not true");
        Expression newExpr = expression.copy(expression.getInputs());
        assertEquals(newExpr.evaluate(Row.EMPTY), true);
    }

    @Test
    public void testIsNull() {
        ExpressionBuilder builder = new DefaultExpressionBuilder();
        Expression expression = builder.isNull(
            builder.literal(false, BooleanType.INSTANCE)
        );
        assertEquals(expression.showExpression(), "false is null");
        Expression newExpr = expression.copy(expression.getInputs());
        assertEquals(newExpr.evaluate(Row.EMPTY), false);
    }

    @Test
    public void testIsTrue() {
        ExpressionBuilder builder = new DefaultExpressionBuilder();
        Expression expression = builder.isTrue(
            builder.literal(false, BooleanType.INSTANCE)
        );
        assertEquals(expression.showExpression(), "false is true");
        Expression newExpr = expression.copy(expression.getInputs());
        assertEquals(newExpr.evaluate(Row.EMPTY), false);
    }

    @Test
    public void testLTE() {
        ExpressionBuilder builder = new DefaultExpressionBuilder();
        Expression expression = builder.lessEqThan(
            builder.literal(1, IntegerType.INSTANCE),
            builder.literal(2, IntegerType.INSTANCE)
        );
        assertEquals(expression.showExpression(), "1 <= 2");
        Expression newExpr = expression.copy(expression.getInputs());
        assertEquals(newExpr.evaluate(Row.EMPTY), true);
    }

    @Test
    public void testLT() {
        ExpressionBuilder builder = new DefaultExpressionBuilder();
        Expression expression = builder.lessThan(
            builder.literal(1, IntegerType.INSTANCE),
            builder.literal(1, IntegerType.INSTANCE)
        );
        assertEquals(expression.showExpression(), "1 < 1");
        Expression newExpr = expression.copy(expression.getInputs());
        assertEquals(newExpr.evaluate(Row.EMPTY), false);
    }

    @Test
    public void testNot() {
        ExpressionBuilder builder = new DefaultExpressionBuilder();
        Expression expression = builder.not(
            builder.literal(false, BooleanType.INSTANCE)
        );
        assertEquals(expression.showExpression(), "Not(false)");
        Expression newExpr = expression.copy(expression.getInputs());
        assertEquals(newExpr.evaluate(Row.EMPTY), true);
    }

    @Test
    public void testOr() {
        ExpressionBuilder builder = new DefaultExpressionBuilder();
        Expression expression = builder.or(Lists.newArrayList(
            builder.literal(false, BooleanType.INSTANCE),
            builder.literal(true, BooleanType.INSTANCE))
        );
        assertEquals(expression.showExpression(), "Or(false,true)");
        Expression newExpr = expression.copy(expression.getInputs());
        assertEquals(newExpr.evaluate(Row.EMPTY), true);
    }

    @Test
    public void testItem() {
        ExpressionBuilder builder = new DefaultExpressionBuilder();
        Expression expression = builder.item(
            builder.literal(new Integer[]{0, 1}, new ArrayType(IntegerType.INSTANCE)),
            builder.literal(1, IntegerType.INSTANCE)
        );
        assertTrue(expression.showExpression().contains("java.lang.Integer"));
        assertTrue(expression.showExpression().contains("[1]"));
        Expression newExpr = expression.copy(expression.getInputs());
        try {
            throw new GeaFlowDSLException(String.valueOf(newExpr.evaluate(Row.EMPTY)));
        } catch (Exception e) {
            assertEquals(e.getMessage(), "1");
        }

    }

    @Test
    public void testSplitByEnd() {
        ExpressionBuilder builder = new DefaultExpressionBuilder();
        Expression expression1 = builder.not(
            builder.literal(false, BooleanType.INSTANCE)
        );
        Expression expression2 = builder.not(
            builder.literal(true, BooleanType.INSTANCE)
        );
        Expression expression = builder.and(Stream.of(expression1, expression2).collect(Collectors.toList()));
        assertEquals(expression.showExpression(), "And(Not(false),Not(true))");
        List<Expression> exprList = expression.splitByAnd();
        assertEquals(exprList.size(), 2);
        assertEquals(exprList.get(0), expression1);
        assertEquals(exprList.get(1), expression2);
    }
}
