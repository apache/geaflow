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

package org.apache.geaflow.dsl.udf.agg;

import com.google.common.collect.Lists;
import org.apache.geaflow.dsl.udf.table.agg.AvgDouble;
import org.apache.geaflow.dsl.udf.table.agg.AvgInteger;
import org.apache.geaflow.dsl.udf.table.agg.AvgLong;
import org.apache.geaflow.dsl.udf.table.agg.BitAndInteger;
import org.apache.geaflow.dsl.udf.table.agg.BitOrInteger;
import org.apache.geaflow.dsl.udf.table.agg.BoolAnd;
import org.apache.geaflow.dsl.udf.table.agg.Count;
import org.apache.geaflow.dsl.udf.table.agg.MaxDouble;
import org.apache.geaflow.dsl.udf.table.agg.MaxInteger;
import org.apache.geaflow.dsl.udf.table.agg.MaxLong;
import org.apache.geaflow.dsl.udf.table.agg.MinDouble;
import org.apache.geaflow.dsl.udf.table.agg.MinInteger;
import org.apache.geaflow.dsl.udf.table.agg.MinLong;
import org.apache.geaflow.dsl.udf.table.agg.SumDouble;
import org.apache.geaflow.dsl.udf.table.agg.SumInteger;
import org.apache.geaflow.dsl.udf.table.agg.SumLong;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UDAFTest {

    @Test
    public void testAvgDouble() {
        AvgDouble af = new AvgDouble();
        AvgDouble.Accumulator accumulator = af.createAccumulator();
        af.accumulate(accumulator, 1.0);
        Assert.assertEquals(af.getValue(accumulator), 1.0);
        af.resetAccumulator(accumulator);
        Assert.assertNull(af.getValue(accumulator));
        af.merge(accumulator, Lists.newArrayList(accumulator));
        Assert.assertEquals(af.getValue(accumulator), null);
    }

    @Test
    public void testAvgInteger() {
        AvgInteger af = new AvgInteger();
        AvgInteger.Accumulator accumulator = af.createAccumulator();
        af.accumulate(accumulator, 1);
        Assert.assertEquals(af.getValue(accumulator), 1.0);
        af.resetAccumulator(accumulator);
        Assert.assertNull(af.getValue(accumulator));
        af.merge(accumulator, Lists.newArrayList(accumulator));
        Assert.assertEquals(af.getValue(accumulator), null);
    }

    @Test
    public void testAvgLong() {
        AvgLong af = new AvgLong();
        AvgLong.Accumulator accumulator = af.createAccumulator();
        af.accumulate(accumulator, 1L);
        Assert.assertEquals(af.getValue(accumulator), 1.0);
        af.resetAccumulator(accumulator);
        Assert.assertNull(af.getValue(accumulator));
        af.merge(accumulator, Lists.newArrayList(accumulator));
        Assert.assertEquals(af.getValue(accumulator), null);
    }

    @Test
    public void testCount() {
        Count af = new Count();
        Count.Accumulator accumulator = af.createAccumulator();
        af.accumulate(accumulator, 1);
        Assert.assertEquals((long) af.getValue(accumulator), 1);
        af.resetAccumulator(accumulator);
        Assert.assertEquals((long) af.getValue(accumulator), 0);
        af.merge(accumulator, Lists.newArrayList(accumulator));
        Assert.assertEquals((long) af.getValue(accumulator), 0);
    }

    @Test
    public void testBitAndInteger() {
        BitAndInteger af = new BitAndInteger();
        BitAndInteger.Accumulator accumulator = af.createAccumulator();
        af.accumulate(accumulator, null);
        Assert.assertNull(af.getValue(accumulator));
        af.accumulate(accumulator, 7);
        af.accumulate(accumulator, 3);
        Assert.assertEquals((int) af.getValue(accumulator), 3);

        BitAndInteger.Accumulator toMerge = af.createAccumulator();
        af.accumulate(toMerge, 1);
        af.merge(accumulator, Lists.newArrayList(toMerge));
        Assert.assertEquals((int) af.getValue(accumulator), 1);

        af.resetAccumulator(accumulator);
        Assert.assertNull(af.getValue(accumulator));
    }

    @Test
    public void testBitOrInteger() {
        BitOrInteger af = new BitOrInteger();
        BitOrInteger.Accumulator accumulator = af.createAccumulator();
        af.accumulate(accumulator, null);
        Assert.assertNull(af.getValue(accumulator));
        af.accumulate(accumulator, 4);
        af.accumulate(accumulator, 2);
        Assert.assertEquals((int) af.getValue(accumulator), 6);

        BitOrInteger.Accumulator toMerge = af.createAccumulator();
        af.accumulate(toMerge, 1);
        af.merge(accumulator, Lists.newArrayList(toMerge));
        Assert.assertEquals((int) af.getValue(accumulator), 7);

        af.resetAccumulator(accumulator);
        Assert.assertNull(af.getValue(accumulator));
    }

    @Test
    public void testBoolAnd() {
        BoolAnd af = new BoolAnd();
        BoolAnd.Accumulator accumulator = af.createAccumulator();
        af.accumulate(accumulator, null);
        Assert.assertNull(af.getValue(accumulator));
        af.accumulate(accumulator, true);
        Assert.assertTrue(af.getValue(accumulator));

        BoolAnd.Accumulator toMerge = af.createAccumulator();
        af.accumulate(toMerge, false);
        af.merge(accumulator, Lists.newArrayList(toMerge));
        Assert.assertFalse(af.getValue(accumulator));

        af.resetAccumulator(accumulator);
        Assert.assertNull(af.getValue(accumulator));
    }

    @Test
    public void testMaxDouble() {
        MaxDouble af = new MaxDouble();
        MaxDouble.Accumulator accumulator = af.createAccumulator();
        af.accumulate(accumulator, 1.0);
        Assert.assertEquals(af.getValue(accumulator), 1.0);
        af.merge(accumulator, Lists.newArrayList(accumulator));
        Assert.assertEquals(af.getValue(accumulator), 1.0);
        af.resetAccumulator(accumulator);
        Assert.assertNull(af.getValue(accumulator));
    }

    @Test
    public void testMaxInteger() {
        MaxInteger af = new MaxInteger();
        MaxInteger.Accumulator accumulator = af.createAccumulator();
        af.accumulate(accumulator, 1);
        Assert.assertEquals((long) af.getValue(accumulator), 1);
        af.merge(accumulator, Lists.newArrayList(accumulator));
        Assert.assertEquals((long) af.getValue(accumulator), 1);
        af.resetAccumulator(accumulator);
        Assert.assertNull(af.getValue(accumulator));
    }

    @Test
    public void testMaxLong() {
        MaxLong af = new MaxLong();
        MaxLong.Accumulator accumulator = af.createAccumulator();
        af.accumulate(accumulator, 1L);
        Assert.assertEquals((long) af.getValue(accumulator), 1);
        af.merge(accumulator, Lists.newArrayList(accumulator));
        Assert.assertEquals((long) af.getValue(accumulator), 1);
        af.resetAccumulator(accumulator);
        Assert.assertNull(af.getValue(accumulator));
    }

    @Test
    public void testMinDouble() {
        MinDouble af = new MinDouble();
        MinDouble.Accumulator accumulator = af.createAccumulator();
        af.accumulate(accumulator, 1.0);
        Assert.assertEquals(af.getValue(accumulator), 1.0);
        af.merge(accumulator, Lists.newArrayList(accumulator));
        Assert.assertEquals(af.getValue(accumulator), 1.0);
        af.resetAccumulator(accumulator);
        Assert.assertNull(af.getValue(accumulator));
    }

    @Test
    public void testMinInteger() {
        MinInteger af = new MinInteger();
        MinInteger.Accumulator accumulator = af.createAccumulator();
        af.accumulate(accumulator, 1);
        Assert.assertEquals((long) af.getValue(accumulator), 1);
        af.merge(accumulator, Lists.newArrayList(accumulator));
        Assert.assertEquals((long) af.getValue(accumulator), 1);
        af.resetAccumulator(accumulator);
        Assert.assertNull(af.getValue(accumulator));
    }

    @Test
    public void testMinLong() {
        MinLong af = new MinLong();
        MinLong.Accumulator accumulator = af.createAccumulator();
        af.accumulate(accumulator, 1L);
        Assert.assertEquals((long) af.getValue(accumulator), 1);
        af.merge(accumulator, Lists.newArrayList(accumulator));
        Assert.assertEquals((long) af.getValue(accumulator), 1);
        af.resetAccumulator(accumulator);
        Assert.assertNull(af.getValue(accumulator));
    }

    @Test
    public void testSumDouble() {
        SumDouble af = new SumDouble();
        SumDouble.Accumulator accumulator = af.createAccumulator();
        af.accumulate(accumulator, 1.0);
        Assert.assertEquals(af.getValue(accumulator), 1.0);
        af.resetAccumulator(accumulator);
        Assert.assertEquals(af.getValue(accumulator), 0.0);
        af.merge(accumulator, Lists.newArrayList(accumulator));
        Assert.assertEquals(af.getValue(accumulator), 0.0);
    }

    @Test
    public void testSumInteger() {
        SumInteger af = new SumInteger();
        SumInteger.Accumulator accumulator = af.createAccumulator();
        af.accumulate(accumulator, 1);
        Assert.assertEquals((long) af.getValue(accumulator), 1);
        af.resetAccumulator(accumulator);
        Assert.assertEquals((int) af.getValue(accumulator), 0);
        af.merge(accumulator, Lists.newArrayList(accumulator));
        Assert.assertEquals((long) af.getValue(accumulator), 0);
    }

    @Test
    public void testSumLong() {
        SumLong af = new SumLong();
        SumLong.Accumulator accumulator = af.createAccumulator();
        af.accumulate(accumulator, 1L);
        Assert.assertEquals((long) af.getValue(accumulator), 1);
        af.resetAccumulator(accumulator);
        Assert.assertEquals((long) af.getValue(accumulator), 0);
        af.merge(accumulator, Lists.newArrayList(accumulator));
        Assert.assertEquals((long) af.getValue(accumulator), 0);
    }
}
