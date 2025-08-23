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

package org.apache.geaflow.utils.math;

import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MathUtilTest {

    @Test
    public void testDeviates() {
    }

    @Test
    public void testPercentile() {

        List<Long> input = Arrays.asList(1L, 2L, 8L, 9L, 13L, 3L, 12L, 4L, 7L, 6L, 11L, 10L, 5L);

        long result = MathUtil.percentile(input, 0);
        Assert.assertEquals(0, result);

        result = MathUtil.percentile(input, 50);
        Assert.assertEquals(7, result);

        result = MathUtil.percentile(input, 90);
        Assert.assertEquals(12, result);
    }

    @Test
    public void testFindTwoGroups() {
        long[] input = new long[]{1L, 2L, 8L, 9L, 13L, 3L, 12L, 4L, 7L, 6L, 11L, 10L, 5L};
        long[][] result = MathUtil.findTwoGroups(input);
        Assert.assertEquals(6, result[0].length);
        Arrays.sort(result[0]);
        Assert.assertEquals(new long[]{1L, 2L, 3L, 4L, 5L, 6L}, result[0]);
        Arrays.sort(result[1]);
        Assert.assertEquals(7, result[1].length);
        Assert.assertEquals(new long[]{7L, 8L, 9L, 10L, 11L, 12L, 13L}, result[1]);
    }

    @Test
    public void testAverage() {

        long[] input = new long[]{1L, 2L, 8L, 9L, 13L, 3L, 12L, 4L, 7L, 6L, 11L, 10L, 5L};
        long result = MathUtil.average(input);
        Assert.assertEquals(7, result);

        List<Long> array = Arrays.asList(1L, 2L, 8L, 9L, 13L, 3L, 12L, 4L, 7L, 6L, 11L, 10L, 5L);
        result = MathUtil.average(array);
        Assert.assertEquals(7, result);
    }

    @Test
    public void testMedian() {
        List<Long> array = Arrays.asList(1L, 2L, 8L, 9L, 13L, 3L, 12L, 4L, 7L, 6L, 11L, 10L, 5L);
        long result = MathUtil.median(array);
        Assert.assertEquals(7, result);
    }

    @Test
    public void testFindNextPositivePowerOfTwo() {
        int result = MathUtil.findNextPositivePowerOfTwo(10);
        Assert.assertEquals(16, result);
    }

    @Test
    public void testSafeFindNextPositivePowerOfTwo() {

        int result = MathUtil.safeFindNextPositivePowerOfTwo(Integer.MAX_VALUE);
        Assert.assertEquals(0x40000000, result);
    }

    @Test
    public void testIsPrime() {
        boolean result = MathUtil.isPrime(7);
        Assert.assertEquals(true, result);

        result = MathUtil.isPrime(10);
        Assert.assertEquals(false, result);
    }

    @Test
    public void testNextPrime() {
        int result = MathUtil.nextPrime(7);
        Assert.assertEquals(7, result);

        result = MathUtil.nextPrime(10);
        Assert.assertEquals(11, result);
    }

    @Test
    public void testLongToIntWithBitMixing() {
        int result = MathUtil.longToIntWithBitMixing(10L);
        Assert.assertEquals(-1456339591, result);

    }

    @Test
    public void testBitMix() {
        int result = MathUtil.bitMix(10);
        Assert.assertEquals(-383449968, result);
    }

    @Test
    public void testMurmurHash() {
        int code = 1;
        Assert.assertEquals(68075478, MathUtil.murmurHash(code));
        code = -1;
        Assert.assertEquals(1982413648, MathUtil.murmurHash(code));

        Object objectCode = 1;
        Assert.assertEquals(68075478, MathUtil.murmurHash(objectCode.hashCode()));
        objectCode = -1;
        Assert.assertEquals(1982413648, MathUtil.murmurHash(objectCode.hashCode()));

        int minCode = Integer.MIN_VALUE;
        Assert.assertEquals(1718298732, MathUtil.murmurHash(minCode));

        Object minHashCode = Integer.MIN_VALUE;
        Assert.assertEquals(1718298732, MathUtil.murmurHash(minHashCode.hashCode()));

        int maxCode = Integer.MAX_VALUE;
        Assert.assertEquals(1653689534, MathUtil.murmurHash(maxCode));

        Object maxHashCode = Integer.MAX_VALUE;
        Assert.assertEquals(1653689534, MathUtil.murmurHash(maxHashCode.hashCode()));

        Object StringCode = "hello";
        Assert.assertEquals(1715862179, MathUtil.murmurHash(StringCode.hashCode()));
    }

    @Test
    public void testIsPowerOf2() {
        Assert.assertFalse(MathUtil.isPowerOf2(0));
        Assert.assertFalse(MathUtil.isPowerOf2(3));
        Assert.assertFalse(MathUtil.isPowerOf2(-3));
        Assert.assertTrue(MathUtil.isPowerOf2(1));
        Assert.assertTrue(MathUtil.isPowerOf2(4));
        Assert.assertFalse(MathUtil.isPowerOf2(-4));
    }

    @Test
    public void testMinPowerOf2() {
        int input = 1;
        Assert.assertEquals(1, MathUtil.minPowerOf2(input));

        input = 2;
        Assert.assertEquals(2, MathUtil.minPowerOf2(input));

        input = 3;
        Assert.assertEquals(4, MathUtil.minPowerOf2(input));

        input = 5;
        Assert.assertEquals(8, MathUtil.minPowerOf2(input));
        input = 6;
        Assert.assertEquals(8, MathUtil.minPowerOf2(input));
        input = 7;
        Assert.assertEquals(8, MathUtil.minPowerOf2(input));
        input = 8;
        Assert.assertEquals(8, MathUtil.minPowerOf2(input));
    }
}
