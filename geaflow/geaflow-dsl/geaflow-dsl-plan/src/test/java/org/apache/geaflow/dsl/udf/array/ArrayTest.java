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

package org.apache.geaflow.dsl.udf.array;

import org.apache.geaflow.dsl.udf.table.array.ArrayAppend;
import org.apache.geaflow.dsl.udf.table.array.ArrayContains;
import org.apache.geaflow.dsl.udf.table.array.ArrayDistinct;
import org.apache.geaflow.dsl.udf.table.array.ArrayMax;
import org.apache.geaflow.dsl.udf.table.array.ArrayMin;
import org.apache.geaflow.dsl.udf.table.array.ArrayReverse;
import org.apache.geaflow.dsl.udf.table.array.ArrayUnion;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ArrayTest {

    @Test
    public void testArrayAppend() throws Exception {
        ArrayAppend udf = new ArrayAppend();
        Object[] input = new Object[]{1, 2, 4, -1};
        Object[] res = udf.eval(input, 6);
        Assert.assertEquals(res, new Object[]{1, 2, 4, -1, 6});
    }

    @Test
    public void testArrayContains() throws Exception {
        ArrayContains udf = new ArrayContains();
        Object[] input = new Object[]{1, 2, 4, -1};
        Assert.assertTrue(udf.eval(input, 2));
        Assert.assertFalse(udf.eval(input, -3));
    }

    @Test
    public void testArrayDistinct() throws Exception {
        ArrayDistinct udf = new ArrayDistinct();
        Object[] input = new Object[]{1, 2, 4, -1, 2, 1};
        Object[] res = udf.eval(input);
        Assert.assertEquals(res.length, 4);
    }

    @Test
    public void testArrayUnion() throws Exception {
        ArrayUnion udf = new ArrayUnion();
        Object[] input1 = new Object[]{1, 2, 4, -1};
        Object[] input2 = new Object[]{1, 3, 5, 2, -4, -6};
        Object[] res = udf.eval(input1, input2);
        Assert.assertEquals(res.length, 8);
    }

    @Test
    public void testArrayReverse() throws Exception {
        ArrayReverse udf = new ArrayReverse();
        Assert.assertEquals(udf.eval(new Object[]{1, 2, 4, -1}), new Object[]{-1, 4, 2, 1});
        Assert.assertEquals(udf.eval(new Object[]{1, null, 3}), new Object[]{3, null, 1});
        Assert.assertEquals(udf.eval(new Object[]{}), new Object[]{});
        Assert.assertNull(udf.eval(null));
    }

    @Test
    public void testArrayMax() throws Exception {
        ArrayMax udf = new ArrayMax();
        Assert.assertEquals(udf.eval(new Object[]{1, 2, 4, -1}), 4);
        Assert.assertEquals(udf.eval(new Object[]{"a", "c", "b"}), "c");
        Assert.assertEquals(udf.eval(new Object[]{1, null, 3}), 3);
        Assert.assertNull(udf.eval(new Object[]{}));
        Assert.assertNull(udf.eval(new Object[]{null, null}));
        Assert.assertNull(udf.eval(null));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testArrayMaxWithUnsupportedElementType() throws Exception {
        ArrayMax udf = new ArrayMax();
        udf.eval(new Object[]{new Object()});
    }

    @Test
    public void testArrayMin() throws Exception {
        ArrayMin udf = new ArrayMin();
        Assert.assertEquals(udf.eval(new Object[]{1, 2, 4, -1}), -1);
        Assert.assertEquals(udf.eval(new Object[]{"a", "c", "b"}), "a");
        Assert.assertEquals(udf.eval(new Object[]{1, null, 3}), 1);
        Assert.assertNull(udf.eval(new Object[]{}));
        Assert.assertNull(udf.eval(new Object[]{null, null}));
        Assert.assertNull(udf.eval(null));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testArrayMinWithUnsupportedElementType() throws Exception {
        ArrayMin udf = new ArrayMin();
        udf.eval(new Object[]{new Object()});
    }
}
