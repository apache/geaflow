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

package org.apache.geaflow.dsl.udf.math;

import org.apache.geaflow.dsl.udf.table.math.Cbrt;
import org.apache.geaflow.dsl.udf.table.math.Sign;
import org.apache.geaflow.dsl.udf.table.math.Trunc;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MathUdfTest {

    @Test
    public void testSign() {
        Sign sign = new Sign();
        // Double
        Assert.assertEquals(sign.eval(3.14), 1.0);
        Assert.assertEquals(sign.eval(-3.14), -1.0);
        Assert.assertEquals(sign.eval(0.0), 0.0);
        Assert.assertNull(sign.eval((Double) null));
        // Long
        Assert.assertEquals(sign.eval(5L), Long.valueOf(1L));
        Assert.assertEquals(sign.eval(-5L), Long.valueOf(-1L));
        Assert.assertEquals(sign.eval(0L), Long.valueOf(0L));
        Assert.assertNull(sign.eval((Long) null));
        // Integer
        Assert.assertEquals(sign.eval(5), Integer.valueOf(1));
        Assert.assertEquals(sign.eval(-5), Integer.valueOf(-1));
        Assert.assertEquals(sign.eval(0), Integer.valueOf(0));
        Assert.assertNull(sign.eval((Integer) null));
    }

    @Test
    public void testCbrt() {
        Cbrt cbrt = new Cbrt();
        Assert.assertEquals(cbrt.eval(27.0), 3.0);
        Assert.assertEquals(cbrt.eval(-8.0), -2.0);
        Assert.assertEquals(cbrt.eval(0.0), 0.0);
        Assert.assertEquals(cbrt.eval(1.0), 1.0);
        Assert.assertNull(cbrt.eval(null));
    }

    @Test
    public void testTrunc() {
        Trunc trunc = new Trunc();
        // Trunc vs Round: trunc(3.1465, 2) = 3.14, round(3.1465, 2) = 3.15
        Assert.assertEquals(trunc.eval(3.1465, 2L), 3.14);
        Assert.assertEquals(trunc.eval(3.1415, 2L), 3.14);
        // Negative: DOWN mode truncates toward zero
        Assert.assertEquals(trunc.eval(-3.1465, 2L), -3.14);
        // Default: truncate to 0 decimal places
        Assert.assertEquals(trunc.eval(3.1415), 3.0);
        Assert.assertEquals(trunc.eval(-3.9), -3.0);
        // Integer scale
        Assert.assertEquals(trunc.eval(3.1465, 2), 3.14);
        // Null handling
        Assert.assertNull(trunc.eval(null, 2L));
        Assert.assertNull(trunc.eval(3.14, (Long) null));
        Assert.assertNull(trunc.eval(null, 2));
        Assert.assertNull(trunc.eval(3.14, (Integer) null));
        Assert.assertNull(trunc.eval((Double) null));
        // Long/Integer pass-through
        Assert.assertEquals(trunc.eval(5L), Long.valueOf(5L));
        Assert.assertEquals(trunc.eval(5), Integer.valueOf(5));
    }
}
