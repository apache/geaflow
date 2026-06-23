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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.apache.geaflow.dsl.udf.table.math.Cbrt;
import org.apache.geaflow.dsl.udf.table.math.Trunc;
import org.testng.annotations.Test;

public class UDFMathTest {

    @Test
    public void testCbrt() {
        Cbrt cbrt = new Cbrt();

        assertEquals(cbrt.eval(27.0), 3.0, 1e-10);
        assertEquals(cbrt.eval(8.0), 2.0, 1e-10);
        assertEquals(cbrt.eval(0.0), 0.0, 1e-10);
        assertEquals(cbrt.eval(-27.0), -3.0, 1e-10);
        assertEquals(cbrt.eval(1.0), 1.0, 1e-10);

        assertEquals(cbrt.eval(27L), 3.0, 1e-10);
        assertEquals(cbrt.eval(0L), 0.0, 1e-10);
        assertEquals(cbrt.eval(-8L), -2.0, 1e-10);

        assertEquals(cbrt.eval(8), 2.0, 1e-10);
        assertEquals(cbrt.eval(0), 0.0, 1e-10);
        assertEquals(cbrt.eval(-27), -3.0, 1e-10);

        assertNull(cbrt.eval((Double) null));
        assertNull(cbrt.eval((Long) null));
        assertNull(cbrt.eval((Integer) null));
    }

    @Test
    public void testTrunc() {
        Trunc trunc = new Trunc();

        // truncate to integer (no second arg)
        assertEquals(trunc.eval(3.9), 3.0, 1e-10);
        assertEquals(trunc.eval(-3.9), -3.0, 1e-10);
        assertEquals(trunc.eval(0.0), 0.0, 1e-10);
        assertEquals(trunc.eval(1.0), 1.0, 1e-10);

        // truncate vs round: 3.567 truncated to 2 decimal places = 3.56, not 3.57
        assertEquals(trunc.eval(3.567, 2), 3.56, 1e-10);
        assertEquals(trunc.eval(-3.567, 2), -3.56, 1e-10);
        assertEquals(trunc.eval(3.567, 0), 3.0, 1e-10);
        assertEquals(trunc.eval(3.567, 1), 3.5, 1e-10);

        // with Long scale
        assertEquals(trunc.eval(3.567, 2L), 3.56, 1e-10);
        assertEquals(trunc.eval(-3.567, 2L), -3.56, 1e-10);

        // integer and long inputs pass through unchanged
        assertEquals((long) trunc.eval(5L), 5L);
        assertEquals((int) trunc.eval(5), 5);
        assertNull(trunc.eval((Long) null));
        assertNull(trunc.eval((Integer) null));

        // NaN and Infinity pass through
        assertEquals(trunc.eval(Double.NaN), Double.NaN);
        assertEquals(trunc.eval(Double.POSITIVE_INFINITY), Double.POSITIVE_INFINITY);
        assertEquals(trunc.eval(Double.NEGATIVE_INFINITY), Double.NEGATIVE_INFINITY);

        assertNull(trunc.eval((Double) null));
        assertNull(trunc.eval(3.5, (Integer) null));
        assertNull(trunc.eval(3.5, (Long) null));
        assertNull(trunc.eval(null, 2));
        assertNull(trunc.eval(null, 2L));
    }
}
