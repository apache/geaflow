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

package org.apache.geaflow.dsl.udf.string;

import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.dsl.udf.table.string.LPad;
import org.apache.geaflow.dsl.udf.table.string.RPad;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PadTest {

    private static final BinaryString PAD = BinaryString.fromString("xy");

    @Test
    public void testLPad() {
        LPad lPad = new LPad();
        Assert.assertEquals(lPad.eval("hi", 5, "xy"), "xyxhi");
        Assert.assertEquals(lPad.eval("hello", 3, "x"), "hel");
        Assert.assertEquals(lPad.eval("hi", 0, "x"), "");
        Assert.assertNull(lPad.eval("hi", -1, "x"));
        Assert.assertEquals(lPad.eval("hi", 5, ""), "hi");
        Assert.assertNull(lPad.eval((String) null, 5, "x"));
        Assert.assertNull(lPad.eval("hi", null, "x"));
        Assert.assertNull(lPad.eval("hi", 5, (String) null));

        Assert.assertEquals(lPad.eval(BinaryString.fromString("hi"), 5, PAD),
            BinaryString.fromString("xyxhi"));
        Assert.assertNull(lPad.eval(BinaryString.fromString("hi"), -1, PAD));
        Assert.assertNull(lPad.eval((BinaryString) null, 5, PAD));
    }

    @Test
    public void testRPad() {
        RPad rPad = new RPad();
        Assert.assertEquals(rPad.eval("hi", 5, "xy"), "hixyx");
        Assert.assertEquals(rPad.eval("hello", 3, "x"), "hel");
        Assert.assertEquals(rPad.eval("hi", 0, "x"), "");
        Assert.assertNull(rPad.eval("hi", -1, "x"));
        Assert.assertEquals(rPad.eval("hi", 5, ""), "hi");
        Assert.assertNull(rPad.eval((String) null, 5, "x"));
        Assert.assertNull(rPad.eval("hi", null, "x"));
        Assert.assertNull(rPad.eval("hi", 5, (String) null));

        Assert.assertEquals(rPad.eval(BinaryString.fromString("hi"), 5, PAD),
            BinaryString.fromString("hixyx"));
        Assert.assertNull(rPad.eval(BinaryString.fromString("hi"), -1, PAD));
        Assert.assertNull(rPad.eval((BinaryString) null, 5, PAD));
    }

    @Test
    public void testUnicodeCodePoints() {
        LPad lPad = new LPad();
        RPad rPad = new RPad();
        Assert.assertEquals(lPad.eval("\u80a1\u7968", 4, "\u661f"),
            "\u661f\u661f\u80a1\u7968");
        Assert.assertEquals(rPad.eval("\u80a1\u7968", 4, "\u661f"),
            "\u80a1\u7968\u661f\u661f");
        Assert.assertEquals(lPad.eval("\ud83d\ude00x", 1, "y"), "\ud83d\ude00");
        Assert.assertEquals(rPad.eval("x", 3, "\ud83d\ude00"),
            "x\ud83d\ude00\ud83d\ude00");
    }
}
