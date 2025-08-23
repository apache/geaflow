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

package org.apache.geaflow.common.binary;

import java.nio.charset.StandardCharsets;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BinaryStringTest {

    @Test
    public void testBinaryString() {
        String testStr = "djwakfmnlkgritio3175453406fsdjhhflkdsk1`26ad09~|?!!";
        byte[] bytes = testStr.getBytes(StandardCharsets.UTF_8);
        BinaryString binaryTestStr = BinaryString.fromBytes(bytes);
        Assert.assertEquals(testStr.length(), binaryTestStr.getLength());
        Assert.assertEquals(binaryTestStr.getBytes(), bytes);

        String compareStr = "3218478293djadhfue8917535566";
        BinaryString binaryCompareStr = BinaryString.fromString(compareStr);
        Assert.assertTrue(binaryCompareStr.compareTo(binaryTestStr) < 0);
        Assert.assertNotEquals(binaryTestStr, binaryCompareStr);

        BinaryString splitStr = BinaryString.fromString("123_445_13da_deg");
        BinaryString[] splits = splitStr.split(BinaryString.fromString("_"), 0);
        Assert.assertEquals(splits.length, 4);
        Assert.assertTrue(splitStr.contains(BinaryString.fromString("13da")));
        Assert.assertFalse(splitStr.contains(BinaryString.fromString("21")));
        Assert.assertTrue(splitStr.startsWith(BinaryString.fromString("123")));
    }

    @Test
    public void testStartWith() {
        BinaryString str = BinaryString.fromString("abcabc");
        Assert.assertTrue(str.startsWith(BinaryString.fromString("abc")));
        Assert.assertFalse(str.startsWith(BinaryString.fromString("c")));
    }

    @Test
    public void testEndsWith() {
        BinaryString str = BinaryString.fromString("abcabc");
        Assert.assertTrue(str.endsWith(BinaryString.fromString("abc")));
        Assert.assertTrue(str.endsWith(BinaryString.fromString("c")));
        Assert.assertFalse(str.endsWith(BinaryString.fromString("d")));
    }
}
