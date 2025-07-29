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

import static org.testng.Assert.assertEquals;

import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.dsl.udf.table.string.Instr;
import org.testng.annotations.Test;

public class UDFInstrTest {

    @Test
    public void test() {

        Instr udf = new Instr();

        assertEquals(1L, (long) udf.eval("abc", "a"));

        assertEquals(3L, (long) udf.eval("abc", "c"));

        assertEquals(0L, (long) udf.eval("abc", "d"));

        assertEquals(3L, (long) udf.eval("abc", "c", 1L));

        assertEquals(6L, (long) udf.eval("abcabc", "c", 4L));

        assertEquals(2L, (long) udf.eval("a\u0002abc\u0002c", "\u0002", 2L));

        assertEquals(2L, (long) udf.eval("a\002b\002c", "\002"));

        assertEquals(9L, (long) udf.eval("s.taobao.com", ".", 3L));
        assertEquals(2, (long) udf.eval("s.taobao.com", ".", 1L));

        assertEquals(0, (long) udf.eval("s.taobao.com", "abc"));
    }

    @Test
    public void testBinaryString() {

        Instr udf = new Instr();

        assertEquals(1L, (long) udf.eval(BinaryString.fromString("abc"), BinaryString.fromString("a")));

        assertEquals(3L, (long) udf.eval(BinaryString.fromString("abc"), BinaryString.fromString("c")));

        assertEquals(0L, (long) udf.eval(BinaryString.fromString("abc"), BinaryString.fromString("d")));

        assertEquals(3L, (long) udf.eval(BinaryString.fromString("abc"), BinaryString.fromString("c"), 1L));

        assertEquals(6L, (long) udf.eval(BinaryString.fromString("abcabc"), BinaryString.fromString("c"), 4L));

        assertEquals(2L, (long) udf.eval(BinaryString.fromString("a\u0002abc\u0002c"), BinaryString.fromString("\u0002"), 2L));

        assertEquals(2L, (long) udf.eval(BinaryString.fromString("a\002b\002c"), BinaryString.fromString("\002")));

        assertEquals(9L, (long) udf.eval(BinaryString.fromString("s.taobao.com"), BinaryString.fromString("."), 3L));
        assertEquals(2, (long) udf.eval(BinaryString.fromString("s.taobao.com"), BinaryString.fromString("."), 1L));

        assertEquals(0, (long) udf.eval(BinaryString.fromString("s.taobao.com"), BinaryString.fromString("abc")));
    }
}
