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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.dsl.udf.table.string.Ascii2String;
import org.apache.geaflow.dsl.udf.table.string.Base64Decode;
import org.apache.geaflow.dsl.udf.table.string.Base64Encode;
import org.apache.geaflow.dsl.udf.table.string.Concat;
import org.apache.geaflow.dsl.udf.table.string.ConcatWS;
import org.apache.geaflow.dsl.udf.table.string.Hash;
import org.apache.geaflow.dsl.udf.table.string.IndexOf;
import org.apache.geaflow.dsl.udf.table.string.Instr;
import org.apache.geaflow.dsl.udf.table.string.IsBlank;
import org.apache.geaflow.dsl.udf.table.string.KeyValue;
import org.apache.geaflow.dsl.udf.table.string.LTrim;
import org.apache.geaflow.dsl.udf.table.string.Length;
import org.apache.geaflow.dsl.udf.table.string.Like;
import org.apache.geaflow.dsl.udf.table.string.RTrim;
import org.apache.geaflow.dsl.udf.table.string.RegExp;
import org.apache.geaflow.dsl.udf.table.string.RegExpReplace;
import org.apache.geaflow.dsl.udf.table.string.Repeat;
import org.apache.geaflow.dsl.udf.table.string.Replace;
import org.apache.geaflow.dsl.udf.table.string.Reverse;
import org.apache.geaflow.dsl.udf.table.string.Space;
import org.apache.geaflow.dsl.udf.table.string.SplitEx;
import org.apache.geaflow.dsl.udf.table.string.UrlDecode;
import org.apache.geaflow.dsl.udf.table.string.UrlEncode;
import org.testng.annotations.Test;

public class UDFStringTest {

    @Test
    public void testAscii2String() {
        Ascii2String test = new Ascii2String();
        test.open(null);
        assertEquals(test.eval(48), "0");
        assertEquals(test.eval(48L), "0");
        assertNull(test.eval((Long) null));
        assertNull(test.eval((Integer) null));
    }

    @Test
    public void testBase64() {
        Base64Encode encode = new Base64Encode();
        encode.open(null);
        Base64Decode decode = new Base64Decode();
        decode.open(null);
        String test = "ant group";
        assertEquals(decode.eval(encode.eval(test)), test);
    }

    @Test
    public void testConcat() {
        String string = "ant group";
        BinaryString binaryString = BinaryString.fromString(string);
        Concat test = new Concat();
        test.open(null);
        assertEquals(test.eval(string, string, string), "ant groupant groupant group");
        assertEquals(test.eval(string, null, string), "ant groupant group");
        assertEquals(test.eval(string, string, null), "ant groupant group");
        assertEquals(test.eval((String) null, null, null), "");

        assertEquals(test.eval(binaryString, binaryString, binaryString),
            BinaryString.fromString("ant groupant groupant group"));
        assertEquals(test.eval(binaryString, null, binaryString), BinaryString.fromString(
            "ant groupant group"));
        assertEquals(test.eval(binaryString, binaryString, null), BinaryString.fromString(
            "ant groupant group"));
        assertEquals(test.eval(BinaryString.fromString("蚂蚁1"), BinaryString.fromString("蚂蚁2"),
            BinaryString.fromString("蚂蚁3")), BinaryString.fromString("蚂蚁1蚂蚁2蚂蚁3"));
        assertEquals(test.eval((BinaryString) null, null, null), BinaryString.fromString(""));
    }

    @Test
    public void testConcatWS() {
        String string = "ant group";
        ConcatWS test = new ConcatWS();
        BinaryString binaryString = BinaryString.fromString(string);
        test.open(null);
        assertEquals(test.eval(",", string, string, string),
            "ant group,ant group,ant group");
        assertEquals(test.eval("-", string, string, string),
            "ant group-ant group-ant group");
        assertEquals(test.eval("***", string, string, string),
            "ant group***ant group***ant group");
        assertEquals(test.eval(",", "1", string, "23"),
            "1,ant group,23");
        assertEquals(test.eval(",", string, null, string),
            "ant group,,ant group");
        assertEquals(test.eval(",", (String) null, null, null),
            ",,");
        assertEquals(test.eval(null, string, string, string),
            "ant groupant groupant group");

        assertEquals(
            test.eval(",", binaryString, binaryString, binaryString),
            BinaryString.fromString("ant group,ant group,ant group"));
        assertEquals(
            test.eval("-", binaryString, binaryString, binaryString),
            BinaryString.fromString("ant group-ant group-ant group"));
        assertEquals(
            test.eval("***", binaryString, binaryString, binaryString),
            BinaryString.fromString("ant group***ant group***ant group"));
        assertEquals(test.eval(",", BinaryString.fromString("1"),
                binaryString, BinaryString.fromString("23")),
            BinaryString.fromString("1,ant group,23"));
        assertEquals(test.eval(",", binaryString, null, binaryString),
            BinaryString.fromString("ant group,,ant group"));
        assertEquals(test.eval(",", (BinaryString) null, null, null),
            BinaryString.fromString(",,"));
        assertEquals(test.eval((String) null, binaryString, binaryString, binaryString),
            BinaryString.fromString("ant groupant groupant group"));
        assertEquals(test.eval((BinaryString) null, binaryString, binaryString, binaryString),
            BinaryString.fromString("ant groupant groupant group"));
        assertEquals(test.eval(",", BinaryString.fromString("蚂蚁1"),
            BinaryString.fromString("蚂蚁2"),
            BinaryString.fromString("蚂蚁3")), BinaryString.fromString("蚂蚁1,蚂蚁2,蚂蚁3"));
    }

    @Test
    public void testHash() {
        String string = "ant group";
        Hash test = new Hash();
        test.open(null);
        assertNull(test.eval((String) null));
        assertNull(test.eval((Integer) null));
    }

    @Test
    public void testIndexOf() {
        String string = "ant group";
        BinaryString binaryString = BinaryString.fromString("ant group");
        IndexOf test = new IndexOf();
        test.open(null);
        assertEquals((int) test.eval(string, "ant"), 0);
        assertEquals((int) test.eval(string, "group", 3), 4);
        assertEquals((int) test.eval(null, "ant"), -1);
        assertEquals((int) test.eval(string, "group", -1), 4);

        assertEquals((int) test.eval(binaryString, BinaryString.fromString("ant")), 0);
        assertEquals((int) test.eval(binaryString, BinaryString.fromString("group"), 3), 4);
        assertEquals((int) test.eval(null, BinaryString.fromString("ant")), -1);
        assertEquals((int) test.eval(binaryString, BinaryString.fromString("group"), -1), 4);

        assertEquals(
            (int) test.eval(BinaryString.fromString("数据砖头"), BinaryString.fromString("砖"), -1),
            2);
    }

    @Test
    public void testInstr() {
        String string = "ant group";
        Instr test = new Instr();
        test.open(null);
        assertEquals((long) test.eval(string, "group"), 5);
        assertEquals((long) test.eval(string, "group", 3L), 5);
        assertNull(test.eval(string, null, 3L));
        assertNull(test.eval(string, "group", -1L));
        assertNull(test.eval(string, "group", 3L, -1L));
    }

    @Test
    public void testIsBlank() {
        String string = "ant group";
        IsBlank test = new IsBlank();
        test.open(null);
        assertEquals((boolean) test.eval(string), false);
    }

    @Test
    public void testKeyValue() {
        KeyValue test = new KeyValue();
        test.open(null);
        assertEquals(test.eval("key1:value1 key2:value2", " ", ":", "key1"), "value1");
        assertNull(test.eval((Object) null, " ", ":", "key"));
    }

    @Test
    public void testLength() {
        Length test = new Length();
        test.open(null);
        assertEquals((long) test.eval(BinaryString.fromString("test")), 4);
    }

    @Test
    public void testLike() {
        Like test = new Like();
        test.open(null);
        assertTrue(test.eval("abc", "%abc"));
        assertTrue(test.eval("abc", "abc%"));
        assertTrue(test.eval("abc", "a%bc"));
        assertFalse(test.eval("test", "abc\\%"));
        assertFalse(test.eval("test", "abc\\%de%"));
        assertFalse(test.eval("test", "abc\\%de%"));
        assertFalse(test.eval("atest", "a%bc"));
    }

    @Test
    public void testLTrim() {
        LTrim test = new LTrim();
        test.open(null);
        assertEquals(test.eval("    facebook   "), "facebook   ");
        assertNull(test.eval((String) null));
    }

    @Test
    public void testRegExp() {
        RegExp test = new RegExp();
        test.open(null);
        assertTrue(test.eval("a.b.c.d.e.f", "."));
        assertNull(test.eval("a.b.c.d.e.f", null));
        assertFalse(test.eval("a.b.c.d.e.f", ""));
    }

    @Test
    public void testRegExpReplace() {
        RegExpReplace test = new RegExpReplace();
        test.open(null);
        assertEquals(test.eval("100-200", "(\\d+)", "num"), "num-num");
        assertNull(test.eval(null, "(\\d+)", "num"));
    }

    @Test
    public void testRepeat() {
        Repeat test = new Repeat();
        test.open(null);
        assertEquals(test.eval("AntGroup", 3), "AntGroupAntGroupAntGroup");
        assertNull(test.eval(null, 3));
    }

    @Test
    public void testReplace() {
        Replace test = new Replace();
        test.open(null);
        assertEquals(test.eval("AntGroup", "Ant", "ant"), "antGroup");
        assertEquals(test.eval((Object) null, "Ant", "ant"), "null");
    }

    @Test
    public void testReverse() {
        Reverse test = new Reverse();
        test.open(null);
        assertEquals(test.eval("AntGroup"), "puorGtnA");
        assertEquals(test.eval(BinaryString.fromString("AntGroup")), BinaryString.fromString("puorGtnA"));
        assertNull(test.eval((String) null));
        assertNull(test.eval((BinaryString) null));
    }

    @Test
    public void testRTrim() {
        RTrim test = new RTrim();
        test.open(null);
        assertEquals(test.eval("  AntGroup "), "  AntGroup");
        assertNull(test.eval((String) null));
    }

    @Test
    public void testSpace() {
        Space test = new Space();
        test.open(null);
        assertEquals(test.eval(6L), "      ");
        assertNull(test.eval(null));
    }

    @Test
    public void testSplitEx() {
        SplitEx test = new SplitEx();
        test.open(null);
        assertEquals(test.eval("a.b.c.d.e", ".", 1), "b");
        assertNull(test.eval(null, ".", 1));
        assertNull(test.eval("a.b.c.d.e", ".", -1));
        assertNull(test.eval("a.b.c.d.e", ".", 5));
        assertEquals(test.eval(BinaryString.fromString("a.b.c.d.e"), BinaryString.fromString("."), 1), BinaryString.fromString("b"));
        assertNull(test.eval(null, BinaryString.fromString("."), 1));
        assertNull(test.eval(null, BinaryString.fromString(""), 1));
        assertNull(test.eval(BinaryString.fromString("a.b.c.d.e"), BinaryString.fromString("."), -1));
        assertNull(test.eval(BinaryString.fromString("a.b.c.d.e"), BinaryString.fromString("."), 5));
    }

    @Test
    public void testUrlDecode() {
        UrlEncode encode = new UrlEncode();
        encode.open(null);
        UrlDecode decode = new UrlDecode();
        decode.open(null);
        String test = "ant group";
        assertEquals(decode.eval(encode.eval(test)), test);
        assertNull(encode.eval(null));
        assertNull(decode.eval(null));
    }
}
