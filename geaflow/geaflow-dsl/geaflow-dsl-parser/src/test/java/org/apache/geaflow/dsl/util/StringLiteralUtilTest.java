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

package org.apache.geaflow.dsl.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class StringLiteralUtilTest {

    @Test
    public void testUnicodeEscapeSequence_Basic() {
        // Test \u0041 => "A"
        String input = "\"\\u0041\"";
        String result = StringLiteralUtil.unescapeSQLString(input);
        Assert.assertEquals(result, "A");
    }

    @Test
    public void testUnicodeEscapeSequence_AccentedCharacter() {
        // Test \u00e9 => "é"
        String input = "\"\\u00e9\"";
        String result = StringLiteralUtil.unescapeSQLString(input);
        Assert.assertEquals(result, "é");
    }

    @Test
    public void testUnicodeEscapeSequence_MultipleUnicode() {
        // Test multiple Unicode characters
        String input = "\"\\u0048\\u0065\\u006c\\u006c\\u006f\"";
        String result = StringLiteralUtil.unescapeSQLString(input);
        Assert.assertEquals(result, "Hello");
    }

    @Test
    public void testUnicodeEscapeSequence_ChineseCharacter() {
        // Test Chinese character \u4e2d => "中"
        String input = "\"\\u4e2d\"";
        String result = StringLiteralUtil.unescapeSQLString(input);
        Assert.assertEquals(result, "中");
    }

    @Test
    public void testUnicodeEscapeSequence_UpperCaseHex() {
        // Test uppercase hex digits
        String input = "\"\\u00FF\"";
        String result = StringLiteralUtil.unescapeSQLString(input);
        Assert.assertEquals(result, "\u00FF");
    }

    @Test
    public void testUnicodeEscapeSequence_LowerCaseHex() {
        // Test lowercase hex digits
        String input = "\"\\u00ff\"";
        String result = StringLiteralUtil.unescapeSQLString(input);
        Assert.assertEquals(result, "\u00FF");
    }

    @Test
    public void testUnicodeEscapeSequence_InvalidHexCharacter() {
        // Test invalid hex character (should fall back to regular escape handling)
        String input = "\"\\u00g1\"";
        String result = StringLiteralUtil.unescapeSQLString(input);
        // Should not crash and should handle gracefully
        Assert.assertNotNull(result);
    }

    @Test
    public void testUnicodeEscapeSequence_IncompleteSequence() {
        // Test incomplete sequence (less than 4 hex digits)
        String input = "\"\\u00a\"";
        String result = StringLiteralUtil.unescapeSQLString(input);
        // Should handle gracefully without crashing
        Assert.assertNotNull(result);
    }

    @Test
    public void testCommonEscapeSequences() {
        // Test common escape sequences
        String input = "\"\\n\\t\\r\\b\"";
        String result = StringLiteralUtil.unescapeSQLString(input);
        Assert.assertEquals(result, "\n\t\r\b");
    }

    @Test
    public void testMixedEscapeSequences() {
        // Test mixing Unicode and common escape sequences
        String input = "\"Hello\\u0020World\\n\"";
        String result = StringLiteralUtil.unescapeSQLString(input);
        Assert.assertEquals(result, "Hello World\n");
    }

    @Test
    public void testUnicodeEscapeSequence_MaxValue() {
        // Test maximum Unicode value \uFFFF
        String input = "\"\\uFFFF\"";
        String result = StringLiteralUtil.unescapeSQLString(input);
        Assert.assertEquals(result, "\uFFFF");
    }

    @Test
    public void testUnicodeEscapeSequence_ZeroValue() {
        // Test zero value \u0000
        String input = "\"\\u0000\"";
        String result = StringLiteralUtil.unescapeSQLString(input);
        Assert.assertEquals(result, "\u0000");
    }

    @Test
    public void testUnicodeEscapeSequence_WithQuotes() {
        // Test with single quotes enclosure
        String input = "'\\u0041'";
        String result = StringLiteralUtil.unescapeSQLString(input);
        Assert.assertEquals(result, "A");
    }

    @Test
    public void testEscapeSQLString_Basic() {
        // Test escapeSQLString method
        String input = "Hello\nWorld";
        String result = StringLiteralUtil.escapeSQLString(input);
        Assert.assertTrue(result.startsWith("'"));
        Assert.assertTrue(result.endsWith("'"));
        Assert.assertTrue(result.contains("\\n"));
    }

    @Test
    public void testEscapeSQLString_WithUnicode() {
        // Test escapeSQLString with Unicode characters
        String input = "A";
        String result = StringLiteralUtil.escapeSQLString(input);
        // Should be properly escaped
        Assert.assertNotNull(result);
        Assert.assertTrue(result.startsWith("'"));
        Assert.assertTrue(result.endsWith("'"));
    }
}

