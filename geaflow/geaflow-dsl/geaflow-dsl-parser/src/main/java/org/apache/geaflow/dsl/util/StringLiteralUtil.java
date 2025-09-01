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

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;

public class StringLiteralUtil {

    private static final int[] multiplier = new int[]{1000, 100, 10, 1};

    public static String unescapeSQLString(String b) {

        Character enclosure = null;

        // Some of the strings can be passed in as unicode. For example, the
        // delimiter can be passed in as \002 - So, we first check if the
        // string is a unicode number, else go back to the old behavior
        StringBuilder sb = new StringBuilder(b.length());
        for (int i = 0; i < b.length(); i++) {

            char currentChar = b.charAt(i);
            if (enclosure == null) {
                if (currentChar == '\'' || currentChar == '\"') {
                    enclosure = currentChar;
                }
                // ignore all other chars outside the enclosure
                continue;
            }

            if (enclosure.equals(currentChar)) {
                enclosure = null;
                continue;
            }


            if (currentChar == '\\' && (i + 6 < b.length()) && b.charAt(i + 1) == 'u') {
                int code = 0;
                int base = i + 2;
                for (int j = 0; j < 4; j++) {
                    int digit = Character.digit(b.charAt(j + base), 16);
                    code += digit * multiplier[j];
                }
                sb.append((char) code);
                i += 5;
                continue;
            } else if (currentChar == '\\') { // process case for '\001'
                int code = 0;
                int base = i + 1;
                int j;

                for (j = 0; j < 3; j++) {
                    char c = b.charAt(j + base);
                    if (c >= '0' && c <= '9') {
                        if (code * 10 + (c - '0') < 128) {
                            code = code * 10 + (c - '0');
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                if (j > 0) {
                    sb.append((char) code);
                    i += j;
                    continue;
                }

            }


            if (currentChar == '\\' && (i + 4 < b.length())) {
                char i1 = b.charAt(i + 1);
                char i2 = b.charAt(i + 2);
                char i3 = b.charAt(i + 3);
                if ((i1 >= '0' && i1 <= '1') && (i2 >= '0' && i2 <= '7')
                    && (i3 >= '0' && i3 <= '7')) {
                    byte bVal = (byte) ((i3 - '0') + ((i2 - '0') * 8) + ((i1 - '0') * 8 * 8));
                    byte[] bValArr = new byte[1];
                    bValArr[0] = bVal;
                    String tmp = new String(bValArr);
                    sb.append(tmp);
                    i += 3;
                    continue;
                }
            }

            if (currentChar == '\\' && (i + 2 < b.length())) {
                char n = b.charAt(i + 1);
                switch (n) {
                    case '0':
                        sb.append("\0");
                        break;
                    case '\'':
                        sb.append("'");
                        break;
                    case '"':
                        sb.append("\"");
                        break;
                    case 'b':
                        sb.append("\b");
                        break;
                    case 'n':
                        sb.append("\n");
                        break;
                    case 'r':
                        sb.append("\r");
                        break;
                    case 't':
                        sb.append("\t");
                        break;
                    case 'Z':
                        sb.append("\u001A");
                        break;
                    case '\\':
                        sb.append("\\");
                        break;
                    // The following 2 lines are exactly what MySQL does TODO: why do we do this?
                    case '%':
                        sb.append("\\%");
                        break;
                    case '_':
                        sb.append("\\_");
                        break;
                    default:
                        sb.append(n);
                }
                i++;
            } else {
                sb.append(currentChar);
            }
        }
        return sb.toString();
    }

    /**
     * Convert Java strings to sql strings.
     */
    public static String escapeSQLString(String b) {
        // There's usually nothing to escape so we will be optimistic.
        String result = b;
        for (int i = 0; i < result.length(); ++i) {
            char currentChar = result.charAt(i);
            if (currentChar == '\\' && ((i + 1) < result.length())) {
                // TODO: do we need to handle the "this is what MySQL does" here?
                char nextChar = result.charAt(i + 1);
                if (nextChar == '%' || nextChar == '_') {
                    ++i;
                    continue;
                }
            }
            switch (currentChar) {
                case '\0':
                    result = spliceString(result, i, "\\0");
                    ++i;
                    break;
                case '\'':
                    result = spliceString(result, i, "\\'");
                    ++i;
                    break;
                case '\"':
                    result = spliceString(result, i, "\\\"");
                    ++i;
                    break;
                case '\b':
                    result = spliceString(result, i, "\\b");
                    ++i;
                    break;
                case '\n':
                    result = spliceString(result, i, "\\n");
                    ++i;
                    break;
                case '\r':
                    result = spliceString(result, i, "\\r");
                    ++i;
                    break;
                case '\t':
                    result = spliceString(result, i, "\\t");
                    ++i;
                    break;
                case '\\':
                    result = spliceString(result, i, "\\\\");
                    ++i;
                    break;
                case '\u001A':
                    result = spliceString(result, i, "\\Z");
                    ++i;
                    break;
                default: {
                    if (currentChar < ' ') {
                        String hex = Integer.toHexString(currentChar);
                        String unicode = "\\u";
                        for (int j = 4; j > hex.length(); --j) {
                            unicode += '0';
                        }
                        unicode += hex;
                        result = spliceString(result, i, unicode);
                        i += (unicode.length() - 1);
                    }
                    break; // if not a control character, do nothing
                }
            }
        }

        return "'" + result + "'";
    }

    private static String spliceString(String str, int i, String replacement) {
        return spliceString(str, i, 1, replacement);
    }

    private static String spliceString(String str, int i, int length, String replacement) {
        return str.substring(0, i) + replacement + str.substring(i + length);
    }


    public static String toJavaString(SqlNode node) {
        if (node == null) {
            return null;
        }
        if (node instanceof SqlCharStringLiteral) {
            SqlCharStringLiteral literal = (SqlCharStringLiteral) node;
            return unescapeSQLString("\"" + literal.getNlsString().getValue() + "\"");
        }
        return node.toString();
    }

}
