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

package org.apache.geaflow.dsl.udf.table.string;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDF;

/**
 * This class is an adaptation of Hive's org.apache.hadoop.hive.ql.udf.UDFLike.
 */
@Description(name = "like", description = "Returns whether string matches to the pattern.")
public class Like extends UDF {

    private PatternType type = PatternType.NONE;
    private String simplePattern;
    private String lastLikePattern;
    private Pattern p = null;

    public static String likePatternToRegExp(String likePattern) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < likePattern.length(); i++) {
            char n = likePattern.charAt(i);
            if (n == '\\' && i + 1 < likePattern.length() && (likePattern.charAt(i + 1) == '_'
                || likePattern.charAt(i + 1) == '%')) {
                sb.append(likePattern.charAt(i + 1));
                i++;
                continue;
            }
            if (n == '_') {
                sb.append(".");
            } else if (n == '%') {
                sb.append(".*");
            } else {
                sb.append(Pattern.quote(Character.toString(n)));
            }
        }
        return sb.toString();
    }

    private static boolean find(String s, String sub, int startS, int endS) {
        byte[] byteS = s.getBytes();
        byte[] byteSub = sub.getBytes();
        int lenSub = byteSub.length;
        boolean match = false;
        for (int i = startS; (i < endS - lenSub + 1) && (!match); i++) {
            match = true;
            for (int j = 0; j < lenSub; j++) {
                if (byteS[j + i] != byteSub[j]) {
                    match = false;
                    break;
                }
            }
        }
        return match;
    }

    private void parseSimplePattern(String likePattern) {
        int length = likePattern.length();
        int beginIndex = 0;
        int endIndex = length;
        char lastChar = 'a';
        String strPattern = "";
        type = PatternType.NONE;

        for (int i = 0; i < length; i++) {
            char n = likePattern.charAt(i);
            if (n == '_') { // such as "a_b"
                if (lastChar != '\\') { // such as "a%bc"
                    type = PatternType.COMPLEX;
                    return;
                } else { // such as "abc\%de%"
                    strPattern += likePattern.substring(beginIndex, i - 1);
                    beginIndex = i;
                }
            } else if (n == '%') {
                if (i == 0) { // such as "%abc"
                    type = PatternType.END;
                    beginIndex = 1;
                } else if (i < length - 1) {
                    if (lastChar != '\\') { // such as "a%bc"
                        type = PatternType.COMPLEX;
                        return;
                    } else { // such as "abc\%de%"
                        strPattern += likePattern.substring(beginIndex, i - 1);
                        beginIndex = i;
                    }
                } else {
                    if (lastChar != '\\') {
                        endIndex = length - 1;
                        if (type == PatternType.END) { // such as "%abc%"
                            type = PatternType.MIDDLE;
                        } else {
                            type = PatternType.BEGIN; // such as "abc%"
                        }
                    } else { // such as "abc\%"
                        strPattern += likePattern.substring(beginIndex, i - 1);
                        beginIndex = i;
                        endIndex = length;
                    }
                }
            }
            lastChar = n;
        }

        strPattern += likePattern.substring(beginIndex, endIndex);
        simplePattern = strPattern;
    }

    public Boolean eval(String s, String likePattern) {
        if (s == null || likePattern == null) {
            return null;
        }
        if (!likePattern.equals(lastLikePattern)) {
            lastLikePattern = likePattern;
            String strLikePattern = likePattern;

            parseSimplePattern(strLikePattern);
            if (type == PatternType.COMPLEX) {
                p = Pattern.compile(likePatternToRegExp(strLikePattern), Pattern.DOTALL);
            }
        }

        if (type == PatternType.COMPLEX) {
            Matcher m = p.matcher(s);
            return m.matches();
        } else {
            int sLen = s.getBytes().length;
            int likeLen = simplePattern.getBytes().length;
            int startS = 0;
            int endS = sLen;
            // if s is shorter than the required pattern
            if (endS < likeLen) {
                return false;
            }
            switch (type) {
                case BEGIN:
                    endS = likeLen;
                    break;
                case END:
                    startS = endS - likeLen;
                    break;
                case NONE:
                    if (likeLen != sLen) {
                        return false;
                    }
                    break;
                default:
                    break;
            }
            return find(s, simplePattern, startS, endS);
        }
    }

    private enum PatternType {
        NONE,
        BEGIN,
        END,
        MIDDLE,
        COMPLEX
    }
}
