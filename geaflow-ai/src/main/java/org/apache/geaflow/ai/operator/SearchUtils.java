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

package org.apache.geaflow.ai.operator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class SearchUtils {

    // Set of excluded characters: these will be replaced with spaces in formatQuery
    private static final Set<Character> EXCLUDED_CHARS = new HashSet<>(Arrays.asList(
            '*', '#', '-', '?', '`', '{', '}', '[', ']', '(', ')', '>', '<', ':', '/', '.'
    ));

    // Set of allowed characters for validation in isAllAllowedChars
    // Includes: digits (0-9), and some common safe symbols
    private static final Set<Character> IGNORE_CHARS = buildIgnoredChars();

    /**
     * Builds the set of allowed characters for input validation.
     * Includes alphanumeric characters and selected common symbols.
     *
     * @return an unmodifiable set of ignored characters
     */
    private static Set<Character> buildIgnoredChars() {
        Set<Character> ignored = new HashSet<>(EXCLUDED_CHARS);
        // Add digits
        for (char c = '0'; c <= '9'; c++) {
            ignored.add(c);
        }
        // Add commonly allowed symbols
        ignored.add('.');
        ignored.add('_');
        ignored.add('-');
        ignored.add('@');
        ignored.add('+');
        ignored.add('!');
        ignored.add('$');
        ignored.add('%');
        ignored.add('&');
        ignored.add('=');
        ignored.add('~');
        return Collections.unmodifiableSet(ignored);
    }

    /**
     * Formats the input query string by replacing each excluded character with a space.
     * This helps sanitize search queries for parsing or indexing.
     *
     * @param query the input string to format
     * @return the formatted string with excluded characters replaced by spaces
     */
    public static String formatQuery(String query) {
        if (query == null || query.isEmpty()) {
            return query;
        }
        StringBuilder result = new StringBuilder();
        for (char c : query.toCharArray()) {
            if (EXCLUDED_CHARS.contains(c)) {
                result.append(' ');
            } else {
                result.append(c);
            }
        }
        String replacedQuery = result.toString();
        replacedQuery = replacedQuery.replace("http", "");
        return replacedQuery;
    }

    /**
     * Checks whether all characters in the given string are within the allowed character set.
     * Useful for validating usernames, identifiers, or safe input formats.
     *
     * @param str the string to validate
     * @return true if all characters are allowed; false otherwise
     */
    public static boolean isAllAllowedChars(String str) {
        if (str == null || str.isEmpty()) {
            return false; // Consider empty/null invalid; adjust based on use case
        }
        for (char c : str.toCharArray()) {
            if (!IGNORE_CHARS.contains(c)) {
                return false;
            }
        }
        return true;
    }

}
