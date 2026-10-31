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

    // Characters that carry no meaning on their own: digits and a few common symbols.
    // A value made up entirely of these is not worth indexing or embedding.
    private static final Set<Character> IGNORABLE_CHARS = buildIgnorableChars();

    /**
     * Builds the set of characters that carry no meaning on their own.
     * Digits and a few common symbols.
     *
     * @return an unmodifiable set of ignorable characters
     */
    private static Set<Character> buildIgnorableChars() {
        Set<Character> ignored = new HashSet<>(32);
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
     * Whether the given value consists entirely of characters that carry no meaning on their own,
     * and therefore has nothing worth indexing or embedding. A bare id, a date or a run of
     * punctuation is ignorable; anything containing a letter or a CJK character is not.
     *
     * <p>Callers use this to skip values, so an empty or absent value is ignorable too: there is
     * nothing in it to index.
     *
     * <p>This replaces {@code isAllAllowedChars}, whose loop returned on the first character
     * <em>inside</em> the set rather than the first one outside it, making it the negation of both
     * its own name and its own documentation. The practical effect was that ordinary prose was
     * discarded while digit-only noise was kept, so an embedding store silently produced nothing
     * for text that happened to contain no digit. The method is renamed rather than corrected in
     * place, so that any caller depending on the previous meaning fails to compile instead of
     * silently flipping behaviour.
     *
     * @param str the value to check
     * @return true if every character is ignorable, or the value is null or empty
     */
    public static boolean isAllIgnorableChars(String str) {
        if (str == null || str.isEmpty()) {
            return true;
        }
        for (char c : str.toCharArray()) {
            if (!IGNORABLE_CHARS.contains(c)) {
                return false;
            }
        }
        return true;
    }

}
