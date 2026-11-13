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

package org.apache.geaflow.ai.temporal.candidate;

import java.text.Normalizer;
import java.util.Locale;
import java.util.Objects;

/** Normalizes retrieval text without changing domain identities or stored values. */
public final class TextNormalizer {

    private TextNormalizer() {
    }

    public static String normalize(String text) {
        String canonical = Normalizer.normalize(Objects.requireNonNull(text, "text"),
            Normalizer.Form.NFC).toLowerCase(Locale.ROOT);
        StringBuilder result = new StringBuilder();
        boolean space = false;
        for (int offset = 0; offset < canonical.length();) {
            int point = canonical.codePointAt(offset);
            offset += Character.charCount(point);
            if (Character.isWhitespace(point) || Character.isSpaceChar(point)) {
                space = result.length() > 0;
            } else {
                if (space) {
                    result.append(' ');
                }
                result.appendCodePoint(point);
                space = false;
            }
        }
        return Normalizer.normalize(result.toString(), Normalizer.Form.NFC);
    }
}
