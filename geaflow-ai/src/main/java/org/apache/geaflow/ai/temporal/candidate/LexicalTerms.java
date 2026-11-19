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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** A fixed, label-independent alias table for retrieval copies only. */
public final class LexicalTerms {

    private static final Map<String, String> ALIASES;
    private static final Map<String, String> CANONICAL_ALIASES;
    private static final Pattern ALIAS_PATTERN;

    static {
        Map<String, String> aliases = new LinkedHashMap<>();
        aliases.put("co2", "carbon dioxide");
        aliases.put("co 2", "carbon dioxide");
        aliases.put("co₂", "carbon dioxide");
        aliases.put("carbon dioxide", "carbon dioxide");
        aliases.put("ch4", "methane");
        aliases.put("methane", "methane");
        aliases.put("n2o", "nitrous oxide");
        aliases.put("nitrous oxide", "nitrous oxide");
        ALIASES = Collections.unmodifiableMap(aliases);
        Map<String, String> canonicalAliases = new LinkedHashMap<>();
        for (Map.Entry<String, String> alias : aliases.entrySet()) {
            canonicalAliases.put(normalize(alias.getKey()), alias.getValue());
        }
        CANONICAL_ALIASES = Collections.unmodifiableMap(canonicalAliases);
        StringBuilder alternatives = new StringBuilder();
        for (String alias : canonicalAliases.keySet()) {
            if (alternatives.length() > 0) {
                alternatives.append('|');
            }
            alternatives.append(Pattern.quote(alias));
        }
        String boundary = "[\\p{L}\\p{M}\\p{N}_]";
        ALIAS_PATTERN = Pattern.compile("(?<!" + boundary + ")(?:" + alternatives
            + ")(?!" + boundary + ")");
    }

    private LexicalTerms() {
    }

    public static String canonicalize(String text) {
        Matcher matcher = ALIAS_PATTERN.matcher(normalize(text));
        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(result,
                Matcher.quoteReplacement(CANONICAL_ALIASES.get(matcher.group())));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    public static String getVersion() {
        return "gas-aliases-v1";
    }

    public static Map<String, String> getAliases() {
        return ALIASES;
    }

    private static String normalize(String text) {
        return TextNormalizer.normalize(Normalizer.normalize(Objects.requireNonNull(text, "text"),
            Normalizer.Form.NFKC));
    }
}
