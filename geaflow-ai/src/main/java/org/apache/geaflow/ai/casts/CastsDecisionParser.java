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

package org.apache.geaflow.ai.casts;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CastsDecisionParser {

    public enum Kind {
        OUT,
        IN,
        BOTH,
        OUT_E,
        IN_E,
        BOTH_E,
        IN_V,
        OUT_V,
        OTHER_V,
        DEDUP,
        SIMPLE_PATH,
        LIMIT,
        ORDER_BY,
        HAS,
        STOP,
        UNKNOWN
    }

    public static class ParsedDecision {
        public final Kind kind;
        public final String label;
        public final String raw;

        public ParsedDecision(Kind kind, String label, String raw) {
            this.kind = kind;
            this.label = label;
            this.raw = raw;
        }
    }

    private static final Pattern STEP_WITH_LABEL =
        Pattern.compile("^(out|in|both|outE|inE|bothE)\\('([^']+)'\\)$");

    public static ParsedDecision parse(String decision) {
        if (decision == null) {
            return new ParsedDecision(Kind.UNKNOWN, null, null);
        }
        String d = decision.trim();
        if (d.isEmpty() || "stop".equals(d)) {
            return new ParsedDecision(Kind.STOP, null, d);
        }

        Matcher m = STEP_WITH_LABEL.matcher(d);
        if (m.matches()) {
            String op = m.group(1);
            String label = m.group(2);
            if ("out".equals(op)) {
                return new ParsedDecision(Kind.OUT, label, d);
            } else if ("in".equals(op)) {
                return new ParsedDecision(Kind.IN, label, d);
            } else if ("both".equals(op)) {
                return new ParsedDecision(Kind.BOTH, label, d);
            } else if ("outE".equals(op)) {
                return new ParsedDecision(Kind.OUT_E, label, d);
            } else if ("inE".equals(op)) {
                return new ParsedDecision(Kind.IN_E, label, d);
            } else if ("bothE".equals(op)) {
                return new ParsedDecision(Kind.BOTH_E, label, d);
            }
        }

        if ("inV()".equals(d)) {
            return new ParsedDecision(Kind.IN_V, null, d);
        }
        if ("outV()".equals(d)) {
            return new ParsedDecision(Kind.OUT_V, null, d);
        }
        if ("otherV()".equals(d)) {
            return new ParsedDecision(Kind.OTHER_V, null, d);
        }
        if ("dedup()".equals(d)) {
            return new ParsedDecision(Kind.DEDUP, null, d);
        }
        if ("simplePath()".equals(d)) {
            return new ParsedDecision(Kind.SIMPLE_PATH, null, d);
        }
        if (d.startsWith("limit(") && d.endsWith(")")) {
            return new ParsedDecision(Kind.LIMIT, null, d);
        }
        if (d.startsWith("order().by(") && d.endsWith(")")) {
            return new ParsedDecision(Kind.ORDER_BY, null, d);
        }
        if (d.startsWith("has(") && d.endsWith(")")) {
            return new ParsedDecision(Kind.HAS, null, d);
        }

        return new ParsedDecision(Kind.UNKNOWN, null, d);
    }
}

