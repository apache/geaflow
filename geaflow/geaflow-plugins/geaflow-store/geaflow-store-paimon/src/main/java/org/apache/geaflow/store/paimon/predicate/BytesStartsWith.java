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

package org.apache.geaflow.store.paimon.predicate;

import java.util.List;
import java.util.Optional;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FunctionVisitor;
import org.apache.paimon.predicate.LeafFunction;
import org.apache.paimon.predicate.NullFalseLeafBinaryFunction;
import org.apache.paimon.types.DataType;

public class BytesStartsWith extends NullFalseLeafBinaryFunction {

    public static final BytesStartsWith INSTANCE = new BytesStartsWith();

    private BytesStartsWith() {
    }

    @Override
    public boolean test(DataType type, Object field, Object patternLiteral) {
        return startsWith((byte[]) field, (byte[]) patternLiteral);
    }

    @Override
    public boolean test(DataType type, long rowCount, Object min, Object max, Long nullCount,
                        Object patternLiteral) {
        byte[] minBytes = (byte[]) min;
        byte[] maxBytes = (byte[]) max;
        byte[] pattern = (byte[]) patternLiteral;
        return (startsWith(minBytes, pattern) || compareTo(minBytes, pattern) <= 0) && (
            startsWith(maxBytes, pattern) || compareTo(maxBytes, pattern) >= 0);
    }

    @Override
    public Optional<LeafFunction> negate() {
        return Optional.empty();
    }

    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        return visitor.visitStartsWith(fieldRef, literals.get(0));
    }

    /**
     * Judge whether the field starts with the pattern.
     *
     * @param field   the field to be compared
     * @param pattern the pattern to be compared
     * @return true if the field starts with the pattern, false otherwise
     */
    private static boolean startsWith(byte[] field, byte[] pattern) {
        if (field.length < pattern.length) {
            return false;
        }
        MemorySegment s1 = MemorySegment.wrap(field);
        MemorySegment s2 = MemorySegment.wrap(pattern);

        return s1.equalTo(s2, 0, 0, pattern.length);
    }

    private static int compareTo(byte[] lhs, byte[] rhs) {
        int len = Math.min(lhs.length, rhs.length);

        for (int i = 0; i < len; i++) {
            int res = Byte.compare(lhs[i], rhs[i]);
            if (res != 0) {
                return res;
            }
        }
        return lhs.length - rhs.length;
    }

}
