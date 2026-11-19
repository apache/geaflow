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

package org.apache.geaflow.ai.provenance;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SourceSpanTest {

    @Test
    public void testValidSpanPassesValidation() {
        SourceSpan span = new SourceSpan("chunk-000012", 48, 132);
        Assertions.assertDoesNotThrow(() -> span.validate());
        Assertions.assertEquals("chunk-000012", span.getChunkId());
        Assertions.assertEquals(48, span.getStartOffset());
        Assertions.assertEquals(132, span.getEndOffset());
    }

    @Test
    public void testEmptySpanIsValid() {
        // A zero-length span (e.g. an insertion point) is still a valid range.
        Assertions.assertDoesNotThrow(() -> SourceSpan.of("chunk-1", 10, 10));
    }

    @Test
    public void testMissingChunkIdFails() {
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> SourceSpan.of(null, 0, 10));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> SourceSpan.of("  ", 0, 10));
    }

    @Test
    public void testNegativeStartOffsetFails() {
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> SourceSpan.of("chunk-1", -1, 10));
    }

    @Test
    public void testEndBeforeStartFails() {
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> SourceSpan.of("chunk-1", 20, 10));
    }

    @Test
    public void testEqualsAndHashCode() {
        SourceSpan first = new SourceSpan("chunk-1", 0, 10);
        SourceSpan second = new SourceSpan("chunk-1", 0, 10);
        SourceSpan other = new SourceSpan("chunk-2", 0, 10);
        Assertions.assertEquals(first, second);
        Assertions.assertEquals(first.hashCode(), second.hashCode());
        Assertions.assertNotEquals(first, other);
    }
}
