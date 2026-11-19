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

package org.apache.geaflow.ai.temporal.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MemoryEntityTest {

    @Test
    public void testValueSemantics() {
        MemoryEntity entity =
            new MemoryEntity("person:alice", "person");
        MemoryEntity same =
            new MemoryEntity("person:alice", "person");

        Assertions.assertEquals("person:alice", entity.getId());
        Assertions.assertEquals("person", entity.getLabel());
        Assertions.assertEquals(entity, same);
        Assertions.assertEquals(entity.hashCode(), same.hashCode());

        Assertions.assertNotEquals(
            entity,
            new MemoryEntity("person:bob", "person"));
        Assertions.assertNotEquals(
            entity,
            new MemoryEntity("person:alice", "company"));
    }

    @Test
    public void testRejectInvalidEntity() {
        Assertions.assertThrows(
            NullPointerException.class,
            () -> new MemoryEntity(null, "person"));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new MemoryEntity(" ", "person"));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> new MemoryEntity("person:alice", null));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new MemoryEntity("person:alice", " "));
    }
}
