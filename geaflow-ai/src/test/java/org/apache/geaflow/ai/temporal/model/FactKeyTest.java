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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FactKeyTest {

    @Test
    public void testValueSemanticsAndDeterministicOrder() {
        FactKey key = new FactKey(
            "person:alice",
            "city",
            "profile");
        FactKey same = new FactKey(
            "person:alice",
            "city",
            "profile");

        Assertions.assertEquals("person:alice", key.getSubjectId());
        Assertions.assertEquals("city", key.getPredicate());
        Assertions.assertEquals("profile", key.getScope());
        Assertions.assertEquals(key, same);
        Assertions.assertEquals(key.hashCode(), same.hashCode());

        FactKey earlierPredicate = new FactKey(
            "person:alice",
            "age",
            "profile");
        FactKey earlierScope = new FactKey(
            "person:alice",
            "city",
            "account");
        FactKey laterSubject = new FactKey(
            "person:bob",
            "age",
            "profile");
        List<FactKey> keys = new ArrayList<>(Arrays.asList(
            laterSubject,
            key,
            earlierScope,
            earlierPredicate));

        Collections.sort(keys);

        Assertions.assertEquals(
            Arrays.asList(
                earlierPredicate,
                earlierScope,
                key,
                laterSubject),
            keys);
    }

    @Test
    public void testRejectInvalidKey() {
        Assertions.assertThrows(
            NullPointerException.class,
            () -> new FactKey(null, "city", "profile"));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new FactKey(" ", "city", "profile"));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> new FactKey("person:alice", null, "profile"));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new FactKey("person:alice", " ", "profile"));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> new FactKey("person:alice", "city", null));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new FactKey("person:alice", "city", " "));
    }
}
