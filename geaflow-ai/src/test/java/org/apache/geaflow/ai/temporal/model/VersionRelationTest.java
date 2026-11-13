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

public class VersionRelationTest {

    @Test
    public void testDirectionalRelationValueSemantics() {
        VersionRelation relation = new VersionRelation(
            VersionRelationType.SUPERSEDES,
            "version-2",
            "version-1");
        VersionRelation same = new VersionRelation(
            VersionRelationType.SUPERSEDES,
            "version-2",
            "version-1");

        Assertions.assertEquals(
            VersionRelationType.SUPERSEDES,
            relation.getType());
        Assertions.assertEquals(
            "version-2",
            relation.getFromVersionId());
        Assertions.assertEquals(
            "version-1",
            relation.getToVersionId());
        Assertions.assertEquals(relation, same);
        Assertions.assertEquals(
            relation.hashCode(),
            same.hashCode());
    }

    @Test
    public void testConflictUsesCanonicalDirection() {
        VersionRelation relation = new VersionRelation(
            VersionRelationType.CONFLICTS_WITH,
            "version-b",
            "version-a");
        VersionRelation canonical = new VersionRelation(
            VersionRelationType.CONFLICTS_WITH,
            "version-a",
            "version-b");

        Assertions.assertEquals(
            "version-a",
            relation.getFromVersionId());
        Assertions.assertEquals(
            "version-b",
            relation.getToVersionId());
        Assertions.assertEquals(canonical, relation);
    }

    @Test
    public void testDeterministicOrder() {
        VersionRelation supersedes = new VersionRelation(
            VersionRelationType.SUPERSEDES,
            "version-2",
            "version-1");
        VersionRelation duplicate = new VersionRelation(
            VersionRelationType.DUPLICATE_OF,
            "version-3",
            "version-1");
        VersionRelation conflict = new VersionRelation(
            VersionRelationType.CONFLICTS_WITH,
            "version-2",
            "version-3");
        List<VersionRelation> relations =
            new ArrayList<>(Arrays.asList(
                conflict,
                duplicate,
                supersedes));

        Collections.sort(relations);

        Assertions.assertEquals(
            Arrays.asList(supersedes, duplicate, conflict),
            relations);
    }

    @Test
    public void testRejectInvalidRelation() {
        Assertions.assertThrows(
            NullPointerException.class,
            () -> new VersionRelation(
                null,
                "version-2",
                "version-1"));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> new VersionRelation(
                VersionRelationType.SUPERSEDES,
                null,
                "version-1"));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new VersionRelation(
                VersionRelationType.SUPERSEDES,
                " ",
                "version-1"));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> new VersionRelation(
                VersionRelationType.SUPERSEDES,
                "version-2",
                null));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new VersionRelation(
                VersionRelationType.SUPERSEDES,
                "version-2",
                " "));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new VersionRelation(
                VersionRelationType.SUPERSEDES,
                "version-1",
                "version-1"));
    }
}
