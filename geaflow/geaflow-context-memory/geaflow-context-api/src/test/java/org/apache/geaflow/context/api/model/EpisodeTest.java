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

package org.apache.geaflow.context.api.model;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class EpisodeTest {

    private Episode episode;

    @Before
    public void setUp() {
        episode = new Episode();
    }

    @Test
    public void testEpisodeCreation() {
        String episodeId = "ep_001";
        String name = "Test Episode";
        long eventTime = System.currentTimeMillis();
        String content = "Test content";

        episode = new Episode(episodeId, name, eventTime, content);

        assertEquals(episodeId, episode.getEpisodeId());
        assertEquals(name, episode.getName());
        assertEquals(eventTime, episode.getEventTime());
        assertEquals(content, episode.getContent());
        assertTrue(episode.getIngestTime() > 0);
    }

    @Test
    public void testEntityCreation() {
        Episode.Entity entity = new Episode.Entity("e_001", "John", "Person");

        assertEquals("e_001", entity.getId());
        assertEquals("John", entity.getName());
        assertEquals("Person", entity.getType());
    }

    @Test
    public void testRelationCreation() {
        Episode.Relation relation = new Episode.Relation("e_001", "e_002", "knows");

        assertEquals("e_001", relation.getSourceId());
        assertEquals("e_002", relation.getTargetId());
        assertEquals("knows", relation.getRelationshipType());
    }

    @Test
    public void testEpisodeMetadata() {
        episode.getMetadata().put("key1", "value1");
        episode.getMetadata().put("key2", 123);

        assertEquals("value1", episode.getMetadata().get("key1"));
        assertEquals(123, episode.getMetadata().get("key2"));
    }
}
