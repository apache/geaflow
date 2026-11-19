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

package org.apache.geaflow.context.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.apache.geaflow.context.api.model.Episode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for RocksDB storage.
 */
public class RocksDBStoreTest {

    private String testPath;
    private RocksDBStore store;

    @Before
    public void setUp() throws Exception {
        testPath = "/tmp/test_rocksdb_" + System.currentTimeMillis();
        store = new RocksDBStore(testPath);
    }

    @After
    public void tearDown() throws Exception {
        if (store != null) {
            store.close();
        }
        // Clean up test directory
        Files.walk(Paths.get(testPath))
            .sorted((a, b) -> b.compareTo(a))
            .forEach(path -> {
                try {
                    Files.delete(path);
                } catch (Exception e) {
                    // Ignore
                }
            });
    }

    @Test
    public void testEpisodeStorage() throws Exception {
        Episode episode = new Episode("ep_001", "Test Episode",
            System.currentTimeMillis(), "Test content");

        store.addEpisode(episode);

        Episode retrieved = store.getEpisode("ep_001");
        assertNotNull(retrieved);
        assertEquals("ep_001", retrieved.getEpisodeId());
        assertEquals("Test Episode", retrieved.getName());
    }

    @Test
    public void testEntityStorage() throws Exception {
        Episode.Entity entity = new Episode.Entity("e1", "Entity One", "Person");

        store.addEntity("e1", entity);

        Episode.Entity retrieved = store.getEntity("e1");
        assertNotNull(retrieved);
        assertEquals("e1", retrieved.getId());
        assertEquals("Entity One", retrieved.getName());
    }

    @Test
    public void testRelationStorage() throws Exception {
        Episode.Relation relation = new Episode.Relation("e1", "e2", "knows");

        store.addRelation("e1->e2", relation);

        Episode.Relation retrieved = store.getRelation("e1->e2");
        assertNotNull(retrieved);
        assertEquals("e1", retrieved.getSourceId());
        assertEquals("e2", retrieved.getTargetId());
    }

    @Test
    public void testNotFound() throws Exception {
        Episode retrieved = store.getEpisode("nonexistent");
        assertNull(retrieved);
    }

    @Test
    public void testMultipleWrites() throws Exception {
        for (int i = 0; i < 10; i++) {
            Episode episode = new Episode("ep_" + i, "Episode " + i,
                System.currentTimeMillis(), "Content " + i);
            store.addEpisode(episode);
        }

        // Verify all can be retrieved
        for (int i = 0; i < 10; i++) {
            Episode retrieved = store.getEpisode("ep_" + i);
            assertNotNull(retrieved);
            assertEquals("ep_" + i, retrieved.getEpisodeId());
        }
    }
}
