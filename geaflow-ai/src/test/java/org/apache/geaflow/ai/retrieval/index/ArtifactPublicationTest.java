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

package org.apache.geaflow.ai.retrieval.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.index.bm25.Bm25IndexReader;
import org.apache.geaflow.ai.retrieval.index.bm25.LuceneBm25IndexBuilder;
import org.apache.geaflow.ai.retrieval.index.graph.GraphArtifactReader;
import org.apache.geaflow.ai.retrieval.index.graph.OfflineGraphArtifactBuilder;
import org.apache.geaflow.ai.retrieval.index.vector.OfflineVectorIndexBuilder;
import org.apache.geaflow.ai.retrieval.index.vector.VectorIndexReader;
import org.apache.geaflow.ai.retrieval.ingest.IngestionContext;
import org.apache.geaflow.ai.retrieval.metadata.ChunkingConfiguration;
import org.apache.geaflow.ai.retrieval.metadata.DatasetManifest;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ArtifactPublicationTest {
    @TempDir
    Path directory;

    @Test
    void concurrentEquivalentBuildersReuseWinnerForEveryArtifactType() throws Exception {
        for (String kind : new String[] {"vector", "graph", "bm25"}) {
            Path output = directory.resolve(kind);
            runRace(kind, output, false);
            build(kind, output, 0);
            validateReadable(kind, output);
            assertNoStaging(output);
        }
    }

    @Test
    void concurrentConflictingBuildersCannotOverwriteWinnerForEveryArtifactType() throws Exception {
        for (String kind : new String[] {"vector", "graph", "bm25"}) {
            Path output = directory.resolve(kind);
            int winner = runRace(kind, output, true);
            build(kind, output, winner);
            assertThrows(Exception.class, () -> build(kind, output, 1 - winner));
            validateReadable(kind, output);
            assertNoStaging(output);
        }
    }

    @Test
    void separateJvmsSerializePublicationAndValidateTheWinner() throws Exception {
        Path output = Files.createDirectory(directory.resolve("processes"));
        String java = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process first = new ProcessBuilder(java, "-cp", classpath, PublisherProcess.class.getName(),
            output.toString(), "0").redirectErrorStream(true).redirectOutput(output.resolve("first.log").toFile()).start();
        Process second = new ProcessBuilder(java, "-cp", classpath, PublisherProcess.class.getName(),
            output.toString(), "1").redirectErrorStream(true).redirectOutput(output.resolve("second.log").toFile()).start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!Files.exists(output.resolve("ready-0")) || !Files.exists(output.resolve("ready-1"))) {
                if (System.nanoTime() > deadline) {
                    throw new TimeoutException("publisher processes did not start");
                }
                Thread.sleep(10);
            }
            Files.createFile(output.resolve("start"));
            org.junit.jupiter.api.Assertions.assertTrue(first.waitFor(10, TimeUnit.SECONDS));
            org.junit.jupiter.api.Assertions.assertTrue(second.waitFor(10, TimeUnit.SECONDS));
            assertEquals(1, (first.exitValue() == 0 ? 1 : 0) + (second.exitValue() == 0 ? 1 : 0));
            assertEquals(first.exitValue() == 0 ? "0" : "1",
                new String(Files.readAllBytes(output.resolve("artifact")), StandardCharsets.UTF_8));
        } finally {
            first.destroyForcibly();
            second.destroyForcibly();
        }
    }

    public static final class PublisherProcess {
        public static void main(String[] args) throws Exception {
            Path output = Paths.get(args[0]);
            byte[] payload = args[1].getBytes(StandardCharsets.UTF_8);
            Path staging = Files.createTempFile(output, "artifact", ".tmp");
            Files.write(staging, payload);
            Files.createFile(output.resolve("ready-" + args[1]));
            try {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (!Files.exists(output.resolve("start"))) {
                    if (System.nanoTime() > deadline) {
                        throw new TimeoutException("publisher process start timed out");
                    }
                    Thread.sleep(10);
                }
                ArtifactPublisher.publish(staging, output.resolve("artifact"), existing -> {
                    if (!Arrays.equals(payload, Files.readAllBytes(existing))) {
                        throw new IOException("artifact conflict");
                    }
                });
            } finally {
                Files.deleteIfExists(staging);
            }
        }
    }

    private int runRace(String kind, Path output, boolean conflicting) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CyclicBarrier start = new CyclicBarrier(2);
        try {
            Future<Boolean> first = executor.submit(() -> publish(kind, output, 0, start));
            Future<Boolean> second = executor.submit(() -> publish(kind, output, conflicting ? 1 : 0, start));
            boolean firstSuccess = first.get(10, TimeUnit.SECONDS);
            boolean secondSuccess = second.get(10, TimeUnit.SECONDS);
            assertEquals(conflicting ? 1 : 2, (firstSuccess ? 1 : 0) + (secondSuccess ? 1 : 0));
            return firstSuccess ? 0 : 1;
        } finally {
            executor.shutdownNow();
        }
    }

    private boolean publish(String kind, Path output, int variant, CyclicBarrier start) throws Exception {
        start.await(5, TimeUnit.SECONDS);
        try {
            build(kind, output, variant);
            return true;
        } catch (IOException | RetrievalException conflict) {
            return false;
        }
    }

    private void build(String kind, Path output, int variant) throws Exception {
        List<TextChunk> chunks = Collections.singletonList(new TextChunk("chunk", "doc", 0, 0, 10, 3,
            "graph evidence " + ("bm25".equals(kind) ? variant : 0)).withSourceUri("fixture://doc"));
        if ("vector".equals(kind)) {
            try (IndexArtifact ignored = new OfflineVectorIndexBuilder(output,
                Collections.singletonMap("chunk", variant == 0 ? new float[] {1, 0} : new float[] {0, 1}),
                "fixture", "v1").build(context(), chunks)) {
                assertEquals("vector", ignored.getMetadata().getIndexType());
            }
        } else if ("graph".equals(kind)) {
            try (org.apache.geaflow.ai.retrieval.ingest.GraphArtifact ignored =
                new OfflineGraphArtifactBuilder(output).build(context(), chunks,
                    Collections.singletonList(new EntityRef("entity", "name " + variant,
                        Collections.emptyList(), "topic", Collections.singletonList("chunk"))),
                    Collections.emptyList())) {
                assertEquals("v1", ignored.getMetadata().getGraphVersion().getVersion());
            }
        } else {
            try (IndexArtifact ignored = new LuceneBm25IndexBuilder(output).build(context(), chunks)) {
                assertEquals("bm25", ignored.getMetadata().getIndexType());
            }
        }
    }

    private void validateReadable(String kind, Path output) throws Exception {
        if ("vector".equals(kind)) {
            try (VectorIndexReader ignored = new VectorIndexReader(output.resolve("vector-v1.bin"))) {
                assertEquals("v1", ignored.getIdentity().toMap().get("graphVersion"));
            }
        } else if ("graph".equals(kind)) {
            try (GraphArtifactReader reader = new GraphArtifactReader(output.resolve("graph-v1.bin"), "v1", "v1")) {
                assertEquals(1, reader.getEntities().size());
            }
        } else {
            try (Bm25IndexReader ignored = new Bm25IndexReader(output.resolve("bm25-v1"))) {
                assertEquals("v1", ignored.getIdentity().toMap().get("graphVersion"));
            }
        }
    }

    private void assertNoStaging(Path output) throws IOException {
        try (java.util.stream.Stream<Path> paths = Files.list(output)) {
            assertFalse(paths.anyMatch(path -> path.getFileName().toString().endsWith(".tmp")
                || path.getFileName().toString().startsWith("bm25-v1-")));
        }
    }

    private IngestionContext context() {
        DatasetManifest manifest = new DatasetManifest("v1", "dataset", "release", "dev", null,
            "cache", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "parser", new ChunkingConfiguration("chunk-v1", 100, 10), "graph-v1", "fixture", "v1", 1L);
        return new IngestionContext(manifest, new GraphVersion("graph", "v1"), "test");
    }
}
