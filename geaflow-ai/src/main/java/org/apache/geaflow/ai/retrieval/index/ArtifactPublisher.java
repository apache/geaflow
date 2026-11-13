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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantLock;

/** Serializes artifact publication per target and never replaces an existing artifact. */
public final class ArtifactPublisher {

    private static final ReentrantLock[] LOCKS = createLocks();

    private ArtifactPublisher() {
    }

    /**
     * Publishes a completed staging path, or validates and reuses the winner if it already exists.
     *
     * @return true when this call moved the staging artifact into place
     */
    public static boolean publish(Path staging, Path target, ExistingArtifactValidator validator)
        throws IOException {
        // Canonicalize the parent so aliases of one output directory share the same JVM lock.
        Path normalizedTarget = target.toAbsolutePath().getParent().toRealPath()
            .resolve(target.getFileName());
        Path lockPath = normalizedTarget.resolveSibling(normalizedTarget.getFileName() + ".publish.lock");
        ReentrantLock processLock = LOCKS[(normalizedTarget.hashCode() & Integer.MAX_VALUE) % LOCKS.length];
        processLock.lock();
        try {
            try (FileChannel lockChannel = FileChannel.open(lockPath, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE); FileLock ignored = lockChannel.lock()) {
                if (Files.exists(normalizedTarget)) {
                    validator.validate(normalizedTarget);
                    return false;
                }
                try {
                    try {
                        Files.move(staging, normalizedTarget, StandardCopyOption.ATOMIC_MOVE);
                    } catch (java.nio.file.AtomicMoveNotSupportedException unsupported) {
                        Files.move(staging, normalizedTarget);
                    }
                    return true;
                } catch (FileAlreadyExistsException winnerPublished) {
                    validator.validate(normalizedTarget);
                    return false;
                }
            }
        } finally {
            processLock.unlock();
        }
    }

    private static ReentrantLock[] createLocks() {
        ReentrantLock[] locks = new ReentrantLock[64];
        for (int index = 0; index < locks.length; index++) {
            locks[index] = new ReentrantLock();
        }
        return locks;
    }

    /** Checks whether an existing target is equivalent to the artifact being built. */
    public interface ExistingArtifactValidator {
        void validate(Path existing) throws IOException;
    }
}
