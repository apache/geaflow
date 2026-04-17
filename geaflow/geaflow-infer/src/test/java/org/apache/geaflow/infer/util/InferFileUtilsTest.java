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

package org.apache.geaflow.infer.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.testng.annotations.Test;

public class InferFileUtilsTest {

    @Test
    public void testShouldExtractInferResourceRejectsTraversalPath() throws Exception {
        assertTrue(invokeShouldExtractInferResource("pkg/infer.py"));
        assertTrue(invokeShouldExtractInferResource("nested/model.pt"));
        assertTrue(invokeShouldExtractInferResource("pkg/sub/model.pt"));
        assertFalse(invokeShouldExtractInferResource("../evil.py"));
        assertFalse(invokeShouldExtractInferResource("..\\evil.py"));
        assertFalse(invokeShouldExtractInferResource("nested/../../evil.py"));
        assertFalse(invokeShouldExtractInferResource("nested/data.json"));
    }

    @Test
    public void testBuildSafeTargetFileKeepsNestedPathUnderTargetDirectory() throws Exception {
        Path targetDirectory = Files.createTempDirectory("infer-file-utils");
        try {
            File targetFile = invokeBuildSafeTargetFile(targetDirectory.toString(), "pkg/model.pt");
            assertEquals(targetFile.toPath(),
                targetDirectory.resolve("pkg/model.pt").toAbsolutePath().normalize());
        } finally {
            deleteRecursively(targetDirectory);
        }
    }

    @Test(expectedExceptions = IOException.class,
        expectedExceptionsMessageRegExp = "illegal infer resource path: .*")
    public void testBuildSafeTargetFileRejectsPathTraversal() throws Throwable {
        Path targetDirectory = Files.createTempDirectory("infer-file-utils");
        try {
            invokeBuildSafeTargetFile(targetDirectory.toString(), "../evil.py");
        } catch (InvocationTargetException e) {
            throw e.getCause();
        } finally {
            deleteRecursively(targetDirectory);
        }
    }

    @Test(expectedExceptions = IOException.class,
        expectedExceptionsMessageRegExp = "illegal infer resource path: .*")
    public void testBuildSafeTargetFileRejectsAbsolutePath() throws Throwable {
        Path targetDirectory = Files.createTempDirectory("infer-file-utils");
        try {
            invokeBuildSafeTargetFile(targetDirectory.toString(), "/tmp/evil.py");
        } catch (InvocationTargetException e) {
            throw e.getCause();
        } finally {
            deleteRecursively(targetDirectory);
        }
    }

    @Test
    public void testBuildSafeTargetFileAllowsDeepNestedPath() throws Exception {
        Path targetDirectory = Files.createTempDirectory("infer-file-utils");
        try {
            File targetFile = invokeBuildSafeTargetFile(targetDirectory.toString(),
                "pkg/sub/model.pt");
            assertEquals(targetFile.toPath(),
                targetDirectory.resolve("pkg/sub/model.pt").toAbsolutePath().normalize());
        } finally {
            deleteRecursively(targetDirectory);
        }
    }

    private static boolean invokeShouldExtractInferResource(String fileName) throws Exception {
        Method method = InferFileUtils.class.getDeclaredMethod("shouldExtractInferResource",
            String.class);
        method.setAccessible(true);
        return (boolean) method.invoke(null, fileName);
    }

    private static File invokeBuildSafeTargetFile(String targetDirectory, String fileName)
        throws Exception {
        Method method = InferFileUtils.class.getDeclaredMethod("buildSafeTargetFile",
            String.class, String.class);
        method.setAccessible(true);
        return (File) method.invoke(null, targetDirectory, fileName);
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        List<Path> paths = new ArrayList<Path>();
        try (Stream<Path> stream = Files.walk(root)) {
            for (Path path : (Iterable<Path>) stream::iterator) {
                paths.add(path);
            }
        }
        Collections.sort(paths, Collections.reverseOrder());
        for (Path path : paths) {
            Files.deleteIfExists(path);
        }
    }
}
