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

package org.apache.geaflow.infer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Stream;
import org.testng.annotations.Test;

public class InferEnvironmentInstallScriptTest {

    @Test
    public void testFreshInstallScriptDownloadsInstallerIntoNonEmptyFile() throws Exception {
        Path rootDir = Files.createTempDirectory("infer-env-script");
        try {
            Path runtimeDir = Files.createDirectories(rootDir.resolve("runtime"));
            Files.createDirectories(rootDir.resolve("inferFiles"));
            Path fakeBinDir = Files.createDirectories(rootDir.resolve("bin"));
            Path fakeInstallerSource = rootDir.resolve("fake-miniconda.sh");
            Files.write(fakeInstallerSource, buildFakeInstaller().getBytes(UTF_8));
            makeExecutable(fakeInstallerSource);

            Path fakeWget = fakeBinDir.resolve("wget");
            Files.write(fakeWget, buildFakeWget().getBytes(UTF_8));
            makeExecutable(fakeWget);

            Path scriptPath = runtimeDir.resolve("install-infer-env.sh");
            try (InputStream inputStream = InferEnvironmentInstallScriptTest.class
                .getResourceAsStream("/infer/env/install-infer-env.sh")) {
                if (inputStream == null) {
                    throw new IOException("cannot load install-infer-env.sh from classpath");
                }
                Files.copy(inputStream, scriptPath);
            }
            makeExecutable(scriptPath);

            Path missingRequirements = rootDir.resolve("missing-requirements.txt");

            ProcessBuilder processBuilder = new ProcessBuilder(
                "bash",
                scriptPath.toAbsolutePath().toString(),
                runtimeDir.toAbsolutePath().toString(),
                missingRequirements.toAbsolutePath().toString(),
                fakeInstallerSource.toUri().toString());
            processBuilder.environment().put("PATH",
                fakeBinDir.toAbsolutePath() + System.getProperty("path.separator")
                    + processBuilder.environment().get("PATH"));
            processBuilder.redirectErrorStream(true);

            Process process = processBuilder.start();
            String output;
            try (InputStream inputStream = process.getInputStream()) {
                output = new String(readAllBytes(inputStream), UTF_8);
            }
            int exitCode = process.waitFor();

            assertEquals(exitCode, 0, output);
            assertTrue(Files.isRegularFile(runtimeDir.resolve("miniconda.sh")), output);
            assertTrue(Files.size(runtimeDir.resolve("miniconda.sh")) > 0, output);
            assertTrue(Files.isRegularFile(runtimeDir.resolve("conda/bin/python3")), output);
        } finally {
            deleteRecursively(rootDir);
        }
    }

    private String buildFakeInstaller() {
        return "#!/bin/sh\n"
            + "prefix=\"\"\n"
            + "while [ \"$#\" -gt 0 ]; do\n"
            + "  if [ \"$1\" = \"-p\" ]; then\n"
            + "    shift\n"
            + "    prefix=\"$1\"\n"
            + "  fi\n"
            + "  shift\n"
            + "done\n"
            + "mkdir -p \"$prefix/bin\"\n"
            + "cat > \"$prefix/bin/python3\" <<'EOF'\n"
            + "#!/bin/sh\n"
            + "exit 0\n"
            + "EOF\n"
            + "chmod +x \"$prefix/bin/python3\"\n";
    }

    private String buildFakeWget() {
        return "#!/bin/sh\n"
            + "src=\"$1\"\n"
            + "out=\"\"\n"
            + "while [ \"$#\" -gt 0 ]; do\n"
            + "  if [ \"$1\" = \"-O\" ]; then\n"
            + "    shift\n"
            + "    out=\"$1\"\n"
            + "  fi\n"
            + "  shift\n"
            + "done\n"
            + "src=\"${src#file://}\"\n"
            + "cp \"$src\" \"$out\"\n";
    }

    private void makeExecutable(Path file) throws IOException {
        Set<PosixFilePermission> permissions = EnumSet.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_EXECUTE,
            PosixFilePermission.GROUP_READ,
            PosixFilePermission.GROUP_EXECUTE,
            PosixFilePermission.OTHERS_READ,
            PosixFilePermission.OTHERS_EXECUTE);
        try {
            Files.setPosixFilePermissions(file, permissions);
        } catch (UnsupportedOperationException e) {
            file.toFile().setExecutable(true, false);
        }
    }

    private void deleteRecursively(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            paths.sorted((left, right) -> right.compareTo(left))
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
        }
    }

    private byte[] readAllBytes(InputStream inputStream) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = inputStream.read(buffer)) >= 0) {
            outputStream.write(buffer, 0, length);
        }
        return outputStream.toByteArray();
    }
}
