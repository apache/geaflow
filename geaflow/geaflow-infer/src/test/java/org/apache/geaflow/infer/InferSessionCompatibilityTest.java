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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.testng.SkipException;
import org.testng.annotations.Test;

public class InferSessionCompatibilityTest {

    @Test
    public void testTransformContractRunsThroughInferSession() throws Exception {
        String output = runInferSessionScript(
            "import json\n"
                + "from inferSession import TorchInferSession\n"
                + "\n"
                + "class Transform(object):\n"
                + "    input_size = 1\n"
                + "\n"
                + "    def __init__(self):\n"
                + "        self.loaded = False\n"
                + "\n"
                + "    def load_model(self, model_path):\n"
                + "        self.loaded = True\n"
                + "        def model(value):\n"
                + "            return {'loaded': self.loaded, 'modeled': value * 2}\n"
                + "        return model\n"
                + "\n"
                + "    def transform_pre(self, value):\n"
                + "        return (value + 1,)\n"
                + "\n"
                + "    def transform_post(self, res):\n"
                + "        return {'loaded': res['loaded'], 'value': res['modeled'], 'post_processed': True}\n"
                + "\n"
                + "print(json.dumps(TorchInferSession(Transform()).run(41), sort_keys=True))\n");

        assertEquals(output, "{\"loaded\": true, \"post_processed\": true, \"value\": 84}");
    }

    @Test
    public void testTransformContractSupportsLegacyModeWhenLoadModelReturnsNone() throws Exception {
        String output = runInferSessionScript(
            "import json\n"
                + "from inferSession import TorchInferSession\n"
                + "\n"
                + "class Transform(object):\n"
                + "    input_size = 1\n"
                + "\n"
                + "    def load_model(self, model_path):\n"
                + "        return None\n"
                + "\n"
                + "    def transform_pre(self, value):\n"
                + "        return {'legacy': value + 1}\n"
                + "\n"
                + "    def transform_post(self, res):\n"
                + "        return {'mode': 'legacy', 'value': res['legacy']}\n"
                + "\n"
                + "print(json.dumps(TorchInferSession(Transform()).run(41), sort_keys=True))\n");

        assertEquals(output, "{\"mode\": \"legacy\", \"value\": 42}");
    }

    @Test
    public void testTransformContractRejectsNonCallableNonLegacyModel() throws Exception {
        String output = runInferSessionScript(
            "from inferSession import TorchInferSession\n"
                + "\n"
                + "class Transform(object):\n"
                + "    input_size = 1\n"
                + "\n"
                + "    def load_model(self, model_path):\n"
                + "        return object()\n"
                + "\n"
                + "    def transform_pre(self, value):\n"
                + "        return (value,)\n"
                + "\n"
                + "    def transform_post(self, res):\n"
                + "        return res\n"
                + "\n"
                + "try:\n"
                + "    TorchInferSession(Transform())\n"
                + "except RuntimeError as e:\n"
                + "    print(str(e))\n");

        assertEquals(output, "load_model(model_path) must return a callable model");
    }

    @Test
    public void testTransformContractExpandsTupleArgsToModel() throws Exception {
        String output = runInferSessionScript(
            "import json\n"
                + "from inferSession import TorchInferSession\n"
                + "\n"
                + "class Transform(object):\n"
                + "    input_size = 1\n"
                + "\n"
                + "    def load_model(self, model_path):\n"
                + "        def model(left, right):\n"
                + "            return {'sum': left + right}\n"
                + "        return model\n"
                + "\n"
                + "    def transform_pre(self, value):\n"
                + "        return (value, value + 2)\n"
                + "\n"
                + "    def transform_post(self, res):\n"
                + "        return {'value': res['sum']}\n"
                + "\n"
                + "print(json.dumps(TorchInferSession(Transform()).run(40), sort_keys=True))\n");

        assertEquals(output, "{\"value\": 82}");
    }

    @Test
    public void testTransformContractSupportsListModelInputAndListPostPayload() throws Exception {
        String output = runInferSessionScript(
            "import json\n"
                + "from inferSession import TorchInferSession\n"
                + "\n"
                + "class Transform(object):\n"
                + "    input_size = 1\n"
                + "\n"
                + "    def load_model(self, model_path):\n"
                + "        def model(left, right):\n"
                + "            return [left, right, left + right]\n"
                + "        return model\n"
                + "\n"
                + "    def transform_pre(self, value):\n"
                + "        return [value, value + 1]\n"
                + "\n"
                + "    def transform_post(self, res):\n"
                + "        return res\n"
                + "\n"
                + "print(json.dumps(TorchInferSession(Transform()).run(5)))\n");

        assertEquals(output, "[5, 6, 11]");
    }

    @Test
    public void testTransformContractSurfacesLoadModelExceptionMessage() throws Exception {
        String output = runInferSessionScript(
            "from inferSession import TorchInferSession\n"
                + "\n"
                + "class Transform(object):\n"
                + "    input_size = 1\n"
                + "\n"
                + "    def load_model(self, model_path):\n"
                + "        raise RuntimeError('boom from load_model')\n"
                + "\n"
                + "    def transform_pre(self, value):\n"
                + "        return (value,)\n"
                + "\n"
                + "    def transform_post(self, res):\n"
                + "        return res\n"
                + "\n"
                + "try:\n"
                + "    TorchInferSession(Transform())\n"
                + "except RuntimeError as e:\n"
                + "    print(str(e))\n");

        assertEquals(output, "boom from load_model");
    }

    private String runInferSessionScript(String scriptBody) throws Exception {
        ensurePythonAvailable();
        Path tempDir = Files.createTempDirectory("infer-session-test");
        try {
            copyInferSessionResource(tempDir);
            Files.write(tempDir.resolve("torch.py"),
                Arrays.asList("def set_num_threads(_):", "    return None"), UTF_8);
            Files.write(tempDir.resolve("runner.py"), scriptBody.getBytes(UTF_8));

            Process process = new ProcessBuilder("python3", "runner.py")
                .directory(tempDir.toFile())
                .redirectErrorStream(true)
                .start();
            byte[] outputBytes;
            try (InputStream inputStream = process.getInputStream()) {
                outputBytes = readAllBytes(inputStream);
            }
            int exitCode = process.waitFor();
            String output = new String(outputBytes, UTF_8).trim();
            assertEquals(exitCode, 0, output);
            return output;
        } finally {
            deleteRecursively(tempDir);
        }
    }

    private void ensurePythonAvailable() throws Exception {
        try {
            Process process = new ProcessBuilder("python3", "--version")
                .redirectErrorStream(true)
                .start();
            try (InputStream inputStream = process.getInputStream()) {
                readAllBytes(inputStream);
            }
            if (process.waitFor() != 0) {
                throw new SkipException("python3 is unavailable for infer session compatibility test");
            }
        } catch (IOException e) {
            throw new SkipException("python3 is unavailable for infer session compatibility test", e);
        }
    }

    private void copyInferSessionResource(Path tempDir) throws IOException {
        try (InputStream inputStream = InferSessionCompatibilityTest.class
            .getResourceAsStream("/infer/inferRuntime/inferSession.py")) {
            if (inputStream == null) {
                throw new IOException("cannot load inferSession.py from classpath");
            }
            Files.copy(inputStream, tempDir.resolve("inferSession.py"));
        }
    }

    private void deleteRecursively(Path root) throws IOException {
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
