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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.SkipException;
import org.testng.annotations.Test;

public class InferHotReloadTest {

    @Test
    public void testDoubleBufferHotReloadSwapsAfterWarmup() throws Exception {
        ensurePythonAvailable();
        Path tempDir = Files.createTempDirectory("infer-hot-reload");
        try {
            copyInferSessionResource(tempDir);
            writeTorchStub(tempDir);
            Files.write(tempDir.resolve("runner.py"), buildHappyPathRunnerScript().getBytes(UTF_8));

            Map<String, String> values = parseOutput(runPython(tempDir));
            String initActiveId = values.get("INIT_ACTIVE_ID");
            String initStandbyId = values.get("INIT_STANDBY_ID");
            String initActiveTransformId = values.get("INIT_ACTIVE_TRANSFORM_ID");
            String initStandbyTransformId = values.get("INIT_STANDBY_TRANSFORM_ID");

            assertEquals(values.get("INITIAL"), "v1");
            assertEquals(values.get("BEFORE_SWAP"), "v1,v1,v1,v1");
            assertEquals(values.get("AFTER_SWAP"), "v2,v2,v2");
            assertEquals(values.get("LOAD_EVENTS"),
                initActiveTransformId + "@v1," + initStandbyTransformId + "@v1,"
                    + initStandbyTransformId + "@v2");
            assertEquals(values.get("WARMUP_EVENTS"), initStandbyTransformId + "@v2");
            assertTrue(!initActiveId.equals(initStandbyId), values.toString());
            assertEquals(values.get("FINAL_ACTIVE_ID"), initStandbyId);
            assertEquals(values.get("FINAL_STANDBY_ID"), initActiveId);
            assertEquals(values.get("FINAL_ACTIVE_RUN"), "v2");
            assertEquals(values.get("FINAL_STANDBY_RUN"), "v1");
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    public void testMissingWarmupKeepsActiveSlot() throws Exception {
        ensurePythonAvailable();
        Path tempDir = Files.createTempDirectory("infer-hot-reload");
        try {
            copyInferSessionResource(tempDir);
            writeTorchStub(tempDir);
            Files.write(tempDir.resolve("runner.py"), buildMissingWarmupRunnerScript().getBytes(UTF_8));

            Map<String, String> values = parseOutput(runPython(tempDir));
            String initActiveId = values.get("INIT_ACTIVE_ID");
            String initStandbyId = values.get("INIT_STANDBY_ID");
            String initActiveTransformId = values.get("INIT_ACTIVE_TRANSFORM_ID");
            String initStandbyTransformId = values.get("INIT_STANDBY_TRANSFORM_ID");

            assertEquals(values.get("INITIAL"), "v1");
            assertEquals(values.get("AFTER_FAILURE"), "v1,v1,v1,v1");
            assertEquals(values.get("LOAD_EVENTS"),
                initActiveTransformId + "@v1," + initStandbyTransformId + "@v1,"
                    + initStandbyTransformId + "@v2");
            assertTrue(values.get("WARMUP_EVENTS").isEmpty(), values.toString());
            assertTrue(!initActiveId.equals(initStandbyId), values.toString());
            assertEquals(values.get("FINAL_ACTIVE_ID"), initActiveId);
            assertEquals(values.get("FINAL_STANDBY_ID"), initStandbyId);
            assertEquals(values.get("FINAL_ACTIVE_RUN"), "v1");
        } finally {
            deleteRecursively(tempDir);
        }
    }

    private String runPython(Path tempDir) throws Exception {
        Process process = new ProcessBuilder("python3", "runner.py")
            .directory(tempDir.toFile())
            .redirectErrorStream(true)
            .start();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new RuntimeException("python hot reload test timed out");
        }

        byte[] outputBytes;
        try (InputStream inputStream = process.getInputStream()) {
            outputBytes = inputStream.readAllBytes();
        }
        String output = new String(outputBytes, UTF_8).trim();
        assertEquals(process.exitValue(), 0, output);
        return output;
    }

    private Map<String, String> parseOutput(String output) {
        Map<String, String> values = new HashMap<>();
        for (String line : output.split("\\R")) {
            int delimiterIndex = line.indexOf('=');
            if (delimiterIndex <= 0) {
                continue;
            }
            values.put(line.substring(0, delimiterIndex), line.substring(delimiterIndex + 1));
        }
        return values;
    }

    private String buildHappyPathRunnerScript() {
        StringBuilder script = new StringBuilder();
        appendPythonLine(script, "import os");
        appendPythonLine(script, "import threading");
        appendPythonLine(script, "import time");
        appendPythonLine(script, "from inferSession import TorchInferSession");
        appendPythonLine(script, "");
        appendPythonLine(script, "class Transform(object):");
        appendPythonLine(script, "    input_size = 1");
        appendPythonLine(script, "    load_events = []");
        appendPythonLine(script, "    warmup_events = []");
        appendPythonLine(script, "    lock = threading.Lock()");
        appendPythonLine(script, "    warmup_started = threading.Event()");
        appendPythonLine(script, "    warmup_release = threading.Event()");
        appendPythonLine(script, "");
        appendPythonLine(script, "    def __init__(self):");
        appendPythonLine(script, "        self.version = 'unset'");
        appendPythonLine(script, "");
        appendPythonLine(script, "    def load_model(self, model_path):");
        appendPythonLine(script, "        with open(model_path, 'r', encoding='utf-8') as model_file:");
        appendPythonLine(script, "            self.version = model_file.read().strip()");
        appendPythonLine(script, "        with self.__class__.lock:");
        appendPythonLine(script, "            self.__class__.load_events.append('{}@{}'.format(id(self), self.version))");
        appendPythonLine(script, "        if self.version == 'v2':");
        appendPythonLine(script, "            time.sleep(0.2)");
        appendPythonLine(script, "        return None");
        appendPythonLine(script, "");
        appendPythonLine(script, "    def get_warmup_inputs(self):");
        appendPythonLine(script, "        return ('warm',)");
        appendPythonLine(script, "");
        appendPythonLine(script, "    def transform_pre(self, value):");
        appendPythonLine(script, "        if value == 'warm':");
        appendPythonLine(script, "            with self.__class__.lock:");
        appendPythonLine(script, "                self.__class__.warmup_events.append('{}@{}'.format(id(self), self.version))");
        appendPythonLine(script, "            self.__class__.warmup_started.set()");
        appendPythonLine(script, "            if not self.__class__.warmup_release.wait(5):");
        appendPythonLine(script, "                raise RuntimeError('warmup release timed out')");
        appendPythonLine(script, "        return self.version");
        appendPythonLine(script, "");
        appendPythonLine(script, "    def transform_post(self, value):");
        appendPythonLine(script, "        return value");
        appendPythonLine(script, "");
        appendPythonLine(script, "def write_model(version):");
        appendPythonLine(script, "    with open('model.pt', 'w', encoding='utf-8') as model_file:");
        appendPythonLine(script, "        model_file.write(version)");
        appendPythonLine(script, "    with open('model.version', 'w', encoding='utf-8') as version_file:");
        appendPythonLine(script, "        version_file.write(version)");
        appendPythonLine(script, "    os.utime('model.pt', None)");
        appendPythonLine(script, "    os.utime('model.version', None)");
        appendPythonLine(script, "");
        appendPythonLine(script, "write_model('v1')");
        appendPythonLine(script, "session = TorchInferSession(Transform(), {");
        appendPythonLine(script, "    'model_path': os.path.join(os.getcwd(), 'model.pt'),");
        appendPythonLine(script, "    'model_version_file': os.path.join(os.getcwd(), 'model.version'),");
        appendPythonLine(script, "    'poll_interval_sec': 0.05,");
        appendPythonLine(script, "    'backoff_sec': 1.0,");
        appendPythonLine(script, "    'warmup_enabled': True,");
        appendPythonLine(script, "    'hot_reload_enabled': True,");
        appendPythonLine(script, "})");
        appendPythonLine(script, "");
        appendPythonLine(script, "initial = session.run('req')");
        appendPythonLine(script, "init_active_id = str(id(session.model_active))");
        appendPythonLine(script, "init_standby_id = str(id(session.model_standby))");
        appendPythonLine(script, "init_active_transform_id = str(id(session.model_active.transform))");
        appendPythonLine(script, "init_standby_transform_id = str(id(session.model_standby.transform))");
        appendPythonLine(script, "write_model('v2')");
        appendPythonLine(script, "if not Transform.warmup_started.wait(5):");
        appendPythonLine(script, "    raise RuntimeError('warmup did not start')");
        appendPythonLine(script, "before_swap = []");
        appendPythonLine(script, "for _ in range(4):");
        appendPythonLine(script, "    before_swap.append(session.run('req'))");
        appendPythonLine(script, "    time.sleep(0.05)");
        appendPythonLine(script, "Transform.warmup_release.set()");
        appendPythonLine(script, "time.sleep(0.8)");
        appendPythonLine(script, "after_swap = []");
        appendPythonLine(script, "for _ in range(3):");
        appendPythonLine(script, "    after_swap.append(session.run('req'))");
        appendPythonLine(script, "    time.sleep(0.05)");
        appendPythonLine(script, "final_active_id = str(id(session.model_active))");
        appendPythonLine(script, "final_standby_id = str(id(session.model_standby))");
        appendPythonLine(script, "standby_run = session._run_with_slot(session.model_standby, 'req')");
        appendPythonLine(script, "final_active_run = session.run('req')");
        appendPythonLine(script, "print('INITIAL=' + initial)");
        appendPythonLine(script, "print('BEFORE_SWAP=' + ','.join(before_swap))");
        appendPythonLine(script, "print('AFTER_SWAP=' + ','.join(after_swap))");
        appendPythonLine(script, "print('LOAD_EVENTS=' + ','.join(Transform.load_events))");
        appendPythonLine(script, "print('WARMUP_EVENTS=' + ','.join(Transform.warmup_events))");
        appendPythonLine(script, "print('INIT_ACTIVE_ID=' + init_active_id)");
        appendPythonLine(script, "print('INIT_STANDBY_ID=' + init_standby_id)");
        appendPythonLine(script, "print('INIT_ACTIVE_TRANSFORM_ID=' + init_active_transform_id)");
        appendPythonLine(script, "print('INIT_STANDBY_TRANSFORM_ID=' + init_standby_transform_id)");
        appendPythonLine(script, "print('FINAL_ACTIVE_ID=' + final_active_id)");
        appendPythonLine(script, "print('FINAL_STANDBY_ID=' + final_standby_id)");
        appendPythonLine(script, "print('FINAL_ACTIVE_RUN=' + final_active_run)");
        appendPythonLine(script, "print('FINAL_STANDBY_RUN=' + standby_run)");
        appendPythonLine(script, "session.close()");
        return script.toString();
    }

    private String buildMissingWarmupRunnerScript() {
        StringBuilder script = new StringBuilder();
        appendPythonLine(script, "import os");
        appendPythonLine(script, "import threading");
        appendPythonLine(script, "import time");
        appendPythonLine(script, "from inferSession import TorchInferSession");
        appendPythonLine(script, "");
        appendPythonLine(script, "class Transform(object):");
        appendPythonLine(script, "    input_size = 1");
        appendPythonLine(script, "    load_events = []");
        appendPythonLine(script, "    warmup_events = []");
        appendPythonLine(script, "    lock = threading.Lock()");
        appendPythonLine(script, "");
        appendPythonLine(script, "    def __init__(self):");
        appendPythonLine(script, "        self.version = 'unset'");
        appendPythonLine(script, "");
        appendPythonLine(script, "    def load_model(self, model_path):");
        appendPythonLine(script, "        with open(model_path, 'r', encoding='utf-8') as model_file:");
        appendPythonLine(script, "            self.version = model_file.read().strip()");
        appendPythonLine(script, "        with self.__class__.lock:");
        appendPythonLine(script, "            self.__class__.load_events.append('{}@{}'.format(id(self), self.version))");
        appendPythonLine(script, "        if self.version == 'v2':");
        appendPythonLine(script, "            time.sleep(0.2)");
        appendPythonLine(script, "        return None");
        appendPythonLine(script, "");
        appendPythonLine(script, "    def transform_pre(self, value):");
        appendPythonLine(script, "        return self.version");
        appendPythonLine(script, "");
        appendPythonLine(script, "    def transform_post(self, value):");
        appendPythonLine(script, "        return value");
        appendPythonLine(script, "");
        appendPythonLine(script, "def write_model(version):");
        appendPythonLine(script, "    with open('model.pt', 'w', encoding='utf-8') as model_file:");
        appendPythonLine(script, "        model_file.write(version)");
        appendPythonLine(script, "    with open('model.version', 'w', encoding='utf-8') as version_file:");
        appendPythonLine(script, "        version_file.write(version)");
        appendPythonLine(script, "    os.utime('model.pt', None)");
        appendPythonLine(script, "    os.utime('model.version', None)");
        appendPythonLine(script, "");
        appendPythonLine(script, "write_model('v1')");
        appendPythonLine(script, "session = TorchInferSession(Transform(), {");
        appendPythonLine(script, "    'model_path': os.path.join(os.getcwd(), 'model.pt'),");
        appendPythonLine(script, "    'model_version_file': os.path.join(os.getcwd(), 'model.version'),");
        appendPythonLine(script, "    'poll_interval_sec': 0.05,");
        appendPythonLine(script, "    'backoff_sec': 1.0,");
        appendPythonLine(script, "    'warmup_enabled': True,");
        appendPythonLine(script, "    'hot_reload_enabled': True,");
        appendPythonLine(script, "})");
        appendPythonLine(script, "");
        appendPythonLine(script, "initial = session.run('req')");
        appendPythonLine(script, "init_active_id = str(id(session.model_active))");
        appendPythonLine(script, "init_standby_id = str(id(session.model_standby))");
        appendPythonLine(script, "init_active_transform_id = str(id(session.model_active.transform))");
        appendPythonLine(script, "init_standby_transform_id = str(id(session.model_standby.transform))");
        appendPythonLine(script, "write_model('v2')");
        appendPythonLine(script, "time.sleep(0.8)");
        appendPythonLine(script, "after_failure = []");
        appendPythonLine(script, "for _ in range(4):");
        appendPythonLine(script, "    after_failure.append(session.run('req'))");
        appendPythonLine(script, "    time.sleep(0.05)");
        appendPythonLine(script, "final_active_id = str(id(session.model_active))");
        appendPythonLine(script, "final_standby_id = str(id(session.model_standby))");
        appendPythonLine(script, "final_active_run = session.run('req')");
        appendPythonLine(script, "print('INITIAL=' + initial)");
        appendPythonLine(script, "print('AFTER_FAILURE=' + ','.join(after_failure))");
        appendPythonLine(script, "print('LOAD_EVENTS=' + ','.join(Transform.load_events))");
        appendPythonLine(script, "print('WARMUP_EVENTS=' + ','.join(Transform.warmup_events))");
        appendPythonLine(script, "print('INIT_ACTIVE_ID=' + init_active_id)");
        appendPythonLine(script, "print('INIT_STANDBY_ID=' + init_standby_id)");
        appendPythonLine(script, "print('INIT_ACTIVE_TRANSFORM_ID=' + init_active_transform_id)");
        appendPythonLine(script, "print('INIT_STANDBY_TRANSFORM_ID=' + init_standby_transform_id)");
        appendPythonLine(script, "print('FINAL_ACTIVE_ID=' + final_active_id)");
        appendPythonLine(script, "print('FINAL_STANDBY_ID=' + final_standby_id)");
        appendPythonLine(script, "print('FINAL_ACTIVE_RUN=' + final_active_run)");
        appendPythonLine(script, "session.close()");
        return script.toString();
    }

    private void appendPythonLine(StringBuilder script, String line) {
        script.append(line).append(System.lineSeparator());
    }

    private void ensurePythonAvailable() throws Exception {
        try {
            Process process = new ProcessBuilder("python3", "--version")
                .redirectErrorStream(true)
                .start();
            process.getInputStream().readAllBytes();
            if (process.waitFor() != 0) {
                throw new SkipException("python3 is unavailable for infer hot reload test");
            }
        } catch (IOException e) {
            throw new SkipException("python3 is unavailable for infer hot reload test", e);
        }
    }

    private void copyInferSessionResource(Path tempDir) throws IOException {
        try (InputStream inputStream = InferHotReloadTest.class
            .getResourceAsStream("/infer/inferRuntime/inferSession.py")) {
            if (inputStream == null) {
                throw new IOException("cannot load inferSession.py from classpath");
            }
            Files.copy(inputStream, tempDir.resolve("inferSession.py"));
        }
    }

    private void writeTorchStub(Path tempDir) throws IOException {
        Files.write(tempDir.resolve("torch.py"), Arrays.asList(
            "def set_num_threads(_):",
            "    return None"
        ), UTF_8);
    }

    private void deleteRecursively(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (Stream<Path> stream = Files.walk(root)) {
            List<Path> paths = stream.sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());
            for (Path path : paths) {
                Files.deleteIfExists(path);
            }
        }
    }
}
