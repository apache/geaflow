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

package org.apache.geaflow.dsl.runtime.query;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.infer.InferContext;
import org.apache.geaflow.infer.InferContextPool;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Production-grade integration test for SA-GNN (Spatial Adaptive Graph Neural Network)
 * with Java-Python inference using PaddlePaddle.
 *
 * <p>This test verifies the complete integration between Java {@code SAGNN} and Python
 * {@code SAGNNTransFormFunction}, including:
 * <ul>
 *   <li>Spatial feature split: last 2 dims as coordinates, first 62 as semantic features</li>
 *   <li>Java-Python data exchange via shared memory</li>
 *   <li>PaddlePaddle inference with SA-GNN model</li>
 *   <li>paddle.Tensor → native Python list coercion (pickle-safe)</li>
 *   <li>Result embedding validation (64-dimensional output)</li>
 * </ul>
 *
 * <p>Prerequisites:
 * <ul>
 *   <li>Python 3.x installed</li>
 *   <li>PaddlePaddle 2.6.0 installed: {@code pip install paddlepaddle==2.6.0}</li>
 *   <li>PGL (Paddle Graph Learning) installed: {@code pip install pgl>=2.2.4}</li>
 *   <li>PaddleSpatial installed: {@code pip install paddlespatial>=0.1.0}</li>
 *   <li>{@code PaddleSpatialSAGNNTransFormFunctionUDF.py} available as a resource</li>
 * </ul>
 *
 * <p>The shared {@link InferContext} is initialised once in {@code @BeforeClass} and reused
 * across all test methods to amortise the cost of Conda/Python environment setup.
 */
public class SAGNNInferIntegrationTest {

    private static final String TEST_WORK_DIR = "/tmp/geaflow/sagnn_test";
    private static final String PYTHON_UDF_DIR = TEST_WORK_DIR + "/python_udf";
    private static final String RESULT_DIR = TEST_WORK_DIR + "/results";

    /** 64-dimensional feature vector: 62 semantic dims + 2 spatial coordinate dims. */
    private static final int TOTAL_FEATURE_DIM = 64;
    private static final int COORD_DIM = 2;
    private static final int SEMANTIC_DIM = TOTAL_FEATURE_DIM - COORD_DIM;

    // Shared InferContext for all tests (initialized once per class)
    private static InferContext<List<Double>> sharedInferContext;

    /**
     * Class-level setup: initialise shared {@link InferContext} once for all test methods.
     * SA-GNN uses PaddlePaddle, so {@code geaflow.infer.framework.type=PADDLE} must be set.
     */
    @BeforeClass
    public static void setUpClass() throws IOException {
        FileUtils.deleteQuietly(new File(TEST_WORK_DIR));
        new File(PYTHON_UDF_DIR).mkdirs();
        new File(RESULT_DIR).mkdirs();

        copyPythonUDFToTestDirStatic();

        if (isPythonAvailableStatic()) {
            try {
                Configuration config = createDefaultConfiguration();
                sharedInferContext = InferContextPool.getOrCreate(config);
                System.out.println("Shared SA-GNN InferContext initialized successfully");
                System.out.println("  Pool status: " + InferContextPool.getStatus());
            } catch (Exception e) {
                System.out.println("Failed to initialize shared InferContext: " + e.getMessage());
                System.out.println("Tests that depend on InferContext will be skipped");
            }
        } else {
            System.out.println("Python not available - InferContext tests will be skipped");
        }
    }

    /** Class-level teardown: release shared resources. */
    @AfterClass
    public static void tearDownClass() {
        System.out.println("Pool status before cleanup: " + InferContextPool.getStatus());
        InferContextPool.closeAll();
        System.out.println("Pool status after cleanup: " + InferContextPool.getStatus());
        FileUtils.deleteQuietly(new File(TEST_WORK_DIR));
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * Test 1: basic SA-GNN inference via shared InferContext.
     *
     * <p>Sends a single spatial POI vertex (64-dim features) and 2 neighbour vectors
     * through the Java-Python bridge and verifies that the returned embedding is
     * 64-dimensional and contains non-zero values.
     */
    @Test(timeOut = 30000)
    public void testSAGNNInferContextJavaPythonCommunication() throws Exception {
        if (sharedInferContext == null) {
            System.out.println("Shared InferContext not available, skipping test");
            return;
        }

        Object vertexId = 1L;
        List<Double> vertexFeatures = buildSpatialFeatureVector(0, 116.39, 39.91);

        Map<Integer, List<List<Double>>> neighborFeaturesMap = new HashMap<>();

        List<List<Double>> layer1Neighbors = new ArrayList<>();
        layer1Neighbors.add(buildSpatialFeatureVector(1, 116.41, 39.92));
        layer1Neighbors.add(buildSpatialFeatureVector(2, 116.38, 39.93));
        neighborFeaturesMap.put(1, layer1Neighbors);

        List<List<Double>> layer2Neighbors = new ArrayList<>();
        layer2Neighbors.add(buildSpatialFeatureVector(3, 116.42, 39.90));
        neighborFeaturesMap.put(2, layer2Neighbors);

        Object[] inputs = {vertexId, vertexFeatures, neighborFeaturesMap};

        long t0 = System.currentTimeMillis();
        List<Double> embedding = sharedInferContext.infer(inputs);
        long elapsed = System.currentTimeMillis() - t0;

        Assert.assertNotNull(embedding, "SA-GNN embedding must not be null");
        Assert.assertEquals(embedding.size(), TOTAL_FEATURE_DIM,
            "SA-GNN output embedding dimension must be " + TOTAL_FEATURE_DIM);

        boolean hasNonZero = embedding.stream().anyMatch(v -> v != 0.0);
        Assert.assertTrue(hasNonZero, "SA-GNN embedding should contain non-zero values");

        System.out.println("SA-GNN inference test passed. Embedding size=" + embedding.size()
            + " in " + elapsed + "ms");
    }

    /**
     * Test 2: multiple sequential SA-GNN inference calls via shared InferContext.
     *
     * <p>Simulates 3 different POI vertices at distinct spatial coordinates, verifying
     * that the PaddleInferSession handles sequential calls without state corruption.
     */
    @Test(timeOut = 60000)
    public void testSAGNNMultipleInferenceCalls() throws Exception {
        if (sharedInferContext == null) {
            System.out.println("Shared InferContext not available, skipping test");
            return;
        }

        double[][] coords = {
            {116.39, 39.91},
            {116.41, 39.92},
            {116.38, 39.93}
        };

        long totalTime = 0;

        for (int v = 0; v < coords.length; v++) {
            Object vertexId = (long) (v + 1);
            List<Double> features = buildSpatialFeatureVector(v, coords[v][0], coords[v][1]);

            Map<Integer, List<List<Double>>> nbrMap = new HashMap<>();
            List<List<Double>> neighbors = new ArrayList<>();
            neighbors.add(buildSpatialFeatureVector(v + 10, coords[(v + 1) % 3][0],
                coords[(v + 1) % 3][1]));
            nbrMap.put(1, neighbors);

            Object[] inputs = {vertexId, features, nbrMap};

            long t0 = System.currentTimeMillis();
            List<Double> embedding = sharedInferContext.infer(inputs);
            long elapsed = System.currentTimeMillis() - t0;
            totalTime += elapsed;

            Assert.assertNotNull(embedding, "Embedding null for vertex " + v);
            Assert.assertEquals(embedding.size(), TOTAL_FEATURE_DIM,
                "Embedding dim mismatch for vertex " + v);
            System.out.println("Inference call " + (v + 1) + " passed (" + elapsed + "ms)");
        }

        System.out.println("Multiple inference test passed. Total=" + totalTime + "ms, "
            + "Avg=" + String.format("%.1f", totalTime / 3.0) + "ms");
    }

    /**
     * Test 3: check that required PaddlePaddle Python modules are importable.
     *
     * <p>Runs a lightweight subprocess that tries {@code import paddle} and {@code import pgl}.
     * If the modules are absent, the test emits a warning rather than failing hard,
     * since the CI environment may not have PaddlePaddle installed.
     */
    @Test
    public void testPaddleModulesAvailable() {
        if (!isPythonAvailableStatic()) {
            System.out.println("Python not available, skipping module check");
            return;
        }

        String[] modules = {"paddle", "pgl"};
        for (String module : modules) {
            boolean available = isPythonModuleAvailable(module);
            if (available) {
                System.out.println("Python module available: " + module);
            } else {
                System.out.println("Warning: Python module not found: " + module
                    + " (install with: pip install paddlepaddle pgl)");
            }
        }
    }

    /**
     * Test 4: direct Python UDF sanity check without expensive InferContext init.
     *
     * <p>Spawns a Python subprocess that imports {@code SAGNNTransFormFunction},
     * constructs a mini spatial graph, and runs a forward pass, verifying that
     * the transform returns a 64-dimensional embedding.
     */
    @Test(timeOut = 30000)
    public void testSAGNNPythonUDFDirect() throws Exception {
        if (!isPythonAvailableStatic()) {
            System.out.println("Python not available, skipping direct UDF test");
            return;
        }

        String testScript = String.join("\n",
            "import sys",
            "sys.path.insert(0, '" + PYTHON_UDF_DIR + "')",
            "try:",
            "    from PaddleSpatialSAGNNTransFormFunctionUDF import SAGNNTransFormFunction",
            "    print('Successfully imported SAGNNTransFormFunction')",
            "    sagnn = SAGNNTransFormFunction()",
            "    print('SAGNNTransFormFunction initialized')",
            "    vertex_id = 1",
            "    # 62 semantic + 2 coordinate dims",
            "    vertex_features = [float(i) * 0.1 for i in range(62)] + [116.39, 39.91]",
            "    neighbor_features_map = {",
            "        1: [[float(j) * 0.1 for j in range(62)] + [116.41, 39.92]",
            "            for _ in range(2)],",
            "    }",
            "    result = sagnn.transform_pre(vertex_id, vertex_features, neighbor_features_map)",
            "    print('transform_pre returned: ' + str(type(result)))",
            "    if result is not None:",
            "        embedding, returned_id = result",
            "        print('Embedding length: ' + str(len(embedding)))",
            "        assert len(embedding) == 64, 'Expected 64-dim embedding, got ' + str(len(embedding))",
            "        print('ALL CHECKS PASSED')",
            "    sys.exit(0)",
            "except Exception as e:",
            "    import traceback",
            "    traceback.print_exc()",
            "    sys.exit(1)"
        );

        File scriptFile = new File(PYTHON_UDF_DIR, "test_sagnn_udf.py");
        try (OutputStreamWriter writer = new OutputStreamWriter(
                new FileOutputStream(scriptFile), StandardCharsets.UTF_8)) {
            writer.write(testScript);
        }

        String pythonExe = getPythonExecutableStatic();
        Process process = Runtime.getRuntime().exec(
            new String[]{pythonExe, scriptFile.getAbsolutePath()});

        StringBuilder out = new StringBuilder();
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = br.readLine()) != null) {
                out.append(line).append("\n");
                System.out.println(line);
            }
        }

        StringBuilder err = new StringBuilder();
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(process.getErrorStream()))) {
            String line;
            while ((line = br.readLine()) != null) {
                err.append(line).append("\n");
                System.err.println(line);
            }
        }

        int exitCode = process.waitFor();
        Assert.assertEquals(exitCode, 0,
            "SA-GNN Python UDF test failed.\nOutput:\n" + out + "\nErrors:\n" + err);

        String outStr = out.toString();
        Assert.assertTrue(outStr.contains("Successfully imported"),
            "SAGNNTransFormFunction import failed");
        Assert.assertTrue(outStr.contains("ALL CHECKS PASSED"),
            "SA-GNN direct UDF check did not pass");

        System.out.println("Direct SA-GNN Python UDF test PASSED");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Build a 64-dimensional spatial feature vector.
     * Dims 0..61 are semantic features derived from {@code seed}.
     * Dims 62..63 are {@code [coordX, coordY]}.
     */
    private static List<Double> buildSpatialFeatureVector(int seed, double coordX, double coordY) {
        List<Double> features = new ArrayList<>(TOTAL_FEATURE_DIM);
        for (int i = 0; i < SEMANTIC_DIM; i++) {
            features.add((double) (seed * 10 + i) * 0.1);
        }
        features.add(coordX);
        features.add(coordY);
        return features;
    }

    private static Configuration createDefaultConfiguration() {
        Configuration config = new Configuration();
        config.put(FrameworkConfigKeys.INFER_ENV_ENABLE.getKey(), "true");
        config.put(FrameworkConfigKeys.INFER_ENV_USE_SYSTEM_PYTHON.getKey(), "true");
        config.put(FrameworkConfigKeys.INFER_ENV_SYSTEM_PYTHON_PATH.getKey(),
            getPythonExecutableStatic());
        config.put(FrameworkConfigKeys.INFER_ENV_USER_TRANSFORM_CLASSNAME.getKey(),
            "SAGNNTransFormFunction");
        config.put(FrameworkConfigKeys.INFER_FRAMEWORK_TYPE.getKey(), "PADDLE");
        config.put(FrameworkConfigKeys.INFER_ENV_INIT_TIMEOUT_SEC.getKey(), "180");
        config.put(ExecutionConfigKeys.JOB_UNIQUE_ID.getKey(), "sagnn_test_job_shared");
        config.put(FileConfigKeys.ROOT.getKey(), TEST_WORK_DIR);
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), "SAGNNInferTest");
        return config;
    }

    private static String getPythonExecutableStatic() {
        String[] candidates = {
            "/opt/homebrew/Caskroom/miniforge/base/envs/paddle_env/bin/python3",
            "/opt/miniconda3/envs/paddle_env/bin/python3",
            "/opt/homebrew/Caskroom/miniforge/base/envs/pytorch_env/bin/python3",
            "/opt/miniconda3/envs/pytorch_env/bin/python3",
            "/usr/local/bin/python3",
            "python3"
        };

        for (String path : candidates) {
            try {
                File f = new File(path);
                if (f.exists()) {
                    Process p = Runtime.getRuntime().exec(path + " --version");
                    if (p.waitFor() == 0) {
                        System.out.println("Found Python at: " + path);
                        return path;
                    }
                }
            } catch (Exception ignored) {
                // try next
            }
        }
        System.err.println("Warning: Could not find Python executable, using 'python3'");
        return "python3";
    }

    private static boolean isPythonAvailableStatic() {
        try {
            String exe = getPythonExecutableStatic();
            Process p = Runtime.getRuntime().exec(exe + " --version");
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean isPythonModuleAvailable(String moduleName) {
        try {
            String exe = getPythonExecutableStatic();
            Process p = Runtime.getRuntime().exec(
                new String[]{exe, "-c", "import " + moduleName});
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    private static void copyPythonUDFToTestDirStatic() throws IOException {
        InputStream is = SAGNNInferIntegrationTest.class.getResourceAsStream(
            "/PaddleSpatialSAGNNTransFormFunctionUDF.py");
        if (is == null) {
            System.out.println("PaddleSpatialSAGNNTransFormFunctionUDF.py not found in resources, "
                + "direct UDF test will be skipped");
            return;
        }
        File udfFile = new File(PYTHON_UDF_DIR, "PaddleSpatialSAGNNTransFormFunctionUDF.py");
        try (OutputStreamWriter writer = new OutputStreamWriter(
                new FileOutputStream(udfFile), StandardCharsets.UTF_8)) {
            writer.write(IOUtils.toString(is, StandardCharsets.UTF_8));
        }
    }
}
