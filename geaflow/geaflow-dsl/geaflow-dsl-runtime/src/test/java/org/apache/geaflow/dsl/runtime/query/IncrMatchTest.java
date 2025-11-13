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
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.Tuple2;


public class IncrMatchTest {

    private int vertexNum = 80;
    private int edgeNum = 400;

    private final String lineSplit = "----";

    private void RunTest(String queryPath) throws Exception {

        QueryTester.build()
            .withConfig(FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT.getKey(), "1")
            .withQueryPath(queryPath)
            .withConfig(DSLConfigKeys.ENABLE_INCR_TRAVERSAL.getKey(), "true")
            .withConfig(DSLConfigKeys.TABLE_SINK_SPLIT_LINE.getKey(), lineSplit)
            .execute();

        String incr = getTargetPath(queryPath);
        List<Set<String>> incrRes = readRes(incr, true);

        QueryTester.build()
            .withConfig(FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT.getKey(), "1")
            .withQueryPath(queryPath)
            .withConfig(DSLConfigKeys.ENABLE_INCR_TRAVERSAL.getKey(), "false")
            .withConfig(DSLConfigKeys.TABLE_SINK_SPLIT_LINE.getKey(), lineSplit)
            .execute();

        String allPath = getTargetPath(queryPath);
        List<Set<String>> allRes = readRes(allPath, false);
        
        // Find the last non-empty window index for both results
        int lastNonEmptyIncr = -1;
        int lastNonEmptyAll = -1;
        for (int i = incrRes.size() - 1; i >= 0; i--) {
            if (!incrRes.get(i).isEmpty()) {
                lastNonEmptyIncr = i;
                break;
            }
        }
        for (int i = allRes.size() - 1; i >= 0; i--) {
            if (!allRes.get(i).isEmpty()) {
                lastNonEmptyAll = i;
                break;
            }
        }
        
        // Compare up to the maximum of last non-empty windows
        int maxCompareIndex = Math.max(lastNonEmptyIncr, lastNonEmptyAll);
        if (maxCompareIndex < 0) {
            // Both are completely empty, that's fine
            return;
        }
        
        // Ensure we have enough windows
        int requiredSize = maxCompareIndex + 1;
        Assert.assertTrue(incrRes.size() >= requiredSize, 
            String.format("Incremental result should have at least %d windows, but has %d", 
                requiredSize, incrRes.size()));
        Assert.assertTrue(allRes.size() >= requiredSize, 
            String.format("Full result should have at least %d windows, but has %d", 
                requiredSize, allRes.size()));
        
        // Compare results window by window
        for (int i = 0; i <= maxCompareIndex; i++) {
            Set<String> incrSet = incrRes.get(i);
            Set<String> allSet = allRes.get(i);
            
            Assert.assertEquals(incrSet, allSet, 
                String.format("Window %d mismatch: incremental=%s, full=%s", i, incrSet, allSet));
        }
    }

    @Test
    public void testIncrMatch0() throws Exception {
        RunTest("/query/gql_incr_match.sql");
    }

    @Test
    public void testIncrMatchMultiParall() throws Exception {
        QueryTester.build()
            .withConfig(FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT.getKey(), "1")
            .withQueryPath("/query/gql_incr_match_multi_parall.sql")
            .withConfig(DSLConfigKeys.ENABLE_INCR_TRAVERSAL.getKey(), "true")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncrMatchRandom() throws Exception {
        createData();
        RunTest("/query/gql_incr_match_random.sql");
    }

    private void createData() {
        createVertex();
        createEdge();
    }

    private void createEdge() {
        File edgeFile = new File("/tmp/geaflow-test/incr_modern_edge.txt");
        Set<Tuple2<Integer, Integer>> edges = new HashSet<>();

        // Use fixed seed for deterministic test results to avoid flaky tests
        Random r = new Random(42L);
        while (edges.size() < edgeNum) {
            int src = r.nextInt(vertexNum) + 1;  // 1 to vertexNum
            int dst = r.nextInt(vertexNum) + 1;  // 1 to vertexNum
            while (src == dst) {
                dst = r.nextInt(vertexNum) + 1;
            }
            edges.add(new Tuple2<>(src, dst));
        }

        List<String> edgeString = new ArrayList<>();
        for (Tuple2<Integer, Integer> edge : edges) {
            edgeString.add(String.format("%s,%s,knows,0.5", edge._1, edge._2));
        }

        try {
            FileUtils.writeLines(edgeFile, edgeString);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void createVertex() {
        File file = new File("/tmp/geaflow-test/incr_modern_vertex.txt");
        List<String> vertices = new ArrayList<>();
        for (int i = 1; i <= vertexNum; i++) {
            vertices.add(String.format("%s,person,name1,1", i));
        }

        try {
            FileUtils.writeLines(file, vertices);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private static String getTargetPath(String queryPath) {
        assert queryPath != null;
        String[] paths = queryPath.split("/");
        String lastPath = paths[paths.length - 1];
        String targetPath = "target/" + lastPath.split("\\.")[0];
        String currentPath = new File(".").getAbsolutePath();
        targetPath = currentPath.substring(0, currentPath.length() - 1) + targetPath + "/partition_0";
        return targetPath;
    }


    private List<Set<String>> readRes(String path, boolean isIncr) throws IOException {
        List<Set<String>> res = new ArrayList<>();
        Set<String> curWindow = new HashSet<>();
        Set<String> allHistoryRes = new HashSet<>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(path));
            String currentLine;
            while ((currentLine = reader.readLine()) != null) {
                if (currentLine.equals(lineSplit)) {
                    // Process window separator
                    if (curWindow.isEmpty()) {
                        // Empty window: for incremental, keep history; for full, use empty
                        if (isIncr) {
                            res.add(new HashSet<>(allHistoryRes));
                        } else {
                            res.add(new HashSet<>());
                        }
                    } else {
                        // Non-empty window
                        if (isIncr) {
                            allHistoryRes.addAll(curWindow);
                            res.add(new HashSet<>(allHistoryRes));
                        } else {
                            res.add(new HashSet<>(curWindow));
                        }
                    }
                    curWindow = new HashSet<>();
                } else {
                    curWindow.add(currentLine);
                }
            }
            // Handle last window if file doesn't end with separator
            if (!curWindow.isEmpty()) {
                if (isIncr) {
                    allHistoryRes.addAll(curWindow);
                    res.add(new HashSet<>(allHistoryRes));
                } else {
                    res.add(new HashSet<>(curWindow));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        return res;
    }
}
