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

package org.apache.geaflow.example.util;

import com.google.common.io.Files;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResultValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResultValidator.class);

    public static void cleanResult(String resultPath) {
        File dir = new File(resultPath);
        FileUtils.deleteQuietly(dir);
    }

    public static void validateResult(String refPath, String resultPath) throws IOException {
        validateResult(refPath, resultPath, null);

    }

    public static void validateResult(String refPath, String resultPath, Comparator<String> comparator)
        throws IOException {

        List<String> result = readFiles(resultPath);
        List<String> reference = readFiles(Resources.getResource(refPath).getFile());
        if (comparator == null) {
            Collections.sort(result);
            Collections.sort(reference);
        } else {
            Collections.sort(result, comparator);
            Collections.sort(reference, comparator);
        }

        LOGGER.info("result size {}, reference size {}", result.size(), reference.size());
        Assert.assertEquals(reference.size(), result.size());
        Assert.assertEquals(reference, result);

        cleanResult(resultPath);
    }

    public static void validateMapResult(String refPath, String resultPath) throws IOException {
        validateMapResult(refPath, resultPath, null);
    }

    public static void validateMapResult(String refPath, String resultPath, Comparator<String> comparator)
        throws IOException {
        List<String> result = readFiles(resultPath);
        if (comparator != null) {
            Collections.sort(result, comparator);
        }
        Map<String, String> resultMap = new HashMap<>();
        for (String temp : result) {
            String[] values = temp.split(",");
            resultMap.put(values[0].trim(), values[1].trim());
        }

        List<String> reference = readFiles(Resources.getResource(refPath).getFile());
        if (comparator != null) {
            Collections.sort(reference, comparator);
        }

        Map<String, String> referenceMap = new HashMap<>();
        for (String temp : reference) {
            String[] values = temp.split(",");
            referenceMap.put(values[0].trim(), values[1].trim());
        }

        LOGGER.info("result size {}, reference size {}", resultMap.size(), referenceMap.size());
        Assert.assertEquals(referenceMap.size(), resultMap.size());
        Assert.assertEquals(referenceMap, resultMap);

        cleanResult(resultPath);
    }

    private static List<String> readFiles(String path) throws IOException {
        File dir = new File(path);
        List<String> result = new ArrayList<>();
        File[] files = dir.listFiles();
        for (File file : files) {
            List<String> lines = Files.readLines(file, Charset.defaultCharset());
            result.addAll(lines);
        }
        return result;
    }
}
