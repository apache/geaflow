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

package org.apache.geaflow.ai.graph.io;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ResourceFileScanner {

    public static Map<String, List<String>> scanGraphLdbcSfFolder(ClassLoader classLoader,
                                                                  String path) {
        Map<String, List<String>> resultMap = new HashMap<>();

        try {
            Path resourcePath = Paths.get(classLoader.getResource(path).toURI());

            Files.list(resourcePath)
                    .filter(Files::isDirectory)
                    .forEach(dirPath -> {
                        String folderName = dirPath.getFileName().toString();
                        try {
                            List<String> fileNames = Files.list(dirPath)
                                    .filter(Files::isRegularFile)
                                    .map(filePath -> filePath.getFileName().toString())
                                    .collect(Collectors.toList());

                            resultMap.put(folderName, fileNames);
                        } catch (IOException e) {
                            System.err.println("Fail to read: " + dirPath.toString());
                            e.printStackTrace();
                        }
                    });

        } catch (IOException | URISyntaxException e) {
            System.err.println("Fail to scan resource files.");
            e.printStackTrace();
        }

        return resultMap;
    }
}
