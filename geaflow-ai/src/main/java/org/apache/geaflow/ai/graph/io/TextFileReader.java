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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextFileReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(TextFileReader.class);

    List<String> fileContent;
    long limit;

    public TextFileReader(long limit) {
        this.fileContent = new ArrayList<>();
        this.limit = limit;
    }

    public void readFile(String fileName) throws IOException {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName);
        if (inputStream == null) {
            throw new IOException("Cannot find the file: " + fileName);
        }

        long count = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;

            while ((line = reader.readLine()) != null) {
                count++;
                if (StringUtils.isNotBlank(line)) {
                    fileContent.add(line);
                }
                if (count > limit) {
                    break;
                }
            }
        }
    }

    public List<String> getFileContent() {
        return fileContent;
    }

    public int getRowCount() {
        if (fileContent.isEmpty()) {
            return 0;
        }
        return fileContent.size();
    }

    public String getRow(int rowIndex) {
        String row = null;
        int rowCount = getRowCount();
        if (rowIndex < 0 || rowIndex >= rowCount) {
            throw new IndexOutOfBoundsException("Row index out of range: " + rowIndex);
        }
        row = fileContent.get(rowIndex);
        return row;
    }

    public void printContent() {
        LOGGER.info("Data content:");
        for (String content : fileContent) {
            LOGGER.info(content);
        }
        LOGGER.info("Total row count: " + getRowCount());
    }
}
