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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvFileReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvFileReader.class);

    List<String> colSchema;
    Map<String, List<String>> fileContent;
    long limit;

    public CsvFileReader(long limit) {
        this.colSchema = new ArrayList<>();
        this.fileContent = new HashMap<>();
        this.limit = limit;
    }

    public void readCsvFile(String fileName) throws IOException {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName);
        if (inputStream == null) {
            throw new IOException("Cannot find the file: " + fileName);
        }

        long count = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            boolean isFirstLine = true;

            while ((line = reader.readLine()) != null) {
                count++;
                if (isFirstLine) {
                    parseHeader(line);
                    isFirstLine = false;
                } else {
                    parseDataRow(line);
                }
                if (count > limit) {
                    break;
                }
            }
        }
    }

    private void parseHeader(String headerLine) {
        String[] headers = headerLine.split("\\|");

        colSchema.clear();
        fileContent.clear();

        for (String header : headers) {
            colSchema.add(header.trim());
            fileContent.put(header.trim(), new ArrayList<>());
        }
    }

    private void parseDataRow(String dataLine) {
        if (dataLine == null || dataLine.trim().isEmpty()) {
            return;
        }

        String[] values = dataLine.split("\\|");
        if (values.length != colSchema.size()) {
            System.err.println("WARNING: line number does not match - " + dataLine);
            return;
        }

        for (int i = 0; i < colSchema.size(); i++) {
            String columnName = colSchema.get(i);
            String value = values[i].trim();
            fileContent.get(columnName).add(value);
        }
    }

    public List<String> getColSchema() {
        return new ArrayList<>(colSchema);
    }

    public Map<String, List<String>> getFileContent() {
        Map<String, List<String>> copy = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : fileContent.entrySet()) {
            copy.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }
        return copy;
    }

    public List<String> getColumnData(String columnName) {
        List<String> data = fileContent.get(columnName);
        return data != null ? new ArrayList<>(data) : new ArrayList<>();
    }

    public int getRowCount() {
        if (fileContent.isEmpty()) {
            return 0;
        }
        return fileContent.values().iterator().next().size();
    }

    public List<String> getRow(int rowIndex) {
        List<String> row = new ArrayList<>();
        int rowCount = getRowCount();

        if (rowIndex < 0 || rowIndex >= rowCount) {
            throw new IndexOutOfBoundsException("Row index out of range: " + rowIndex);
        }

        for (String columnName : colSchema) {
            List<String> columnData = fileContent.get(columnName);
            if (columnData != null && rowIndex < columnData.size()) {
                row.add(columnData.get(rowIndex));
            }
        }

        return row;
    }

    public void printContent() {
        LOGGER.info("ColName: " + colSchema);
        LOGGER.info("Data content:");
        for (Map.Entry<String, List<String>> entry : fileContent.entrySet()) {
            LOGGER.info(entry.getKey() + ": " + entry.getValue());
        }
        LOGGER.info("Total row count: " + getRowCount());
    }
}
