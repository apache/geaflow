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

public class CsvFileReader {

    List<String> colSchema;
    Map<String, List<String>> fileContent;
    long limit;

    // 构造函数
    public CsvFileReader(long limit) {
        this.colSchema = new ArrayList<>();
        this.fileContent = new HashMap<>();
        this.limit = limit;
    }

    /**
     * 从resources目录读取CSV文件
     * @param fileName 文件名
     * @throws IOException 读取文件异常
     */
    public void readCsvFile(String fileName) throws IOException {
        // 获取资源文件输入流
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName);
        if (inputStream == null) {
            throw new IOException("无法找到文件: " + fileName);
        }

        long count = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            boolean isFirstLine = true;

            while ((line = reader.readLine()) != null) {
                count++;
                if (isFirstLine) {
                    // 处理表头
                    parseHeader(line);
                    isFirstLine = false;
                } else {
                    // 处理数据行
                    parseDataRow(line);
                }
                if (count > limit) {
                    break;
                }
            }
        }
    }

    /**
     * 解析表头行
     * @param headerLine 表头行
     */
    private void parseHeader(String headerLine) {
        String[] headers = headerLine.split("\\|");

        // 清空之前的数据
        colSchema.clear();
        fileContent.clear();

        // 初始化列名和对应的列表
        for (String header : headers) {
            colSchema.add(header.trim());
            fileContent.put(header.trim(), new ArrayList<>());
        }
    }

    /**
     * 解析数据行
     * @param dataLine 数据行
     */
    private void parseDataRow(String dataLine) {
        if (dataLine == null || dataLine.trim().isEmpty()) {
            return;
        }

        String[] values = dataLine.split("\\|");

        // 确保值的数量与列数匹配
        if (values.length != colSchema.size()) {
            System.err.println("警告: 数据列数不匹配 - " + dataLine);
            return;
        }

        // 将每个值添加到对应的列列表中
        for (int i = 0; i < colSchema.size(); i++) {
            String columnName = colSchema.get(i);
            String value = values[i].trim();
            fileContent.get(columnName).add(value);
        }
    }

    // Getter方法
    public List<String> getColSchema() {
        return new ArrayList<>(colSchema);
    }

    public Map<String, List<String>> getFileContent() {
        // 返回一个副本以防止外部修改
        Map<String, List<String>> copy = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : fileContent.entrySet()) {
            copy.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }
        return copy;
    }

    /**
     * 获取指定列的所有数据
     * @param columnName 列名
     * @return 该列的数据列表
     */
    public List<String> getColumnData(String columnName) {
        List<String> data = fileContent.get(columnName);
        return data != null ? new ArrayList<>(data) : new ArrayList<>();
    }

    /**
     * 获取行数（不包括表头）
     * @return 行数
     */
    public int getRowCount() {
        if (fileContent.isEmpty()) {
            return 0;
        }
        // 获取任意一列的大小即可
        return fileContent.values().iterator().next().size();
    }

    /**
     * 获取指定行的数据
     * @param rowIndex 行索引（从0开始）
     * @return 该行的数据映射
     */
    public List<String> getRow(int rowIndex) {
        List<String> row = new ArrayList<>();
        int rowCount = getRowCount();

        if (rowIndex < 0 || rowIndex >= rowCount) {
            throw new IndexOutOfBoundsException("行索引超出范围: " + rowIndex);
        }

        for (String columnName : colSchema) {
            List<String> columnData = fileContent.get(columnName);
            if (columnData != null && rowIndex < columnData.size()) {
                row.add(columnData.get(rowIndex));
            }
        }

        return row;
    }

    /**
     * 打印文件内容（用于调试）
     */
    public void printContent() {
        System.out.println("列名: " + colSchema);
        System.out.println("数据内容:");
        for (Map.Entry<String, List<String>> entry : fileContent.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
        System.out.println("总行数: " + getRowCount());
    }
}
