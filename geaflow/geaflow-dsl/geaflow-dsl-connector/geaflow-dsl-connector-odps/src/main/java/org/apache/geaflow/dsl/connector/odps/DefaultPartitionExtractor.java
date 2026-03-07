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

package org.apache.geaflow.dsl.connector.odps;

import com.aliyun.odps.Column;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.StructType;

public class DefaultPartitionExtractor implements PartitionExtractor {

    private static final String DEFAULT_SEPARATOR_PATTERN = "[,/]";
    private static final String QUOTE_SEPARATOR_PATTERN = "[`'\"]";
    private static final String EQUAL_SEPARATOR = "=";
    private static final String COMMA_SEPARATOR = ",";
    private static final String SLASH_SEPARATOR = "/";
    private static final String DYNAMIC_KEY_PREFIX = "$";

    // partition spec separator
    private final String separator;
    // all partition keys
    private final String[] keys;
    // dynamic fields index
    private final int[] columns;
    // dynamic field types
    private final IType<?>[] types;
    // constant fields, values.length should be equal to keys.length
    // if values[i] is null, it means the i-th key is a dynamic field
    private final String[] values;

    /**
     * Create a partition extractor.
     * @param partitionColumns partition columns
     * @param schema the input schema
     * @return the partition extractor
     */
    public static PartitionExtractor create(List<Column> partitionColumns, StructType schema) {
        if (partitionColumns == null || partitionColumns.isEmpty()) {
            return row -> "";
        }
        int[] columns = new int[partitionColumns.size()];
        IType<?>[] types = new IType<?>[partitionColumns.size()];
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < partitionColumns.size(); i++) {
            String partitionColumn = partitionColumns.get(i).getName();
            int index = schema.indexOf(partitionColumn);
            if (index < 0) {
                throw new IllegalArgumentException("Partition column " + partitionColumn + " not found in schema");
            }
            columns[i] = index;
            types[i] = schema.getType(index);
            sb.append(partitionColumn).append(EQUAL_SEPARATOR).append(DYNAMIC_KEY_PREFIX)
                    .append(partitionColumn).append(COMMA_SEPARATOR);
        }
        return new DefaultPartitionExtractor(sb.substring(0, sb.length() - 1), columns, types);
    }

    /**
     * Create a partition extractor.
     * @param spec partition spec, like "dt=$dt,hh=$hh"
     * @param schema the input schema
     * @return the partition extractor
     */
    public static PartitionExtractor create(String spec, StructType schema) {
        if (spec == null || spec.isEmpty()) {
            return row -> "";
        }
        String[] groups = spec.split(DEFAULT_SEPARATOR_PATTERN);
        List<Integer> index = new ArrayList<>();
        List<IType<?>> types = new ArrayList<>();
        for (String group : groups) {
            String[] kv = group.split(EQUAL_SEPARATOR);
            if (kv.length != 2) {
                throw new IllegalArgumentException("Invalid partition spec.");
            }
            String k = kv[0].trim();
            String v = unquoted(kv[1].trim());
            if (k.isEmpty() || v.isEmpty()) {
                throw new IllegalArgumentException("Invalid partition spec.");
            }
            if (v.startsWith(DYNAMIC_KEY_PREFIX)) {
                int val = schema.indexOf(v.substring(1));
                if (val != -1) {
                    index.add(val);
                    types.add(schema.getType(val));
                }
            }
        }
        return new DefaultPartitionExtractor(spec, index.stream().mapToInt(i -> i).toArray(), types.toArray(new IType[0]));
    }

    public DefaultPartitionExtractor(String spec, int[] columns, IType<?>[] types) {
        this.columns = columns;
        this.types = types;
        if (spec == null) {
            throw new IllegalArgumentException("Argument 'spec' cannot be null");
        }
        String[] groups = spec.split(DEFAULT_SEPARATOR_PATTERN);
        this.separator = spec.contains(COMMA_SEPARATOR) ? COMMA_SEPARATOR : SLASH_SEPARATOR;
        this.keys = new String[groups.length];
        this.values = new String[groups.length];
        for (int i = 0; i < groups.length; i++) {
            String[] kv = groups[i].split(EQUAL_SEPARATOR);
            if (kv.length != 2) {
                throw new IllegalArgumentException("Invalid partition spec.");
            }
            String k = kv[0].trim();
            String v = unquoted(kv[1].trim());
            if (k.isEmpty() || v.isEmpty()) {
                throw new IllegalArgumentException("Invalid partition spec.");
            }
            this.keys[i] = k;
            this.values[i] = v.startsWith(DYNAMIC_KEY_PREFIX) ? null : v;
        }
    }

    /**
     * Unquote the string.
     * @param s the string
     * @return the unquoted string
     */
    public static String unquoted(String s) {
        return s.replaceAll(QUOTE_SEPARATOR_PATTERN, "");
    }

    @Override
    public String extractPartition(Row row) {
        StringBuilder sb = new StringBuilder();
        // dynamic field
        int col = 0;
        for (int i = 0; i < keys.length; i++) {
            sb.append(keys[i]).append(EQUAL_SEPARATOR);
            if (values[i] == null) {
                sb.append(row.getField(columns[col], types[col]));
                col++;
            } else {
                sb.append(values[i]);
            }
            if (i < keys.length - 1) {
                sb.append(separator);
            }
        }
        return sb.toString();
    }
}
