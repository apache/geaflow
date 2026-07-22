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

package org.apache.geaflow.dsl.connector.doris;

import com.google.gson.Gson;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;

public class DorisUtils {

    private static final Gson GSON = new Gson();

    /**
     * Serialize a row to a single csv line using the given column separator. Null fields are
     * rendered as Doris's null placeholder ("\N").
     */
    public static String rowToCsv(Row row, StructType schema, String columnSeparator) {
        List<TableField> fields = schema.getFields();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                builder.append(columnSeparator);
            }
            Object value = row.getField(i, fields.get(i).getType());
            if (value == null) {
                builder.append(DorisConstants.NULL_VALUE);
            } else {
                builder.append(value);
            }
        }
        return builder.toString();
    }

    /**
     * Serialize a row to a json object string keyed by the column names.
     */
    public static String rowToJson(Row row, StructType schema) {
        List<TableField> fields = schema.getFields();
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Object value = row.getField(i, fields.get(i).getType());
            map.put(fields.get(i).getName(), value);
        }
        return GSON.toJson(map);
    }
}
