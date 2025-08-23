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

package org.apache.geaflow.dsl.connector.api.serde.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.util.TypeCastUtil;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;

public class TextDeserializer implements TableDeserializer<String> {

    private String lineSeparator;

    private String columnSeparator;

    private boolean isColumnTrim;

    private StructType schema;

    @Override
    public void init(Configuration conf, StructType schema) {
        this.lineSeparator = conf.getString(ConnectorConfigKeys.GEAFLOW_DSL_LINE_SEPARATOR);
        this.columnSeparator = conf.getString(ConnectorConfigKeys.GEAFLOW_DSL_COLUMN_SEPARATOR);
        this.isColumnTrim = conf.getBoolean(ConnectorConfigKeys.GEAFLOW_DSL_COLUMN_TRIM);
        this.schema = Objects.requireNonNull(schema);
    }

    @Override
    public List<Row> deserialize(String text) {
        if (text == null || text.isEmpty()) {
            return Collections.emptyList();
        }
        List<Row> rows = new ArrayList<>();
        String[] lines = StringUtils.splitByWholeSeparator(text, lineSeparator);
        for (String line : lines) {
            if (line.isEmpty() && schema.size() >= 1) {
                continue;
            }
            String[] fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, columnSeparator);
            if (schema.size() != fields.length) {
                throw new GeaFlowDSLException("Data fields size:{}, is not equal to the schema size:{}",
                    fields.length, schema.size());
            }
            Object[] values = new Object[schema.size()];
            for (int i = 0; i < values.length; i++) {
                String trimField = isColumnTrim ? StringUtils.trim(fields[i]) : fields[i];
                values[i] = TypeCastUtil.cast(trimField, schema.getType(i));
            }
            rows.add(ObjectRow.create(values));
        }
        return rows;
    }
}
