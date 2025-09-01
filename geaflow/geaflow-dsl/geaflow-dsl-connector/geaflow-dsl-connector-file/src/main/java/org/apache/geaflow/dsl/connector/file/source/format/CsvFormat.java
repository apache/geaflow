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

package org.apache.geaflow.dsl.connector.file.source.format;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.common.util.TypeCastUtil;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import org.apache.geaflow.dsl.connector.file.source.SourceConstants;

public class CsvFormat implements FileFormat<Row> {

    private TextFormat txtFormat;

    private StructType dataSchema;

    private Configuration tableConf;

    @Override
    public String getFormat() {
        return SourceConstants.CSV;
    }

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema, FileSplit split) throws IOException {
        this.txtFormat = new TextFormat();
        this.txtFormat.init(tableConf, tableSchema, split);
        this.dataSchema = tableSchema.getDataSchema();
        this.tableConf = tableConf;
    }

    @Override
    public Iterator<Row> batchRead() throws IOException {
        Iterator<String> textIterator = txtFormat.batchRead();
        return new CsvIterator(textIterator, dataSchema, tableConf);
    }

    @Override
    public void close() throws IOException {
        txtFormat.close();
    }

    @Override
    public TableDeserializer<Row> getDeserializer() {
        return null;
    }

    private static class CsvIterator implements Iterator<Row> {

        private final Iterator<String> textIterator;

        private final StructType schema;

        private final int[] fieldIndices;

        private final String separator;

        public CsvIterator(Iterator<String> textIterator, StructType schema, Configuration tableConf) {
            this.textIterator = textIterator;
            this.schema = schema;
            this.separator = tableConf.getString(ConnectorConfigKeys.GEAFLOW_DSL_COLUMN_SEPARATOR);

            boolean skipHeader = tableConf.getBoolean(ConnectorConfigKeys.GEAFLOW_DSL_SKIP_HEADER);
            List<String> headerFields = new ArrayList<>();
            if (skipHeader) {
                if (textIterator.hasNext()) { // skip header
                    String header = textIterator.next();
                    headerFields = Lists.newArrayList(
                        StringUtils.splitByWholeSeparatorPreserveAllTokens(header, separator));
                }
            }
            fieldIndices = new int[schema.size()];
            if (headerFields.size() > 0) {
                int i = 0;
                for (TableField field : schema.getFields()) {
                    int index = headerFields.indexOf(field.getName());
                    if (index == -1) {
                        throw new GeaFlowDSLException("Field: '{}' is not exists in the csv "
                            + "header. header field is: {}", field.getName(),
                            StringUtils.join(headerFields, ","));
                    }
                    fieldIndices[i++] = index;
                }
            } else {
                for (int i = 0; i < schema.size(); i++) {
                    fieldIndices[i] = i;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return textIterator.hasNext();
        }

        @Override
        public Row next() {
            String line = textIterator.next();
            String[] fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, separator);
            Object[] selectFields = new Object[fieldIndices.length];
            for (int i = 0; i < selectFields.length; i++) {
                selectFields[i] = TypeCastUtil.cast(fields[fieldIndices[i]], schema.getType(i));
            }
            return ObjectRow.create(selectFields);
        }
    }
}
