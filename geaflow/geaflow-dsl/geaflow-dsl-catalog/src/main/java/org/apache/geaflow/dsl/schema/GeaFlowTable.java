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

package org.apache.geaflow.dsl.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.util.SqlTypeUtil;
import org.apache.geaflow.dsl.util.StringLiteralUtil;

public class GeaFlowTable extends AbstractTable implements Serializable {

    private final String instanceName;

    private final String name;

    private final List<TableField> fields;

    private final List<String> primaryFields;

    private final List<String> partitionFields;

    private final Map<String, String> config;

    private final boolean ifNotExists;

    private final boolean isTemporary;

    public GeaFlowTable(String instanceName, String name, List<TableField> fields, List<String> primaryFields,
                        List<String> partitionFields, Map<String, String> config,
                        boolean ifNotExists, boolean isTemporaryTable) {
        this.instanceName = Objects.requireNonNull(instanceName);
        this.name = Objects.requireNonNull(name, "name is null");
        this.fields = Objects.requireNonNull(fields, "fields is null");
        this.primaryFields = Objects.requireNonNull(primaryFields, "primaryFields is null");
        this.partitionFields = Objects.requireNonNull(partitionFields, "partitionFields is null");
        this.config = Objects.requireNonNull(config, "config is null");
        this.ifNotExists = ifNotExists;
        this.isTemporary = isTemporaryTable;
    }

    @Override
    public Statistic getStatistic() {
        return new GQLStatistic();
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        List<String> fieldNames = new ArrayList<>();
        List<RelDataType> fieldTypes = new ArrayList<>();
        for (TableField field : fields) {
            fieldNames.add(field.getName());
            fieldTypes.add(SqlTypeUtil.convertToRelType(field.getType(), field.isNullable(), typeFactory));
        }
        return typeFactory.createStructType(fieldTypes, fieldNames);
    }

    public TableSchema getTableSchema() {
        return new TableSchema(getDataSchema(), getPartitionSchema());
    }

    public StructType getDataSchema() {
        return new StructType(fields).dropRight(partitionFields.size());
    }

    public StructType getPartitionSchema() {
        List<TableField> pFields = new ArrayList<>(partitionFields.size());
        for (int i = fields.size() - partitionFields.size(); i < fields.size(); i++) {
            pFields.add(fields.get(i));
        }
        return new StructType(pFields);
    }

    public String getInstanceName() {
        return instanceName;
    }

    public String getName() {
        return name;
    }

    public List<TableField> getFields() {
        return fields;
    }

    public List<String> getPrimaryFields() {
        return primaryFields;
    }

    public List<String> getPartitionFields() {
        return partitionFields;
    }

    public List<Integer> getPartitionIndices() {
        StructType tableSchema = getTableSchema();
        return partitionFields.stream()
            .map(tableSchema::indexOf)
            .collect(Collectors.toList());
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    public String getTableType() {
        return Configuration.getString(DSLConfigKeys.GEAFLOW_DSL_TABLE_TYPE, config);
    }

    public Configuration getConfigWithGlobal(Configuration globalConf) {
        Map<String, String> conf = new HashMap<>(globalConf.getConfigMap());
        conf.putAll(this.config);
        return new Configuration(conf);
    }

    public Configuration getConfigWithGlobal(Configuration globalConf, Map<String, String> setOptions) {
        Map<String, String> conf = new HashMap<>(globalConf.getConfigMap());
        conf.putAll(setOptions);
        conf.putAll(this.config);
        return new Configuration(conf);
    }

    public boolean isPartitionField(int index) {
        return partitionFields.contains(fields.get(index).getName());
    }

    @Override
    public String toString() {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ");

        sql.append(name).append(" (\n");

        for (int i = 0; i < fields.size(); i++) {
            TableField field = fields.get(i);
            if (i > 0) {
                sql.append("\n\t,");
            } else {
                sql.append("\t");
            }
            sql.append(field.getName()).append("\t");
            sql.append(field.getType());
        }

        sql.append("\n)");

        if (config.size() > 0) {
            boolean first = true;
            sql.append(" WITH (\n");
            for (Map.Entry<String, String> entry : config.entrySet()) {
                if (!first) {
                    sql.append("\t,");
                } else {
                    sql.append("\t");
                }
                first = false;
                sql.append(entry.getKey()).append("=")
                    .append(StringLiteralUtil.escapeSQLString(entry.getValue()))
                    .append("\n");
            }
            sql.append(")");
        }
        return sql.toString();
    }

    public enum StoreType {
        FILE,
        CONSOLE;

        public static StoreType of(String value) {
            for (StoreType storeType : values()) {
                if (storeType.name().equalsIgnoreCase(value)) {
                    return storeType;
                }
            }
            throw new IllegalArgumentException("Illegal storeType: " + value);
        }
    }
}
