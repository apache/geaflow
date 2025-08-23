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

package org.apache.geaflow.dsl.sqlnode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.commons.lang.StringUtils;
import org.apache.geaflow.dsl.util.SqlTypeUtil;

/**
 * Parse tree node that represents a CREATE TABLE statement.
 */
public class SqlCreateTable extends SqlCreate {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

    private SqlIdentifier name;
    private SqlNodeList columns;
    private SqlNodeList properties;
    private SqlNodeList primaryKeys;
    private SqlNodeList partitionFields;
    private final boolean isTemporary;

    /**
     * Creates a SqlCreateTable.
     */
    public SqlCreateTable(SqlParserPos pos,
                          boolean isTemporary,
                          boolean ifNotExists,
                          SqlIdentifier name,
                          SqlNodeList columns,
                          SqlNodeList properties,
                          SqlNodeList primaryKeys,
                          SqlNodeList partitionFields) {
        super(OPERATOR, pos, false, ifNotExists);
        this.name = name;
        this.columns = columns;
        this.properties = properties;
        this.primaryKeys = primaryKeys;
        this.partitionFields = partitionFields;
        this.isTemporary = isTemporary;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(getName(), getColumns(), getProperties(),
            getPrimaryKeys(), getPartitionFields());
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.name = (SqlIdentifier) operand;
                break;
            case 1:
                this.columns = (SqlNodeList) operand;
                break;
            case 2:
                this.properties = (SqlNodeList) operand;
                break;
            case 3:
                this.primaryKeys = (SqlNodeList) operand;
                break;
            case 4:
                this.partitionFields = (SqlNodeList) operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }


    public void setProperties(SqlNodeList properties) {
        this.properties = properties;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (isTemporary) {
            writer.keyword("TEMPORARY");
        }
        writer.keyword("TABLE");
        if (super.ifNotExists) {
            writer.keyword("IF");
            writer.keyword("NOT");
            writer.keyword("EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);
        if (columns.size() >= 0) {
            final SqlWriter.Frame frame =
                writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
            writer.newlineAndIndent();
            writer.print("  ");
            if (columns.size() > 0) {
                for (int i = 0; i < columns.size(); i++) {
                    if (i > 0) {
                        writer.print(",");
                        writer.newlineAndIndent();
                        writer.print("  ");
                    }
                    columns.get(i).unparse(writer, leftPrec, rightPrec);
                }
            }
            writer.newlineAndIndent();
            writer.endList(frame);
        }
        if (partitionFields != null && partitionFields.size() > 0) {
            writer.keyword("PARTITIONED");
            writer.keyword("BY");
            writer.print("(");
            boolean first = true;
            for (SqlNode partitionField : partitionFields) {
                if (!first) {
                    writer.print(",");
                }
                first = false;
                partitionField.unparse(writer, 0, 0);
            }
            writer.print(")");
        }
        if (properties != null) {
            writer.keyword("WITH");
            final SqlWriter.Frame with =
                writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
            writer.newlineAndIndent();
            for (int i = 0; i < properties.size(); i++) {
                if (i > 0) {
                    writer.print(",");
                    writer.newlineAndIndent();
                }
                properties.get(i).unparse(writer, leftPrec, rightPrec);
            }

            writer.newlineAndIndent();
            writer.endList(with);
        }
    }

    /**
     * Sql syntax validation.
     */
    public void validate() throws ValidationException {
        Map<String, Boolean> columnNameMap = new HashMap<>();
        if (columns != null) {
            for (SqlNode column : columns) {
                SqlTableColumn sqlTableColumn = (SqlTableColumn) column;
                String columnName = sqlTableColumn.getName().getSimple();
                if (columnNameMap.get(columnName) == null) {
                    columnNameMap.put(columnName, true);
                } else {
                    throw new ValidationException(
                        "duplicate column name " + "[" + columnName + "], at " + column.getParserPosition());
                }

                SqlDataTypeSpec typeSpec = sqlTableColumn.getType();
                try {
                    SqlTypeUtil.convertType(typeSpec);
                } catch (UnsupportedOperationException e) {
                    throw new ValidationException(
                        "not support type " + "[" + typeSpec + "], at " + column.getParserPosition());
                }
            }
        }

        if (properties != null) {
            for (int i = 0; i < properties.size(); i++) {
                SqlTableProperty property = (SqlTableProperty) properties.get(i);
                if (property.getKey() == null || StringUtils.isEmpty(property.getKey().toString())) {
                    throw new ValidationException(
                        "property key is null or empty string at " + property.getParserPosition());
                }

            }
        }
    }

    public SqlIdentifier getName() {
        return name;
    }

    public SqlNodeList getColumns() {
        return columns;
    }

    public SqlNodeList getProperties() {
        return properties;
    }

    public SqlNodeList getPrimaryKeys() {
        return primaryKeys;
    }

    public SqlNodeList getPartitionFields() {
        return partitionFields;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }
}
