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

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.util.SqlTypeUtil;

public class SqlTableColumn extends SqlCall {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("Table Column", SqlKind.OTHER_DDL);

    private SqlIdentifier name;
    private SqlDataTypeSpec type;
    private SqlIdentifier category;
    private SqlIdentifier typeFrom;

    public SqlTableColumn(SqlIdentifier name,
                          SqlDataTypeSpec type,
                          SqlIdentifier category,
                          SqlParserPos pos) {
        super(pos);
        this.name = name;
        this.type = Objects.requireNonNull(type);
        this.typeFrom = null;
        this.category = category;
    }

    public SqlTableColumn(SqlIdentifier name,
                          SqlDataTypeSpec type,
                          SqlIdentifier typeFrom,
                          SqlIdentifier category,
                          SqlParserPos pos) {
        super(pos);
        this.name = name;
        this.type = type;
        this.typeFrom = typeFrom;
        assert type != null || typeFrom != null;
        this.category = category;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(getName(), getType() != null ? getType() : getTypeFrom(), category);
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.name = (SqlIdentifier) operand;
                break;
            case 1:
                if (operand instanceof SqlDataTypeSpec) {
                    this.type = (SqlDataTypeSpec) operand;
                    this.typeFrom = null;
                } else {
                    this.type = null;
                    this.typeFrom = (SqlIdentifier) operand;
                }
                break;
            case 2:
                this.category = (SqlIdentifier) operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    @Override
    public void unparse(SqlWriter writer,
                        int leftPrec,
                        int rightPrec) {

        name.unparse(writer, leftPrec, rightPrec);
        writer.print(" ");
        if (type == null) {
            writer.keyword("from");
            typeFrom.unparse(writer, leftPrec, rightPrec);
        } else {
            type.unparse(writer, leftPrec, rightPrec);
        }

        if (category != null) {
            ColumnCategory category = getCategory();
            writer.print(" ");
            writer.keyword(category.name);
        }
    }

    public void validate() {
        if (type == null && typeFrom != null) {
            ColumnCategory columnCategory = ColumnCategory.of(this.category.toString());
            assert columnCategory == ColumnCategory.SOURCE_ID
                || columnCategory == ColumnCategory.DESTINATION_ID
                : "Only edge source/destination id field can use type from syntax.";
        }
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        this.validate();
        super.validate(validator, scope);
    }

    public SqlIdentifier getName() {
        return name;
    }

    public void setName(SqlIdentifier name) {
        this.name = name;
    }

    public SqlDataTypeSpec getType() {
        return type;
    }

    public SqlIdentifier getTypeFrom() {
        return typeFrom;
    }

    public ColumnCategory getCategory() {
        if (category == null) {
            return ColumnCategory.NONE;
        }
        return ColumnCategory.of(category.getSimple());
    }

    public TableField toTableField() {
        IType<?> columnType = SqlTypeUtil.convertType(type);
        Boolean nullable = type.getNullable();
        if (nullable == null) {
            nullable = true;
        }
        return toTableField(columnType, nullable);
    }

    public TableField toTableField(IType<?> columnType, boolean nullable) {
        String columnName = name.getSimple();
        return new TableField(columnName, columnType, nullable);
    }

    public enum ColumnCategory {
        NONE(""),
        ID("ID"),
        SOURCE_ID("SOURCE ID"),
        DESTINATION_ID("DESTINATION ID"),
        TIMESTAMP("TIMESTAMP");

        private final String name;

        ColumnCategory(String name) {
            this.name = Objects.requireNonNull(name);
        }

        public String getName() {
            return name;
        }

        public static ColumnCategory of(String name) {
            for (ColumnCategory category : values()) {
                if (category.name.equalsIgnoreCase(name)) {
                    return category;
                }
            }
            return NONE;
        }
    }
}
