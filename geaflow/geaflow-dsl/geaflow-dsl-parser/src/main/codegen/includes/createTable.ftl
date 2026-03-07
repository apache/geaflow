<#--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

void TableColumn(List<SqlNode> list) :
{
    SqlParserPos pos;
    SqlIdentifier name = null;
    SqlDataTypeSpec type;
    SqlIdentifier category = null;
}
{
    {
        pos = getPos();
        category = new SqlIdentifier(ColumnCategory.NONE.getName(), getPos());
    }
    name = SimpleIdentifier()
    type = DataType()
    [ <NOT> <NULL> { type = type.withNullable(false); } ]
    {
        SqlTableColumn tableColumn = new SqlTableColumn(name, type, category, pos);
        list.add(tableColumn);
    }
}

void PropertyValue(List<SqlNode> list) :
{
    SqlIdentifier key;
    SqlNode value;
}
{
    key = CompoundIdentifier()

    <EQ>

    (
          value = StringLiteral()
        | value = SpecialLiteral()
        | value = NumericLiteral()
    )
    {
        SqlTableProperty property = new SqlTableProperty(key, value, getPos());
        list.add(property);
    }
}

void PropertyList(List<SqlNode> list) :
{
    <#--SqlParserPos pos;-->
}
{
    <LPAREN>
    [
        PropertyValue(list)
        (
            <COMMA> PropertyValue(list)
        )*
    ]
    <RPAREN>
}

void PrimaryKey(List<SqlNode> list) :
{
    SqlIdentifier name;
}
{
    name = SimpleIdentifier()
    {
        list.add(name);
    }
}

void PrimaryKeyList(List<SqlNode> list) :
{
}
{
    [
        PrimaryKey(list)
        (
            <COMMA> PrimaryKey(list)
        )*
    ]
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    boolean ifNotExists = false;
    boolean isTemporary = false;
    final SqlIdentifier id;
    final SqlNodeList columns;
    SqlNodeList propertyList = null;
    SqlNodeList primaryKeyList = null;
    SqlNode partitionField;
    List<SqlNode> partitionFields = new ArrayList();
    SqlNodeList partitionFieldList = null;
}
{
    [ <TEMPORARY> { isTemporary = true; } ]
    <TABLE> [ <IF> <NOT> <EXISTS> { ifNotExists = true; } ]
    id = CompoundIdentifier()

    {
        List<SqlNode> colList = new ArrayList();
    }

    <LPAREN>

    TableColumn(colList)
    (
        <COMMA> TableColumn(colList)
    )*
    {
        columns = new SqlNodeList(colList, s.addAll(colList).pos());
    }
    [
        {
            List<SqlNode> pkList = new ArrayList();
        }
        <COMMA> <PRIMARY> <KEY>

        <LPAREN>
        (
            PrimaryKeyList(pkList)
        )
        <RPAREN>
        {
            primaryKeyList = new SqlNodeList(pkList, s.addAll(pkList).pos());
        }
    ]

    <RPAREN>

    {
        List<SqlNode> proList = new ArrayList();
    }
    [
      <PARTITIONED> <BY>
      <LPAREN>
        partitionField = SimpleIdentifier() {
          partitionFields.add(partitionField);
        }
        (
          <COMMA> partitionField = SimpleIdentifier() {
             partitionFields.add(partitionField);
          }
        )*
      <RPAREN>
    ]
    [
        <WITH> PropertyList(proList) {  propertyList = new SqlNodeList(proList, s.addAll(proList).pos()); }
    ]
    {
        partitionFieldList = new SqlNodeList(partitionFields, s.addAll(partitionFields).pos());
        return new SqlCreateTable(s.end(this), isTemporary, ifNotExists, id, columns, propertyList,
        primaryKeyList, partitionFieldList);
    }
}