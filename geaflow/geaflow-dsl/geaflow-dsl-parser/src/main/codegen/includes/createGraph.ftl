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

SqlCreate SqlCreateGraph(Span s, boolean replace):
{
    boolean ifNotExists = false;
    boolean isTemporary = false;
    final SqlIdentifier id;
    SqlNode vertex;
    final List<SqlNode> vertices = new ArrayList<SqlNode>();
    SqlNode edge;
    final List<SqlNode> edges = new ArrayList<SqlNode>();
    List<SqlTableProperty> propertyList = new ArrayList<SqlTableProperty>();
}
{
    [ <TEMPORARY> { isTemporary = true; } ]
    <GRAPH> [ <IF> <NOT> <EXISTS> { ifNotExists = true; } ]
    id = CompoundIdentifier()
    <LPAREN>
        (vertex = GraphVertex() { vertices.add(vertex);})
        ( <COMMA> vertex = GraphVertex() { vertices.add(vertex); } )*
        ( <COMMA> edge = GraphEdge() { edges.add(edge); } )
        ( <COMMA> edge = GraphEdge() { edges.add(edge); } )*
    <RPAREN>
    [
     <WITH>
        propertyList =  GraphPropertyList()
    ]
    {
        return new SqlCreateGraph(
                        s.end(this),
                        isTemporary,
                        ifNotExists,
                        id,
                        new SqlNodeList(vertices, s.addAll(vertices).pos()),
                        new SqlNodeList(edges, s.addAll(edges).pos()),
                        new SqlNodeList(propertyList, s.addAll(propertyList).pos()));
    }
}

SqlNode GraphVertex() :
{
  Span s;
  final SqlIdentifier vertexName;
  final List<SqlTableColumn> vertexColumns = new ArrayList<SqlTableColumn>();
  SqlTableColumn column;
  final SqlIdentifier identifier;
  SqlIdentifier usingTableName = null;
}
{
  <VERTEX> { s = span(); } vertexName = CompoundIdentifier()
  (
    (
      <LPAREN>
            column = GraphTableColumn() {vertexColumns.add(column);}
            (
                <COMMA> column = GraphTableColumn() {vertexColumns.add(column);}
            )*
      <RPAREN>
    ) {
            return new SqlVertex(s.add(this).pos(), vertexName,
                                      new SqlNodeList(vertexColumns, s.addAll(vertexColumns).pos()));
      }
    |
    (
      <USING> usingTableName = SimpleIdentifier()  <WITH> <ID>
      <LPAREN>
            identifier = CompoundIdentifier()
      <RPAREN>
    ) {
      return new SqlVertexUsing(getPos(), vertexName, usingTableName, identifier);
      }
  )
}

SqlNode GraphEdge() :
{
  Span s;
  final SqlIdentifier edgeName;
  final List<SqlTableColumn> edgeColumns = new ArrayList<SqlTableColumn>();
  SqlTableColumn column;
  SqlIdentifier usingTableName = null;
  SqlIdentifier sourceId = null;
  SqlIdentifier targetId = null;
  SqlIdentifier timeField = null;
}
{
  <EDGE> { s = span(); } edgeName = CompoundIdentifier()
  (
      (
        <LPAREN>
              column = GraphTableColumn() { edgeColumns.add(column); }
              (
                  <COMMA> column = GraphTableColumn() { edgeColumns.add(column); }
              )*
        <RPAREN>
      ) {
              return new SqlEdge(s.add(this).pos(), edgeName,
                                        new SqlNodeList(edgeColumns, s.addAll(edgeColumns).pos()),
                                        SqlNodeList.EMPTY);
        }
      |
      (
        <USING> usingTableName = SimpleIdentifier()  <WITH>
        (
          <ID>
          <LPAREN>
                sourceId = CompoundIdentifier()
                <COMMA>
                targetId = CompoundIdentifier()
          <RPAREN>
          [(
              <COMMA>
              <TIMESTAMP>
              <LPAREN>
                    timeField = CompoundIdentifier()
              <RPAREN>
          )]
        )
      ) {
        return new SqlEdgeUsing(getPos(), edgeName, usingTableName, sourceId, targetId, timeField,
                                SqlNodeList.EMPTY);
        }
    )
}

SqlTableColumn GraphTableColumn() :
{
   SqlParserPos pos;
   SqlIdentifier name = null;
   SqlDataTypeSpec type = null;
   SqlIdentifier typeFrom = null;
   SqlIdentifier category = null;
}
{
  {
    pos = getPos();
    category = new SqlIdentifier(ColumnCategory.NONE.getName(), getPos());
  }
  name = SimpleIdentifier()
  (
    (
        <FROM> typeFrom = SimpleIdentifier()
    )
    |
    (
        type = DataType()
        [ <NOT> <NULL> { type = type.withNullable(false); } ]
    )
  )
  [
    (
         <ID> { category = new SqlIdentifier(ColumnCategory.ID.getName(), getPos()); }
      |  <SOURCE> <ID>
              { category = new SqlIdentifier(ColumnCategory.SOURCE_ID.getName(), getPos()); }
      |  <DESTINATION> <ID>
              { category =
                new SqlIdentifier(ColumnCategory.DESTINATION_ID.getName(), getPos()); }
      |  <TIMESTAMP>
              { category = new SqlIdentifier(ColumnCategory.TIMESTAMP.getName(), getPos()); }
    )
  ] {
      return new SqlTableColumn(name, type, typeFrom, category, pos);
    }
}

List<SqlTableProperty> GraphPropertyList() :
{
    final List<SqlTableProperty> propertyList = new ArrayList<SqlTableProperty>();
    SqlTableProperty  property;
}
{
  <LPAREN>
   [
      property = GraphPropertyValue() { propertyList.add(property);}
       (
       <COMMA> property = GraphPropertyValue() { propertyList.add(property);}
       )*
   ]
   <RPAREN>
   {
       return propertyList;
   }
}

SqlTableProperty GraphPropertyValue() :
{
   SqlIdentifier key;
   SqlNode value;
}
{
   key = CompoundIdentifier()
   <EQ>
    (
      value = StringLiteral()
      |
      value = SpecialLiteral()
      |
      value = NumericLiteral()
    )
    {
        return new SqlTableProperty(key, value, getPos());
    }
}