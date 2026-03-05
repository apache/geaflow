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

SqlVertexConstruct SqlVertexConstruct():
{
  List<SqlNode> operands = new ArrayList();
  SqlIdentifier key;
  SqlNode value;
  Span s = Span.of();
}
{
  <VERTEX> <LBRACE>
      key = SimpleIdentifier() <EQ> value = Expression(ExprContext.ACCEPT_SUB_QUERY)
      {
         operands.add(key);
         operands.add(value);
      }
      (
        <COMMA>
        key = SimpleIdentifier() <EQ> value = Expression(ExprContext.ACCEPT_SUB_QUERY)
        {
           operands.add(key);
           operands.add(value);
         }
      )*
    <RBRACE>
    {
      return new SqlVertexConstruct(operands.toArray(new SqlNode[]{}), s.end(this));
    }
}


SqlEdgeConstruct SqlEdgeConstruct():
{
  List<SqlNode> operands = new ArrayList();
  SqlIdentifier key;
  SqlNode value;
  Span s = Span.of();
}
{
  <EDGE> <LBRACE>
      key = SimpleIdentifier() <EQ> value = Expression(ExprContext.ACCEPT_SUB_QUERY)
      {
        operands.add(key);
        operands.add(value);
      }
      (
        <COMMA>
        key = SimpleIdentifier() <EQ> value = Expression(ExprContext.ACCEPT_SUB_QUERY)
        {
           operands.add(key);
           operands.add(value);
        }
      )*

      <RBRACE>
      {
        return new SqlEdgeConstruct(operands.toArray(new SqlNode[]{}), s.end(this));
      }
}

SqlPathPatternSubQuery SqlPathPatternSubQuery():
{
  SqlPathPattern pathPattern;
  SqlNode returnValue = null;
  Span s = Span.of();
}
{
  pathPattern = SqlPathPattern()
  [
    <NAMED_ARGUMENT_ASSIGNMENT> returnValue = Expression(ExprContext.ACCEPT_NON_QUERY)
  ]
  {
    return new SqlPathPatternSubQuery(pathPattern, returnValue, s.end(this));
  }
}


