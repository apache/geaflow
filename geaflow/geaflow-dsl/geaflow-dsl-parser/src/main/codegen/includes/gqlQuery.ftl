SqlCall GQLMatchStatement() :
{
      SqlCall statement = null;
}
{
      statement = SqlMatchPattern(statement)
      (
        (
          statement = SqlLetStatement(statement) (<COMMA> statement = SqlLetStatement(statement))*
          [
            [ <NEXT> ]
            statement = SqlMatchPattern(statement)
          ]
        )
        |
        (
           [ <NEXT> ]
           statement = SqlMatchPattern(statement)
        )
      )*
      (
          statement = SqlReturn(statement)
          [
              <THEN>
              statement = SqlFilter(statement)
          ]
      )*
      {
          return statement;
      }
}

SqlCall GQLGraphAlgorithmCall() :
{
      SqlCall statement = null;
      SqlIdentifier algorithm;
      SqlNodeList parameters = null;
      SqlNodeList yieldList = null;
      SqlNode parameter = null;
      SqlIdentifier label = null;
      Span s = Span.of();
}
{
      <CALL>
      algorithm = SimpleIdentifier()
      <LPAREN>
          {
              List<SqlNode> inputParameters = new ArrayList<SqlNode>();
          }
          [
              parameter = Literal()
              {
                  inputParameters.add(parameter);
              }
              (
                  <COMMA> parameter = Literal()
                  {
                      inputParameters.add(parameter);
                  }
              )*
              {
                  parameters = new SqlNodeList(inputParameters, s.addAll(inputParameters).pos());
              }
          ]
      <RPAREN>
      <YIELD>
      <LPAREN>
          {
              List<SqlIdentifier> outputYieldList = new ArrayList<SqlIdentifier>();
          }
          [
              label = SimpleIdentifier() { outputYieldList.add(label);  }
              (
                  <COMMA> label = SimpleIdentifier() { outputYieldList.add(label);  }
              )*
              {
                  yieldList = new SqlNodeList(outputYieldList, s.addAll(outputYieldList).pos());
              }
          ]
      <RPAREN>
      {
          statement = new SqlGraphAlgorithmCall(s.end(this), statement, algorithm, parameters, yieldList);
      }
      (
          statement = SqlReturn(statement)
          [
              <THEN>
              statement = SqlFilter(statement)
          ]
      )*
      {
          return statement;
      }
}

SqlFilterStatement SqlFilter(SqlNode from) :
{
    SqlNode condition = null;
    Span s = Span.of();
}
{
    <FILTER>
    condition = Expression(ExprContext.ACCEPT_SUB_QUERY)
    {
        return new SqlFilterStatement(s.end(this), from, condition);
    }
}

SqlReturnStatement SqlReturn(SqlNode from) :
{
    List<SqlNode> selectList;
    List<SqlNode> keywordList = new ArrayList<SqlNode>();
    SqlNodeList gqlReturnKeywordList;
    SqlNodeList groupBy = null;
    SqlNodeList orderBy = null;
    SqlNode start = null;
    SqlNode count = null;
    Span s = Span.of();
}
{
    <RETURN>
    [
        <DISTINCT> { keywordList.add(GQLReturnKeyword.DISTINCT.symbol(getPos())); }
        |
        <ALL> { keywordList.add(GQLReturnKeyword.ALL.symbol(getPos())); }
    ]
    selectList = SelectList()
    groupBy = GroupByOpt()
    [ orderBy = OrderBy(true) ]
    [
        // Postgres-style syntax. "LIMIT ... OFFSET ..."
        <LIMIT>
        (
            // MySQL-style syntax. "LIMIT start, count"
            start = UnsignedNumericLiteralOrParam()
            <COMMA> count = UnsignedNumericLiteralOrParam()
        |
            count = UnsignedNumericLiteralOrParam()
        |
            <ALL>
        )
    ]
    [
        <OFFSET> start = UnsignedNumericLiteralOrParam()
    ]
    [
        // SQL:2008-style syntax. "OFFSET ... FETCH ...".
        // If you specify both LIMIT and FETCH, FETCH wins.
        <FETCH> ( <FIRST> | <NEXT> ) count = UnsignedNumericLiteralOrParam() <ONLY>
    ]
    {
        gqlReturnKeywordList = keywordList.isEmpty() ? null
            : new SqlNodeList(keywordList, s.addAll(keywordList).pos());
        return new SqlReturnStatement(s.end(this), gqlReturnKeywordList, from,
            new SqlNodeList(selectList, Span.of(selectList).pos()),
            groupBy, orderBy, start, count);
    }
}

SqlNodeList SqlMatchNodePropertySpecification() :
{
    List<SqlNode> propertySpecificationList = null;
    SqlIdentifier variable = null;
    SqlNode expr = null;
    Span s = Span.of();
}
{
      {
          propertySpecificationList = new ArrayList<SqlNode>();
      }
      variable = SimpleIdentifier() { propertySpecificationList.add(variable);  }
      <COLON>
      expr = Expression(ExprContext.ACCEPT_NON_QUERY) { propertySpecificationList.add(expr);}
      (
          <COMMA>
          variable = SimpleIdentifier() { propertySpecificationList.add(variable);  }
          <COLON>
          expr = Expression(ExprContext.ACCEPT_NON_QUERY) { propertySpecificationList.add(expr);}
      )*
      {
          return new SqlNodeList(propertySpecificationList, s.addAll(propertySpecificationList).pos());
      }
}

SqlCall SqlMatchNode() :
{
    SqlIdentifier variable = null;
    SqlNodeList labels = null;
    SqlIdentifier label = null;
    SqlNodeList propertySpecification = null;
    SqlNode condition = null;
    Span s = Span.of();
}
{
    <LPAREN>
    [ variable = SimpleIdentifier() ]
    {
        List<SqlNode> labelList = new ArrayList<SqlNode>();
    }
    [
        ( <COLON> )
        label = SimpleIdentifier() { labelList.add(label);  }
        (
            <VERTICAL_BAR> label = SimpleIdentifier() { labelList.add(label);  }
        )*
        {
            labels = new SqlNodeList(labelList, s.addAll(labelList).pos());
        }
    ]
    [
        (
          <LBRACE>
          propertySpecification = SqlMatchNodePropertySpecification()
          <RBRACE>
        )
      |
        (
          <WHERE>
          condition = Expression(ExprContext.ACCEPT_NON_QUERY)
        )
    ]

    <RPAREN>

    {
        return new SqlMatchNode(s.end(this), variable, labels, propertySpecification, condition);
    }

}

SqlCall SqlMatchEdge() :
{
    SqlIdentifier variable = null;
    SqlNodeList labels = null;
    SqlIdentifier label = null;
    SqlNodeList propertySpecification = null;
    SqlNode condition = null;
    Span s = Span.of();
    EdgeDirection direction = null;
    int minHop = 1;
    int maxHop = 1;
}
{
    { direction = EdgeDirection.BOTH; }
    [ <LT> { direction = EdgeDirection.IN; } ]
    <MINUS>
    [
        <LBRACKET>

            [ variable = SimpleIdentifier() ]

            {
                List<SqlNode> labelList = new ArrayList<SqlNode>();
            }
            [
                ( <COLON> )
                label = SimpleIdentifier() { labelList.add(label);  }
                (
                    <VERTICAL_BAR> label = SimpleIdentifier() { labelList.add(label);  }
                )*
                {
                    labels = new SqlNodeList(labelList, s.addAll(labelList).pos());
                }
            ]
            [
                (
                  <LBRACE>
                  propertySpecification = SqlMatchNodePropertySpecification()
                  <RBRACE>
                )
              |
                (
                  <WHERE>
                  condition = Expression(ExprContext.ACCEPT_NON_QUERY)
                )
            ]
        <RBRACKET>
        <MINUS>
    ]
    [
        <GT> { direction = direction == EdgeDirection.IN ? EdgeDirection.BOTH : EdgeDirection.OUT ; }
    ]
    [
      <LBRACE> { minHop = -1; maxHop = -1; }
        <UNSIGNED_INTEGER_LITERAL> { minHop = Integer.parseInt(token.image); maxHop = minHop;}
        [
          (<COMMA> <UNSIGNED_INTEGER_LITERAL>) { maxHop = Integer.parseInt(token.image); }
          |
          (<COMMA>) { maxHop = -1; }
        ]
      <RBRACE>
    ]
    {
        return new SqlMatchEdge(s.end(this), variable, labels, propertySpecification, condition,
        direction, minHop, maxHop);
    }
}

SqlPathPattern SqlPathPatternWithAlias() :
{
   SqlIdentifier pathAlias = null;
   SqlPathPattern pathPattern;
   Span s = Span.of();
}
{
  [ pathAlias = SimpleIdentifier() <EQ> ]
  pathPattern = SqlPathPattern()
  {
    return new SqlPathPattern(s.end(this), pathPattern.getPathNodes(), pathAlias);
  }
}

SqlPathPattern SqlPathPattern() :
{
    Span s = Span.of();
    List<SqlNode> nodeList = new ArrayList<SqlNode>();
    SqlNodeList pathNodes;
    SqlNode nodeOrEdge = null;
}
{

    nodeOrEdge = SqlMatchNode()  { nodeList.add(nodeOrEdge); }
    (
        nodeOrEdge = SqlMatchEdge()  { nodeList.add(nodeOrEdge); }
        nodeOrEdge = SqlMatchNode()  { nodeList.add(nodeOrEdge); }
    )*
    {
        pathNodes = new SqlNodeList(nodeList, s.addAll(nodeList).pos());
        return new SqlPathPattern(s.end(this), pathNodes, null);
    }
}

SqlCall SqlUnionPathPattern() :
{
    Span s = Span.of();
    SqlCall left = null;
    SqlCall right = null;
    SqlCall union = null;
    boolean distinct = true;
}
{
    left = SqlPathPatternWithAlias()
    (
        (
            ( <VERTICAL_BAR> <PLUS> <VERTICAL_BAR> { distinct = false; } )
            |
            ( <VERTICAL_BAR> { distinct = true; } )
        )
        right = SqlPathPatternWithAlias()
        {
            union = new SqlUnionPathPattern(s.end(this), left, right, distinct);
            left = union;
        }
    )*
    {
        return union != null ? union : left;
    }
}

SqlMatchPattern SqlMatchPattern(SqlNode preMatch) :
{
    Span s = Span.of();
    List<SqlNode> pathList = new ArrayList<SqlNode>();
    SqlNodeList graphPattern;
    SqlNode pathPattern = null;
    SqlNode condition = null;
    SqlNodeList orderBy = null;
    SqlNode count = null;
    boolean optional = false;
}
{
    [ <OPTIONAL> { optional = true; } ] <MATCH>
    pathPattern = SqlUnionPathPattern()  { pathList.add(pathPattern); }
    ( <COMMA> pathPattern = SqlUnionPathPattern()  { pathList.add(pathPattern); } )*

    [
        <WHERE>
        condition = Expression(ExprContext.ACCEPT_SUB_QUERY)
    ]

    [ orderBy = OrderBy(true) ]
    [
        <LIMIT>
        (
            count = UnsignedNumericLiteralOrParam()
        |
            <ALL>
        )
    ]
    {
        graphPattern = new SqlNodeList(pathList, s.addAll(pathList).pos());
        return new SqlMatchPattern(s.end(this), preMatch, graphPattern, condition, orderBy, count, optional);
    }
}

SqlLetStatement SqlLetStatement(SqlNode from) :
{
    Span s = Span.of();
    SqlIdentifier leftVar;
    SqlNode expression;
    boolean isGlobal = false;
}
{
    <LET> [<GLOBAL> { isGlobal = true; }] leftVar = CompoundIdentifier()
    (<EQ> | <AS>) expression = Expression(ExprContext.ACCEPT_SUB_QUERY)
    {
        return new SqlLetStatement(s.end(this), from, leftVar, expression, isGlobal);
    }
}
