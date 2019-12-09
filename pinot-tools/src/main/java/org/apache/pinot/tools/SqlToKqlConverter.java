package org.apache.pinot.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.List;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.pql.parsers.pql2.ast.AstNode;
import org.apache.pinot.pql.parsers.pql2.ast.BetweenPredicateAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.BooleanOperatorAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.ComparisonPredicateAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.FunctionCallAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.GroupByAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.IdentifierAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.InPredicateAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.LiteralAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.OutputColumnAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.OutputColumnListAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.PredicateAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.PredicateListAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.SelectAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.StarColumnListAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.StarExpressionAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.StringLiteralAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.WhereAstNode;

public class SqlToKqlConverter {

  public void convert(String queryDir, String queryFileName) throws Exception {

    File queryFile = new File(queryDir + "/" + queryFileName);
    InputStream inputStream = new FileInputStream(queryFile);
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    String query;
    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(queryDir + "/" + "queries.kql")));
    int count = 0;
    while ((query = reader.readLine()) != null) {
      convertHelper(query, out);
      count++;
      System.out.println("query: " + count);
    }
    out.flush();
  }

  private String convertHelper(String sql, PrintWriter out) {
    AstNode root = Pql2Compiler.buildAst(sql);
    SelectAstNode selectAstNode = (SelectAstNode)root;
    String filter = null;
    String selectList = null;
    String groupBy = null;
    List<? extends AstNode> selectChildren = selectAstNode.getChildren();
    for (AstNode child : selectChildren) {
      if (child instanceof OutputColumnListAstNode) {
        // handle select list
        selectList = rewriteSelectList((OutputColumnListAstNode)child);
      } else if (child instanceof WhereAstNode) {
        // handle where
        filter = rewriteFilter((WhereAstNode)child);
      } else if (child instanceof GroupByAstNode) {
        // handle group by
        groupBy = rewriteGroupBy((GroupByAstNode)child);
      }  else if (child instanceof StarColumnListAstNode) {
        // TODO: handle select *
        throw new UnsupportedOperationException("select * is not supported");
      } else {
        // TODO: handle order by
        // for now throw exception instead of generating incorrect query
        throw new UnsupportedOperationException("Order by is currently not supported");
      }
    }

    StringBuilder genQuery = new StringBuilder();

    // TABLE
    genQuery.append("pinot_test");

    // WHERE
    if (filter != null) {
      filter = filter.trim();
      genQuery.append(" | ").append(filter);
    }

    // SUMMARIZE
    if (groupBy != null) {
      selectList = selectList.trim();
      if (selectList.contains("count")) {
        selectList = selectList.replaceAll("count\\(.*\\)", "agg_count = count()");
      }
      genQuery.append(" | summarize ").append(" ").append(selectList).append(" by ").append(groupBy);
      if (selectAstNode.isHasTopClause()) {
        genQuery.append(" | top ").append(selectAstNode.getTopN()).append(" ").append("by ").append("agg_count");
      } else {
        genQuery.append(" | top ").append(10);
      }
    }

    // LIMIT
    if (selectAstNode.isHasLimitClause()) {
      if (selectAstNode.getRecordLimit() > 0) {
        genQuery.append(" | take ").append(selectAstNode.getRecordLimit());
      } else {
        genQuery.append(" | take ").append(10);
      }
    }

    // PROJECT
    if (selectList != null && groupBy == null) {
      if (selectList.startsWith("count")) {
        genQuery.append(" | ").append("count");
      } else {
        genQuery.append(" | ").append("project ").append(selectList);
      }
    }

    String result = genQuery.toString().trim();
    out.println(result);


    return result;
  }

  private String rewriteGroupBy(GroupByAstNode groupByAstNode) {
    StringBuilder groupBy = new StringBuilder();
    List<? extends AstNode> children = groupByAstNode.getChildren();
    int count = 0;
    for (AstNode groupByChild : children) {
      String column = ((IdentifierAstNode)groupByChild).getName();
      if (count > 0) {
        groupBy.append(", ");
      }
      groupBy.append(column);
      count++;
    }
    return groupBy.toString();
  }

  private String rewriteFilter(WhereAstNode whereAstNode) {
    StringBuilder filter = new StringBuilder();
    filter.append("where ");
    PredicateListAstNode predicateListAstNode = (PredicateListAstNode)whereAstNode.getChildren().get(0);
    int numChildren = predicateListAstNode.getChildren().size();
    List<? extends AstNode> predicateList = predicateListAstNode.getChildren();
    for (int i = 0; i < numChildren; i += 2) {
      PredicateAstNode predicate = (PredicateAstNode) predicateList.get(i);
      rewritePredicate(predicate, filter);
      BooleanOperatorAstNode nextOperator;
      if (i + 1 < numChildren) {
        nextOperator = (BooleanOperatorAstNode) predicateList.get(i + 1);
        rewriteBooleanOperator(nextOperator, filter);
      }
    }
    return filter.toString();
  }

  private void rewriteBooleanOperator(BooleanOperatorAstNode operatorAstNode, StringBuilder filter) {
    if (operatorAstNode.name().equalsIgnoreCase("AND")) {
      filter.append(" and ");
    } else {
      filter.append(" or ");
    }
  }

  private void rewritePredicate(PredicateAstNode predicateAstNode, StringBuilder filter) {
    /// get column name participating in the predicate
    String columnName = predicateAstNode.getIdentifier();
    LiteralAstNode literal;
    if (predicateAstNode instanceof ComparisonPredicateAstNode) {
      // handle COMPARISON
      ComparisonPredicateAstNode comparisonPredicate = (ComparisonPredicateAstNode)predicateAstNode;
      // get operator: <, >, <=, >=, != ....
      String operator = comparisonPredicate.getOperand();
      // get right hand side literal
      literal = comparisonPredicate.getLiteral();
      // build comparison predicate using the column name, operator and literal
      // e.g id <= 2000
      if (operator.equals("=")) {
        operator = "==";
      }
      filter.append(columnName).append(" ").append(operator).append(" ");
      rewriteLiteral(literal, filter);
    } else if (predicateAstNode instanceof BetweenPredicateAstNode) {
      // handle BETWEEN
      List<? extends AstNode> betweenChildren = predicateAstNode.getChildren();
      // append column name for BETWEEN and BETWEEN operator itself
      filter.append(columnName).append(" ").append("between(");
      int count = 0;
      for (AstNode betweenChild: betweenChildren) {
        literal = (LiteralAstNode)betweenChild;
        if (count > 0) {
          // separate two operands for BETWEEN with AND
          // e.g timestamp BETWEEN 1000 AND 1001
          filter.append("..");
        }
        rewriteLiteral(literal, filter);
        count++;
      }
      filter.append(") ");
    } else if (predicateAstNode instanceof InPredicateAstNode) {
      // handle IN
      List<? extends AstNode> inChildren = predicateAstNode.getChildren();
      // append column name for IN and IN operator itself
      filter.append(columnName).append(" ").append("in ").append("(");
      int count  = 0;
      for (AstNode betweenChild: inChildren) {
        literal = (LiteralAstNode)betweenChild;
        if (count > 0) {
          // separate two operands for IN with ","
          // e.g timestamp IN (a,b,c,d)
          filter.append(",");
        }
        rewriteLiteral(literal, filter);
        count++;
      }
      // finish the IN predicate
      filter.append(")");
    } else {
      // TODO: handle parenthesised predicate
      // for now throw exception as opposed to generating incorrect query
      throw new UnsupportedOperationException("predicate ast node: " + predicateAstNode.getClass() + " not supported");
    }
  }

  private void rewriteLiteral (LiteralAstNode literalAstNode, StringBuilder sb) {
    String literalValue = literalAstNode.getValueAsString();
    if (literalAstNode instanceof StringLiteralAstNode) {
      // quote string literals
      sb.append("\"").append(literalValue).append("\"");
    } else {
      // numeric literals
      sb.append(literalValue);
    }
  }

  private String rewriteSelectList(OutputColumnListAstNode outputColumnListAstNode) {
    StringBuilder selectList = new StringBuilder();
    List<? extends AstNode> outputChildren = outputColumnListAstNode.getChildren();
    int numOutputColumns = outputChildren.size();
    int columnIndex = 0;
    for (AstNode outputChild : outputChildren) {
      if (outputChild instanceof OutputColumnAstNode) {
        // handle SELECT COL1, COL2 ....
        List<? extends AstNode> children = outputChild.getChildren();
        // OutputColumnAstNode represents an output column in the select list
        // the actual output column is represented by the child node of OutputColumnAstNode
        // and there can only be 1 such child
        // handle the exact type of output column -- identifier or a function
        rewriteSelectListColumn(children.get(0), selectList, null, columnIndex, numOutputColumns);
      } else {
        throw new UnsupportedOperationException("Invalid type of child node for OuptutColumnListAstNode");
      }
      columnIndex++;
    }
    return selectList.toString();
  }

  private void rewriteSelectListColumn(
      AstNode output,
      StringBuilder selectList,
      AstNode parent,
      int columnIndex,
      int numOutputColumns) {
    if (output instanceof IdentifierAstNode) {
      // OUTPUT COLUMN is identifier (column name)
      IdentifierAstNode identifier = (IdentifierAstNode)output;
      String columnName = identifier.getName();
      if (parent instanceof FunctionCallAstNode) {
        // this column is part of a parent function expression in the select list
        selectList.append(columnIndex);
      } else {
        // this column is a standalone column in the select list
        // so simply add the derived column name to select list
        if (columnIndex <= numOutputColumns - 2) {
          // multi column select list then separate with comma and space
          selectList.append(columnName).append(", ");
        } else {
          // single column select list or last column in the select list
          selectList.append(columnName);
        }
      }
    } else if (output instanceof FunctionCallAstNode) {
      // OUTPUT COLUMN is function
      // handle function nesting by recursing and build the expression incrementally
      // e.g1 SUM(C1)
      // 1. SUM(
      // 2. recurse for C1
      // 3. SUM(DERIVED_C1
      // 4. recursion over
      // 5. SUM(DERIVED_C1)
      //
      // e.g2 SUM(ADD(C1,C2))
      // 1. SUM(
      // 2. recurse for ADD(C1,C2)
      // 3. SUM(ADD(
      // 4. recurse for C1
      // 5. SUM(ADD(DERIVED_C1
      // 6. append "," -> SUM(ADD(DERIVED_C1,
      // 7. recurse for C2
      // 8. SUM(ADD(DERIVED_C1,DERIVED_C2
      // 9. recursion for add's operands finishes
      // 10. finish add expression -> SUM(ADD(DERIVED_C1,DERIVED_C2)
      // 11. recursion for sum's operands finishes
      // 12. finish sum expression -> SUM(ADD(DERIVED_C1,DERIVED_C2))
      // 13. DONE
      FunctionCallAstNode function = (FunctionCallAstNode)output;
      List<? extends AstNode> functionOperands = function.getChildren();
      String name = function.getName();
      selectList.append(name).append("(");
      int count = 0;
      for (AstNode functionOperand : functionOperands) {
        if (count > 0) {
          // add "," before handling the next operand
          selectList.append(",");
        }
        rewriteSelectListColumn(functionOperand, selectList, function, columnIndex, numOutputColumns);
        count++;
      }
      // finish this function expression at the end of recursion
      if (!(parent instanceof FunctionCallAstNode)) {
        if (columnIndex <= numOutputColumns - 2) {
          selectList.append("), ");
        } else {
          selectList.append(")");
        }
      } else {
        selectList.append(")");
      }
    } else if (output instanceof StarExpressionAstNode) {
      // COUNT(*)
      selectList.append("*");
    } else {
      throw new UnsupportedOperationException("Literals are not supported in output columns");
    }
  }

  public static void main (String [] args) throws Exception {
    SqlToKqlConverter converter = new SqlToKqlConverter();
    converter.convert("/Users/steotia/Desktop/kusto/snapActivity/queryDir/broker_17725", "queries.generated");
  }
}
