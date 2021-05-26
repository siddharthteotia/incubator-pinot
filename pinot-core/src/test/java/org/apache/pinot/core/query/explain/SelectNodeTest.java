/**
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

package org.apache.pinot.core.query.explain;

import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class SelectNodeTest {

  IndexingConfig indexingConfig = new IndexingConfig();
  TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
  @Test
  public void testSelect() {
    {
      // SELECT *
      tableConfig.setIndexingConfig(indexingConfig);
      String query = "SELECT * FROM testTable";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);
      String selectNodeString = selectNode.toString();
      assertEquals(selectNodeString, "SELECT(selectList:ALL)");
//      StringBuilder stringBuilder = new StringBuilder();
//      buildPlan(selectNode, 0, stringBuilder);
//      System.out.println(query);
//      System.out.println(stringBuilder.toString());
    }

    {
      // only identifier
      tableConfig.setIndexingConfig(indexingConfig);
      String query = "SELECT col1, col2 FROM testTable WHERE a = 1.5 AND b = 1 ORDER BY col1 DESC LIMIT 200";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);
      String selectNodeString = selectNode.toString();
      assertEquals(selectNodeString, "SELECT(selectList:col1,col2)");
    }

    {
      // function + identifier
      tableConfig.setIndexingConfig(indexingConfig);
      String query = "SELECT col2, max(col1), min(col4) FROM testTable WHERE col3 = 10 GROUP BY col2";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);
      String selectNodeString = selectNode.toString();
      assertEquals(selectNodeString, "SELECT(selectList:col2,max(col1),min(col4))");
    }

    {
      // function + identifier + alias + literal
      tableConfig.setIndexingConfig(indexingConfig);
      String query = "SELECT col1, DATETIMECONVERT(col2), sum(col3) as col3_sum, 2"
          + "FROM testTable WHERE col5 in (1, 2, 3) GROUP BY col1, DATETIMECONVERT(col2) "
          + "HAVING sum(col3) > 100 "
          + "ORDER BY col1 DESC "
          + "LIMIT 100";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);
      String selectNodeString = selectNode.toString();
      // literal is represented as a string (?)
      assertEquals(selectNodeString, "SELECT(selectList:col1,datetimeconvert(col2),col3_sum,2)");
    }

  }

  @Test
  public void testAlias() {
    {
      // no alias
      String query = "SELECT col2, max(col1), min(col4) FROM testTable WHERE col3 = 10 GROUP BY col2";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);
      assertFalse(selectNode.getChildNodes()[0] instanceof UpdateAliasNode);
      assertTrue(selectNode.getChildNodes()[0] instanceof BrokerReduceNode);
    }

    {
      // function + identifier + alias
      String query = "SELECT col1, DATETIMECONVERT(col2) as conv_col2, sum(col3) as col3_sum "
          + "FROM testTable WHERE col5 in (1, 2, 3) GROUP BY col1, DATETIMECONVERT(col2) "
          + "HAVING sum(col3) > 100 "
          + "ORDER BY col1 DESC "
          + "LIMIT 100";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);
      UpdateAliasNode updateAliasNode = (UpdateAliasNode) selectNode.getChildNodes()[0];
      String aliasNodeString = updateAliasNode.toString();
      assertEquals(aliasNodeString, "UPDATE_ALIAS(datetimeconvert(col2)->conv_col2,sum(col3)->col3_sum)");
    }

    {
      // all alias
      String query = "SELECT col3 as A, max(col1) as B, min(col4) as C FROM testTable WHERE col3 = 10 GROUP BY col3";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);
      String aliasNodeString = selectNode.getChildNodes()[0].toString();
      assertEquals(aliasNodeString, "UPDATE_ALIAS(col3->A,max(col1)->B,min(col4)->C)");
    }
  }

  @Test
  public void testBrokerReduce() {
    {
      // simple broker reduce
      String query = "SELECT * FROM testTable";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);
      assertTrue(selectNode.getChildNodes()[0] instanceof BrokerReduceNode);
      BrokerReduceNode brokerReduceNode = (BrokerReduceNode) selectNode.getChildNodes()[0];
      assertEquals(brokerReduceNode.toString(), "BROKER_REDUCE(limit:10)");
    }

    {
      // only order by
      String query = "SELECT * FROM testTable ORDER BY col1, col2 desc, col3 asc";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);
      assertTrue(selectNode.getChildNodes()[0] instanceof BrokerReduceNode);
      BrokerReduceNode brokerReduceNode = (BrokerReduceNode) selectNode.getChildNodes()[0];
      assertEquals(brokerReduceNode.toString(), "BROKER_REDUCE(sort:[col1 ASC, col2 DESC, col3 ASC],limit:10)");
    }

    {
      // broker reduce (limit, having filter, order) + update alias
      String query = "SELECT col1, DATETIMECONVERT(col2) as conv_col2, sum(col3) as col3_sum "
          + "FROM testTable WHERE col5 in (1, 2, 3) GROUP BY col1, DATETIMECONVERT(col2) "
          + "HAVING sum(col3) > 100 "
          + "ORDER BY col1 DESC "
          + "LIMIT 100";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);
      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(selectNode, 0, stringBuilder);
      System.out.println(stringBuilder.toString());

      UpdateAliasNode updateAliasNode = (UpdateAliasNode) selectNode.getChildNodes()[0];
      String aliasNodeString = updateAliasNode.toString();
      assertEquals(aliasNodeString, "UPDATE_ALIAS(datetimeconvert(col2)->conv_col2,sum(col3)->col3_sum)");
      BrokerReduceNode brokerReduceNode = (BrokerReduceNode) updateAliasNode.getChildNodes()[0];
      assertEquals(brokerReduceNode.toString(), "BROKER_REDUCE(havingFilter:sum(col3) > 100,sort:[col1 DESC],limit:100)");
    }
  }

  @Test
  public void testOnlyFilter() {
    {
      // simple
      String query = "SELECT * FROM testTable WHERE col1 > 15";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      FilterContext filter = queryContext.getFilter();
      FilterNode filterNode = new FilterNode(queryContext, filter, tableConfig);
      String filterString = filterNode.toString();
      assertEquals(filterString, "FILTER(operator:RANGE,predicate:col1 > '15')");
      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(filterNode, 0, stringBuilder);
      System.out.println(stringBuilder.toString());
    }
    {
      // nested
      String query = "SELECT * FROM testTable "
          + "WHERE col1 > 15 "
          + "AND (DIV(bar, col2) BETWEEN 10 AND 20 "
          + "OR TEXT_MATCH(col3, 'potato'))";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      FilterContext filter = queryContext.getFilter();
      FilterNode filterNode = new FilterNode(queryContext, filter, tableConfig);
      String filterString = filterNode.toString();
      assertEquals(filterString, "FILTER(operator:AND)");
      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(filterNode, 0, stringBuilder);
      System.out.println(stringBuilder.toString());
    }
    {
      // nested
      String query = "SELECT * FROM testTable "
          + "WHERE col1 > 15 "
          + "AND (DIV(bar, col2) BETWEEN 10 AND 20 "
          + "OR TEXT_MATCH(col3, 'potato')) AND (col4 < 50) OR col5 = 100";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      FilterContext filter = queryContext.getFilter();
      FilterNode filterNode = new FilterNode(queryContext, filter, tableConfig);
       String filterString = filterNode.toString();
       assertEquals(filterString, "FILTER(operator:AND)");
      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(filterNode, 0, stringBuilder);
      System.out.println(stringBuilder.toString());
    }
  }

  @Test
  public void testProject() {
    {
      // prject ALL
      String query = "SELECT * FROM testTable WHERE foo > 15";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      ProjectNode projectNode = new ProjectNode(queryContext, tableConfig);
      String projectString = projectNode.toString();
      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(projectNode, 0, stringBuilder);
      System.out.println(stringBuilder.toString());
    }

    {
      // project identifiers
      String query = "SELECT col1, col2 FROM testTable WHERE foo > 15";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      ProjectNode projectNode = new ProjectNode(queryContext, tableConfig);
      String projectString = projectNode.toString();
      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(projectNode, 0, stringBuilder);
      System.out.println(stringBuilder.toString());
    }

    {
      // project cols
      String query = "SELECT col1, col2, 2 FROM testTable WHERE foo > 15";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      ProjectNode projectNode = new ProjectNode(queryContext, tableConfig);
      String projectString = projectNode.toString();
      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(projectNode, 0, stringBuilder);
      System.out.println(stringBuilder.toString());
    }
  }

  @Test
  public void testTransform() {
    {
      // simple
      String query = "SELECT ADD(foo, ADD(bar, 123)), SUB('456', foobar) "
          + "FROM testTable WHERE foo > 5"
          + "ORDER BY SUB(456, foobar)";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      ApplyTransformNode applyTransformNode =
          new ApplyTransformNode(queryContext, QueryContextUtils.generateTransforms(queryContext), false, tableConfig);
      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(applyTransformNode, 0, stringBuilder);
      System.out.println(stringBuilder.toString());

    }

    {
      // nested
      String query = "SELECT datetimeconvert(foo) FROM testTable "
          + "WHERE foo > 15 "
          + "AND (DIV(bar, foo) BETWEEN 10 AND 20 "
          + "OR TEXT_MATCH(foobar, 'potato'))";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      ApplyTransformNode applyTransformNode =
          new ApplyTransformNode(queryContext, QueryContextUtils.generateTransforms(queryContext), false, tableConfig);
      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(applyTransformNode, 0, stringBuilder);
      System.out.println(stringBuilder.toString());
    }
  }

  @Test
  public void testAggregateOnly() {
    {
      String query = "SELECT count(*), datetimeconvert(col1) FROM testTable WHERE a = 1";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      AggregateNode aggregateNode = new AggregateNode(queryContext, tableConfig);
      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(aggregateNode, 0, stringBuilder);
      System.out.println(stringBuilder.toString());
    }

    {
      // nested
      String query = "SELECT datetimeconvert(foo), count(*) FROM testTable "
          + "WHERE foo > 15 "
          + "AND (DIV(bar, foo) BETWEEN 10 AND 20 "
          + "OR TEXT_MATCH(foobar, 'potato'))";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      AggregateNode aggregateNode = new AggregateNode(queryContext, tableConfig);
      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(aggregateNode, 0, stringBuilder);
      System.out.println(stringBuilder.toString());
    }
  }

  @Test
  public void testServerCombine() {
    {
      // server_combine -> aggregate -> transform
      String query = "SELECT datetimeconvert(foo), count(*) FROM testTable "
          + "WHERE foo > 15 "
          + "AND (DIV(bar, foo) BETWEEN 10 AND 20 "
          + "OR TEXT_MATCH(foobar, 'potato'))";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);

      PlanTreeNode selectNode = new SelectNode(queryContext, tableConfig);
      PlanTreeNode brokerReduceNode = selectNode.getChildNodes()[0];
      assertTrue(brokerReduceNode instanceof BrokerReduceNode);
      PlanTreeNode serverCombineNode = brokerReduceNode.getChildNodes()[0];
      assertTrue(serverCombineNode instanceof ServerCombineNode);
      PlanTreeNode aggregateNode = serverCombineNode.getChildNodes()[0];
      assertTrue(aggregateNode instanceof AggregateNode);
      PlanTreeNode applyTransformNode = aggregateNode.getChildNodes()[0];
      assertTrue(applyTransformNode instanceof ApplyTransformNode);
      PlanTreeNode projectNode = applyTransformNode.getChildNodes()[0];
      assertTrue(projectNode instanceof ProjectNode);
      PlanTreeNode filterNode = projectNode.getChildNodes()[0];
      assertTrue(filterNode instanceof FilterNode);

//      StringBuilder stringBuilder = new StringBuilder();
//      buildPlan(selectNode, 0, stringBuilder);
//      System.out.println(query);
//      System.out.println(stringBuilder.toString());
    }

    {
      // server_combine -> select -> transform
      String query = "SELECT DATETIMECONVERT(col1), col2 FROM testTable WHERE a = 1.5 AND b = 1 ORDER BY col1 DESC LIMIT 200";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      PlanTreeNode selectNode = new SelectNode(queryContext, tableConfig);

//      StringBuilder stringBuilder = new StringBuilder();
//      buildPlan(selectNode, 0, stringBuilder);
//      System.out.println(query);
//      System.out.println(stringBuilder.toString());

    }

    {
      // server_combine -> aggregate_groupby -> project
      String query = "SELECT col2, max(col1), min(col4) FROM testTable WHERE col3 = 10 GROUP BY col2";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);

//      StringBuilder stringBuilder = new StringBuilder();
//      buildPlan(selectNode, 0, stringBuilder);
//      System.out.println(query);
//      System.out.println(stringBuilder.toString());

    }

    {
      // server_combine -> aggregate_groupby -> transform -> project
      String query = "SELECT DATETIMECONVERT(col2), max(col1), min(col4) FROM testTable "
          + "WHERE col3 = 10 GROUP BY DATETIMECONVERT(col2)";
      QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);

      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(selectNode, 0, stringBuilder);
      System.out.println(query);
      System.out.println(stringBuilder.toString());
    }


  }

  @Test
  public void testAll() {
    String query;
    QueryContext queryContext;
    {
      // simple
      query = "SELECT * FROM testTable";
      queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);

      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(selectNode, 0, stringBuilder);
      System.out.println(query);
      System.out.println(stringBuilder.toString());
    }

    {
      // filter
      query = "SELECT * FROM testTable WHERE a = 1.5 AND b = 1";
      queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);

      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(selectNode, 0, stringBuilder);
      System.out.println(query);
      System.out.println(stringBuilder.toString());
    }

    {
      // limit, order-by
      query = "SELECT col1, col2 FROM testTable WHERE a = 1.5 AND b > 1 ORDER BY col1 DESC LIMIT 200";
      queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);

      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(selectNode, 0, stringBuilder);
      System.out.println(query);
      System.out.println(stringBuilder.toString());
    }

    {
      // transform
      query = "SELECT DATETIMECONVERT(col1), col2 FROM testTable WHERE a = 1.5 AND b <= 1.6 ORDER BY col1 DESC LIMIT 200";
      queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);

      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(selectNode, 0, stringBuilder);
      System.out.println(query);
      System.out.println(stringBuilder.toString());
    }

    {
      // simple agg
      query = "SELECT count(*) FROM testTable WHERE a = 1";
      queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);

      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(selectNode, 0, stringBuilder);
      System.out.println(query);
      System.out.println(stringBuilder.toString());
    }

    {
      // agg group-by
      query = "SELECT col2, max(col1), min(col4) FROM testTable WHERE col3 = 10 GROUP BY col2";
      queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);

      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(selectNode, 0, stringBuilder);
      System.out.println(query);
      System.out.println(stringBuilder.toString());
    }

    {
      // agg groupby transform
      query = "SELECT DATETIMECONVERT(col2), max(col1), min(col4) FROM testTable WHERE col3 = 10 GROUP BY DATETIMECONVERT(col2)";
      queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);

      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(selectNode, 0, stringBuilder);
      System.out.println(query);
      System.out.println(stringBuilder.toString());
    }

    {
      // agg groupby orderby transform
      query = "SELECT max(col1), min(col4), DATETIMECONVERT(col2) FROM testTable "
          + "WHERE col3 = 10 "
          + "GROUP BY DATETIMECONVERT(col2) "
          + "ORDER BY max(col1) DESC";
      queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);

      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(selectNode, 0, stringBuilder);
      System.out.println(query);
      System.out.println(stringBuilder.toString());
    }

    {
      // agg groupby orderby having transform
      query = "SELECT max(col1), min(col4), DATETIMECONVERT(col2) FROM testTable WHERE col3 = 10 "
          + "GROUP BY DATETIMECONVERT(col2) HAVING max(col1) > 20 ORDER BY max(col1) DESC";
      queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);

      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(selectNode, 0, stringBuilder);
      System.out.println(query);
      System.out.println(stringBuilder.toString());
    }

    {
      // distinct
      query = "SELECT DISTINCT col1, col2 FROM testTable WHERE col3 = 10 ";
      queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);

      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(selectNode, 0, stringBuilder);
      System.out.println(query);
      System.out.println(stringBuilder.toString());
    }

    {
      // alias agg groupby orderby having transform
      query = "SELECT col1, DATETIMECONVERT(col2), sum(col3) as col3_sum, count(*), distinctCount(col4) "
          + "FROM testTable WHERE col5 in ('a', 'b', 'c') AND col6 in (1, 2, 3) "
          + "GROUP BY col1, DATETIMECONVERT(col2) "
          + "HAVING sum(col3) > 100 "
          + "ORDER BY col1 DESC LIMIT 100";
      queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);

      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(selectNode, 0, stringBuilder);
      System.out.println(query);
      System.out.println(stringBuilder.toString());
    }

    {
      // Filter with transform
      query = "SELECT * FROM testTable WHERE foo > 15 AND (DIV(bar, foo) BETWEEN 10 AND 20 OR TEXT_MATCH(foobar, 'potato'))";
      queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);

      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(selectNode, 0, stringBuilder);
      System.out.println(query);
      System.out.println(stringBuilder.toString());
    }

    {
      // Aggregation group-by with transform, order-by
      query = "SELECT SUB(foo, bar), bar, SUM(ADD(foo, bar)) "
          + "FROM testTable GROUP BY SUB(foo, bar), bar "
          + "ORDER BY SUM(ADD(foo, bar)), SUB(foo, bar) DESC LIMIT 20";
      queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
      SelectNode selectNode = new SelectNode(queryContext, tableConfig);

      StringBuilder stringBuilder = new StringBuilder();
      buildPlan(selectNode, 0, stringBuilder);
      System.out.println(query);
      System.out.println(stringBuilder.toString());
    }

//    {
//      // case
//      query = "SELECT col1, "
//          + "CASE WHEN col1 > 30 THEN 'value of col1 > 30' WHEN col1 < 30 THEN 'value of col1 < 30' ELSE 'col1 is 30' AS col1_info "
//          + "FROM testTable";
//      queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);
//      SelectNode selectNode = new SelectNode(queryContext);
//
//      StringBuilder stringBuilder = new StringBuilder();
//      buildPlan(selectNode, 0, stringBuilder);
//      System.out.println(query);
//      System.out.println(stringBuilder.toString());
//    }

  }

  /*
  * Builds a plan tree with node being the root
  * */
  private void buildPlan(PlanTreeNode node, int indentation, StringBuilder sb) {
    if (node == null) {
      return;
    }
    for (int i=0; i < indentation; i++){
      sb.append(" ");
    }
    sb.append(node).append("\n");
    for (PlanTreeNode child : node.getChildNodes()) {
      buildPlan(child, indentation + 4, sb);
    }
  }
}
