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

package org.apache.pinot.broker.requesthandler;

import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.testng.Assert;
import org.testng.annotations.Test;


public class QueryValidationTest {

  /**
   * These DISTINCT queries are not supported
   * (1) SELECT sum(col1), min(col2), DISTINCT(col3, col4) FROM foo
   * (2) SELECT sum(col1), DISTINCT(col2, col3), min(col4) FROM foo
   * (3) SELECT DISTINCT(col1, col2), DISTINCT(col3) FROM foo
   *
   * (4) SELECT DISTINCT(col1, col2), sum(col3), min(col4) FROM foo
   *
   * (5) SELECT DISTINCT(col1, col2) FROM foo ORDER BY col1, col2
   * (6) SELECT DISTINCT(col1, col2) FROM foo GROUP BY col1
   *
   * (1), (2) and (3) are not valid SQL queries and so PQL won't support
   * them too.
   *
   * (4) is a valid SQL query for multi column distinct. It will output
   * exactly 1 row by taking [col1, col2, sum(col3) and min(col4)] as one entire column
   * set as an input into DISTINCT. However, we can't support it
   * since DISTINCT, sum and min are implemented as independent aggregation
   * functions. So unless the output of sum and min is piped into DISTINCT,
   * we can't execute this query.
   *
   * DISTINCT is currently not supported with ORDER BY and GROUP BY and
   * so we throw exceptions for (5) and (6)
   *
   * NOTE: There are other two other types of queries that should ideally not be
   * supported but we let them go through since that behavior has been there
   * from a long time
   *
   * SELECT DISTINCT(col1), col2, col3 FROM foo
   *
   * The above query is both a selection and aggregation query.
   * The reason this is a bad query is that DISTINCT(col1) will output
   * potentially less number of rows than entire dataset, whereas all
   * column values from col2 and col3 will be selected. So the output
   * does not make sense.
   *
   * However, when the broker request is built in {@link org.apache.pinot.pql.parsers.pql2.ast.SelectAstNode},
   * we check if it has both aggregation and selections and set the latter to NULL.
   * Thus we execute such queries as if the user had only specified the aggregation in
   * the query.
   *
   * SELECT DISTINCT(COL1), transform_func(col2) FROM foo
   *
   * The same reason is applicable to the above query too. It is a combination
   * of aggregation (DISTINCT) and selection (transform) and we just execute
   * the aggregation.
   *
   * Note that DISTINCT(transform_func(col)) is supported as in this case,
   * the output of transform_func(col) is piped into DISTINCT.
   * See {@link org.apache.pinot.queries.DistinctQueriesTest} for tests.
   */

  @Test
  public void testUnsupportedDistinctQueries() {
    Pql2Compiler compiler = new Pql2Compiler();

    String pql = "SELECT DISTINCT(col1, col2) FROM foo ORDER BY col1, col2";
    testUnsupportedQueriesHelper(compiler, pql, "DISTINCT with ORDER BY is currently not supported");

    pql = "SELECT DISTINCT(col1, col2) FROM foo GROUP BY col1";
    testUnsupportedQueriesHelper(compiler, pql, "DISTINCT with GROUP BY is currently not supported");

    pql = "SELECT sum(col1), min(col2), DISTINCT(col3, col4) FROM foo";
    testUnsupportedQueriesHelper(compiler, pql, "Aggregation functions cannot be used with DISTINCT");

    pql = "SELECT sum(col1), DISTINCT(col2, col3), min(col4) FROM foo";
    testUnsupportedQueriesHelper(compiler, pql, "Aggregation functions cannot be used with DISTINCT");

    pql = "SELECT DISTINCT(col1, col2), DISTINCT(col3) FROM foo";
    testUnsupportedQueriesHelper(compiler, pql, "Aggregation functions cannot be used with DISTINCT");

    pql = "SELECT DISTINCT(col1, col2), sum(col3), min(col4) FROM foo";
    testUnsupportedQueriesHelper(compiler, pql, "Aggregation functions cannot be used with DISTINCT");
  }

  private void testUnsupportedQueriesHelper(Pql2Compiler compiler, String query, String errorMessage) {
    try {
      BrokerRequest brokerRequest = compiler.compileToBrokerRequest(query);
      BaseBrokerRequestHandler.validateRequest(brokerRequest, 1000);
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(errorMessage));
    }
  }
}
