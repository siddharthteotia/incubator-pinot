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
package org.apache.pinot.pql.parsers.pql2.ast;

import org.apache.pinot.common.function.FunctionDefinitionRegistry;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.pql.parsers.Pql2CompilationException;


/**
 * AST node for a list of columns.
 */
public class OutputColumnListAstNode extends BaseAstNode {
  private boolean _star;

  public boolean isStar() {
    return _star;
  }

  @Override
  public void addChild(AstNode childNode) {
    if (childNode instanceof StarColumnListAstNode) {
      _star = true;
    } else {
      super.addChild(childNode);
    }
  }

  /**
   * Check for following style PQLs and raise syntax error
   *
   * These 4 queries are not valid SQL queries as well so PQL won't support them too
   * (1) SELECT sum(col1), min(col2), DISTINCT(col3, col4)
   * (2) SELECT col1, col2, DISTINCT(col3) FROM foo
   * (3) SELECT DISTINCT(col1, col2), DISTINCT(col3) FROM foo
   * (4) SELECT timeConvert(DaysSinceEpoch,'DAYS','SECONDS'), DISTINCT(DaysSinceEpoch) FROM foo
   *
   * These 3 queries are either both selection and aggregation query
   * or the query does not make sense from result point of view (like 6)
   * (5) SELECT DISTINCT(col1), col2, col3 FROM foo
   * (6) SELECT DISTINCT(col1), sum(col3), min(col4) FROM foo
   * (7) SELECT DISTINCT(DaysSinceEpoch), timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM foo
   *
   * SQL versions of the above queries:
   *
   * (1) SELECT sum(col1), min(col2), DISTINCT col3, col4
   * (2) SELECT col1, col2, DISTINCT col3 FROM foo
   * (3) SELECT DISTINCT col1, col2, DISTINCT col3 FROM foo
   * (4) SELECT timeConvert(DaysSinceEpoch,'DAYS','SECONDS'), DISTINCT DaysSinceEpoch FROM foo
   *
   * 1, 2, 3 and 4 will still not be supported in compliance with SQL
   *
   * (5) SELECT DISTINCT col1, col2, col3 FROM foo
   * will be supported as it effectively becomes a multi column distinct
   *
   * (6) SELECT DISTINCT col1, sum(col3), min(col4) FROM foo
   * although a valid SQL syntax for multi column distinct, if we decide to support
   * them, we will have to do sum and min as transform functions which is not the case today.
   * In any case, not a helpful query.
   *
   * (7) SELECT DISTINCT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM foo
   * again a valid SQL syntax for multi column distinct, we can support this since timeConvert
   * is a valid supported transform function.
   */
  private void validate() {
    boolean identifierPresent = false;
    boolean distinctPresent = false;
    boolean functionPresent = false;
    if (hasChildren()) {
      for (AstNode child : getChildren()) {
        if (child instanceof OutputColumnAstNode) {
          if (child.hasChildren()) {
            for (AstNode selectChild : child.getChildren()) {
              if (selectChild instanceof IdentifierAstNode) {
                if (distinctPresent) {
                  throw new Pql2CompilationException("Syntax error: SELECT list columns should be part of DISTINCT clause");
                } else {
                  identifierPresent = true;
                }
              } else if (selectChild instanceof FunctionCallAstNode) {
                FunctionCallAstNode function = (FunctionCallAstNode)selectChild;
                if (function.getName().equalsIgnoreCase("distinct")) {
                  if (identifierPresent) {
                    throw new Pql2CompilationException("Syntax error: SELECT list columns should be part of DISTINCT clause");
                  } else if (functionPresent) {
                    throw new Pql2CompilationException("Syntax error: Functions cannot be used with DISTINCT clause");
                  } else if (distinctPresent) {
                    throw new Pql2CompilationException("Syntax error: DISTINCT clause can be used only once in a query");
                  } else {
                    distinctPresent = true;
                  }
                } else {
                  if (distinctPresent) {
                    throw new Pql2CompilationException("Syntax error: Functions cannot be used with DISTINCT clause");
                  } else {
                    functionPresent = true;
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  @Override
  public void updateBrokerRequest(BrokerRequest brokerRequest) {
    validate();
    sendBrokerRequestUpdateToChildren(brokerRequest);
  }

  @Override
  public void updatePinotQuery(PinotQuery pinotQuery) {
    sendPinotQueryUpdateToChildren(pinotQuery);
  }
}
