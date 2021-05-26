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

import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.config.table.TableConfig;


public class BrokerReduceNode implements PlanTreeNode{

  private String _name = "BROKER_REDUCE";
  private String _havingFilter; // not sure if we want a tree structure
  private String _sort;
  private int _limit;
  private PlanTreeNode[] _childNodes = new PlanTreeNode[1];

  public BrokerReduceNode(QueryContext queryContext, TableConfig tableConfig){
    _havingFilter = queryContext.getHavingFilter() != null ? queryContext.getHavingFilter().toString() : null;
    _sort = queryContext.getOrderByExpressions() != null ? queryContext.getOrderByExpressions().toString() : null;
    _limit = queryContext.getLimit();
    _childNodes[0] = new ServerCombineNode(queryContext, tableConfig);
  }

  @Override
  public PlanTreeNode[] getChildNodes() {
    return _childNodes;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder(_name).append('(');
    if (_havingFilter != null) {
      stringBuilder.append("havingFilter").append(':').append(_havingFilter).append(',');
    }
    if (_sort != null) {
      stringBuilder.append("sort").append(':').append(_sort).append(',');
    }
    stringBuilder.append("limit:").append(_limit);
    return stringBuilder.append(')').toString();
  }
}
