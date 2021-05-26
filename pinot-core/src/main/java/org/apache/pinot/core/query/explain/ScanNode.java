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


public class ScanNode implements PlanTreeNode{

  private String _name;
  // scan nodes are leaves
  private PlanTreeNode[] _childNodes = new PlanTreeNode[0];
  private String _column;
  private String _tableName;

  public ScanNode(QueryContext queryContext, String column, String tableName, String indexUsed) {
    _name = indexUsed;
    _column = column;
    _tableName = tableName;
  }

  @Override
  public PlanTreeNode[] getChildNodes() {
    return _childNodes;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder(_name).append("(table:");
    stringBuilder.append(_tableName).append(',').append("column:").append(_column);
    return stringBuilder.append(')').toString();
  }
}
