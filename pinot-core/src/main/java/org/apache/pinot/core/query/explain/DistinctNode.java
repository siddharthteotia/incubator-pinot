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

import java.util.Set;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.spi.config.table.TableConfig;


public class DistinctNode implements PlanTreeNode{

  private String _name = "DISTINCT";
  private QueryContext _queryContext;
  private String[] _keys;
  private PlanTreeNode[] _childNodes = new PlanTreeNode[1];

  public DistinctNode(QueryContext queryContext, TableConfig tableConfig) {
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    assert aggregationFunctions != null && aggregationFunctions.length == 1
        && aggregationFunctions[0] instanceof DistinctAggregationFunction;
    DistinctAggregationFunction distinctAggregationFunction= (DistinctAggregationFunction) aggregationFunctions[0];
    _queryContext = queryContext;
    _keys = distinctAggregationFunction.getColumns();

    Set<String> transforms = QueryContextUtils.generateTransforms(queryContext);
    if (!transforms.isEmpty()) {
      // transform
      _childNodes[0] = new ApplyTransformNode(queryContext, transforms, false, tableConfig);
    } else {
      // project
      _childNodes[0] = new ProjectNode(queryContext, tableConfig);
    }
  }
  @Override
  public PlanTreeNode[] getChildNodes() {
    return _childNodes;
  }
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder(_name).append("(keyColumns:");
    if (_keys.length > 0) {
      stringBuilder.append(_keys[0]);
      for (int i = 1; i < _keys.length; i++) {
        stringBuilder.append(',').append(_keys[i]);
      }
    }
    return stringBuilder.append(')').toString();
  }
}
