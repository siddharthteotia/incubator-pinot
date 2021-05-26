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

import java.util.List;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.spi.config.table.TableConfig;


public class ServerCombineNode implements PlanTreeNode {

  private String _name = "SERVER_COMBINE";
  private PlanTreeNode[] _childNodes = new PlanTreeNode[1];

  // aggregation only
  // aggregation group by
  // distinct
  // selection
  // transformation apply
  // no transform
  public ServerCombineNode (QueryContext queryContext, TableConfig tableConfig) {
    if (QueryContextUtils.isAggregationQuery(queryContext)) {
      List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
      if (groupByExpressions != null) {
        // aggregate group-by -> transform -> project -> filter (may apply transform)
        _childNodes[0] = new AggregateGroupByNode(queryContext, tableConfig);
      } else {
        // aggregate only -> transform -> project -> filter (may apply transform)
        _childNodes[0] = new AggregateNode(queryContext, tableConfig);
      }
    } else if (QueryContextUtils.isSelectionQuery(queryContext)) {
      // select (omitted in the output) -> transform -> project -> filter
      Set<String> transforms = QueryContextUtils.generateTransforms(queryContext);
      if (!transforms.isEmpty()) {
        // transform
        _childNodes[0] = new ApplyTransformNode(queryContext, transforms, false, tableConfig);
      } else {
        // project
        _childNodes[0] = new ProjectNode(queryContext, tableConfig);
      }
    } else {
      assert QueryContextUtils.isDistinctQuery(queryContext);
      // distinct -> transform -> project -> filter
      _childNodes[0] = new DistinctNode(queryContext, tableConfig);
    }
  }

  @Override
  public PlanTreeNode[] getChildNodes() {
    return _childNodes;
  }

  @Override
  public String toString() {
    return _name;
  }

}
