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

import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;


public class FilterNode implements PlanTreeNode{

  private String _name = "FILTER";
  private PlanTreeNode[] _childNodes;
  private String _operator;
  private String _predicate;

  public FilterNode(QueryContext queryContext, FilterContext filter, TableConfig tableConfig) {
    assert (filter != null);
    if (filter.getType().equals(FilterContext.Type.PREDICATE)) {
      // leaf filter
      _operator = filter.getPredicate().getType().toString();
      _predicate = filter.getPredicate().toString();

      Set<String> transforms = new HashSet<>();
      QueryContextUtils.collectTransforms(filter, transforms);
      if (!transforms.isEmpty()) {
        _childNodes = new PlanTreeNode[1];
        _childNodes[0] = new ApplyTransformNode(queryContext, transforms, true, tableConfig);
      } else {
        // no functions in the predicate -> the lhs has to be an identifier
        Predicate predicate = filter.getPredicate();
        ExpressionContext lhs = predicate.getLhs();
        String tableName = tableConfig.getTableName(); // with suffix
        IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
        _childNodes = new PlanTreeNode[1];
        String column = lhs.getIdentifier();
        String indexUsed = ExplainPlanUtils.getIndexUsed(column, predicate.getType(), indexingConfig);
        _childNodes[0] = new ScanNode(queryContext, column, tableName, indexUsed);
      }
    } else {
      // AND / OR
      _operator = filter.getType().toString();
      _childNodes = new PlanTreeNode[filter.getChildren().size()];
      for (int i = 0; i < _childNodes.length; i++) {
        FilterContext childFilter = filter.getChildren().get(i);
        _childNodes[i] = new FilterNode(queryContext, childFilter, tableConfig);
      }
    }
  }

  @Override
  public PlanTreeNode[] getChildNodes() {
    return _childNodes;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder(_name).append("(operator:").append(_operator);
    if (_predicate != null) {
      stringBuilder.append(",predicate:").append(_predicate);
    }
    return stringBuilder.append(')').toString();
  }
}
