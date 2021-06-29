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
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.spi.config.table.IndexingConfig;


public class ExplainPlanUtils {

  private ExplainPlanUtils() {
  }

  public static String getIndexUsed(String column, Predicate.Type predicateType, IndexingConfig indexingConfig) {
    String indexUsed = "FULL_SCAN";
    HashSet<String> invertedIndexColumns = new HashSet<>();
    if (indexingConfig.getInvertedIndexColumns() != null) {
      invertedIndexColumns.addAll(indexingConfig.getInvertedIndexColumns());
    }
    HashSet<String> rangeIndexColumns = new HashSet<>();
    if (indexingConfig.getRangeIndexColumns() != null) {
      rangeIndexColumns.addAll(indexingConfig.getRangeIndexColumns());
    }
    HashSet<String> jsonIndexColumns = new HashSet<>();
    if (indexingConfig.getJsonIndexColumns() != null) {
      jsonIndexColumns.addAll(indexingConfig.getJsonIndexColumns());
    }
    HashSet<String> textIndexColumns = new HashSet<>();
    // TODO: no text index scan in indexingConfig?
    boolean isSorted = false;
    if (indexingConfig.getSortedColumn() != null) {
      isSorted = column.equals(indexingConfig.getSortedColumn().get(0));
    }
    switch (predicateType) {
      case RANGE:
        // range index or sorted index can be used
        if (isSorted) {
          indexUsed = "SORTED_INDEX_SCAN";
        } else if (rangeIndexColumns.contains(column)) {
          indexUsed = "RANGE_INDEX_SCAN";
        }
      case JSON_MATCH:
        // json index scan can be used
        if (jsonIndexColumns.contains(column)) {
          indexUsed = "JSON_INDEX_SCAN";
        }
      case TEXT_MATCH:
        // text index scan scan be used
      case REGEXP_LIKE:
        // ?
      case IS_NULL:
        // ?
      case IS_NOT_NULL:
        // ?
      default:
        // EQ, NOT_EQ, IN, NOT_IN
        // inverted or sorted can be used
        if (isSorted) {
          indexUsed = "SORTED_INDEX_SCAN";
        } else if (invertedIndexColumns.contains(column)) {
          indexUsed = "INVERTED_INDEX_SCAN";
        }
    }
    return indexUsed;
  }
}
