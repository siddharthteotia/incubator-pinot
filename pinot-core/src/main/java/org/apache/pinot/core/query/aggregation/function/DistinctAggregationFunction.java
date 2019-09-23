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
package org.apache.pinot.core.query.aggregation.function;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.operator.transform.TransformBlockDataFetcher;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.DistinctTable;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.pql.parsers.pql2.ast.FunctionCallAstNode;


/**
 * DISTINCT clause in SQL is implemented as function in the
 * execution engine of Pinot.
 */
public class DistinctAggregationFunction implements AggregationFunction<DistinctTable, Comparable> {

  private final DistinctTable _distinctTable;
  private final String[] columnNames;
  private FieldSpec.DataType[] _dataTypes;
  private boolean init = false;
  private Stopwatch _aggregateWatch = Stopwatch.createUnstarted();

  DistinctAggregationFunction(String multiColumnExpression, int limit) {
    _distinctTable = new DistinctTable(limit);
    columnNames = multiColumnExpression.split(FunctionCallAstNode.DISTINCT_MULTI_COLUMN_SEPARATOR);
    _dataTypes = new FieldSpec.DataType[columnNames.length];
  }

  @Nonnull
  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCT;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String column) {
    return AggregationFunctionType.DISTINCT.getName() + "_" + column;
  }

  @Nonnull
  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public void accept(@Nonnull AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Nonnull
  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public void aggregate(int records, AggregationResultHolder aggregationResultHolder,
      @Nonnull BlockValSet... blockValSets) {
    Preconditions.checkArgument(blockValSets.length == columnNames.length, "Error invalid number of block value sets");

    //_aggregateWatch.start();
    if (!init) {
      for (int i = 0; i < blockValSets.length; i++) {
        _dataTypes[i] = blockValSets[i].getValueType();
      }
      _distinctTable.setProjectedColumnNames(columnNames);
      _distinctTable.setProjectedColumnTypes(_dataTypes);
      init = true;
    }

    // TODO: Follow up PR will make few changes to start using DictionaryBasedAggregationOperator
    // for DISTINCT queries without filter.
    TransformBlockDataFetcher transformBlockDataFetcher = new TransformBlockDataFetcher(blockValSets, new Dictionary[0], new TransformResultMetadata[0]);

    int rowIndex = 0;
    while (rowIndex < records) {
      Object[] columnData = transformBlockDataFetcher.getRow(rowIndex);
      _distinctTable.addKey(new Key(columnData));
      ++rowIndex;
    }

    //_aggregateWatch.stop();
  }

  @Nonnull
  @Override
  public DistinctTable extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return _distinctTable;
  }

  @Nonnull
  @Override
  public DistinctTable merge(DistinctTable inProgressMergedResult, DistinctTable newResultToMerge) {
    // do the union
    final Iterator<Key> iterator = newResultToMerge.getIterator();
    while (iterator.hasNext()) {
      final Key key = iterator.next();
      inProgressMergedResult.addKey(key);
    }

    return inProgressMergedResult;
  }

  @Nonnull
  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
   throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder, BlockValSet... blockValSets) {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, BlockValSet... blockValSets) {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  @Nonnull
  @Override
  public DistinctTable extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return false;
  }

  @Nonnull
  @Override
  public Comparable extractFinalResult(DistinctTable intermediateResult) {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  public void dumpTime() {
    System.out.println("Aggregation time: " + _aggregateWatch.elapsed(TimeUnit.MILLISECONDS));
  }
}
