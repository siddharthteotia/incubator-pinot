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
package org.apache.pinot.core.data.table;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.postaggregation.PostAggregationFunction;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.FunctionContext;
import org.apache.pinot.core.query.request.context.OrderByExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Helper class for trimming and sorting records in the IndexedTable, based on the order by information
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class OptimizedTableResizer {
  private final DataSchema _dataSchema;
  private final int _numGroupByExpressions;
  private final Map<ExpressionContext, Integer> _groupByExpressionIndexMap;
  private final AggregationFunction[] _aggregationFunctions;
  private final Map<FunctionContext, Integer> _aggregationFunctionIndexMap;
  private final int _numOrderByExpressions;
  private final OrderByValueExtractor[] _orderByValueExtractors;
  private final Comparator<Record> _recordComparator;

  public OptimizedTableResizer(DataSchema dataSchema, QueryContext queryContext) {
    _dataSchema = dataSchema;

    // NOTE: The data schema will always have group-by expressions in the front, followed by aggregation functions of
    //       the same order as in the query context. This is handled in AggregationGroupByOrderByOperator.

    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    assert groupByExpressions != null;
    _numGroupByExpressions = groupByExpressions.size();
    _groupByExpressionIndexMap = new HashMap<>();
    for (int i = 0; i < _numGroupByExpressions; i++) {
      _groupByExpressionIndexMap.put(groupByExpressions.get(i), i);
    }

    _aggregationFunctions = queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null;
    _aggregationFunctionIndexMap = queryContext.getAggregationFunctionIndexMap();
    assert _aggregationFunctionIndexMap != null;

    List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
    assert orderByExpressions != null;
    _numOrderByExpressions = orderByExpressions.size();
    _orderByValueExtractors = new OrderByValueExtractor[_numOrderByExpressions];
    boolean[] ordering = new boolean[_numOrderByExpressions];
    for (int i = 0; i < _numOrderByExpressions; i++) {
      OrderByExpressionContext orderByExpression = orderByExpressions.get(i);
      _orderByValueExtractors[i] = getOrderByValueExtractor(orderByExpression.getExpression());
      ordering[i] = orderByExpression.isAsc();
    }
    _recordComparator = new RecordComparator(_orderByValueExtractors, ordering);
  }

  private static class RecordComparator implements Comparator<Record> {
    private final OrderByValueExtractor[] _orderByValueExtractors;
    private final boolean[] _ordering;

    RecordComparator(OrderByValueExtractor[] valueExtractors, boolean[] ordering) {
      _orderByValueExtractors = valueExtractors;
      _ordering = ordering;
    }

    @Override
    public int compare (Record r1, Record r2) {
      int length = _orderByValueExtractors.length;
      for (int i = 0; i < length; i++) {
        Comparable v1 = _orderByValueExtractors[i].extract(r1);
        Comparable v2 = _orderByValueExtractors[i].extract(r2);
        int result = 0;
        // ordering (ASC or DESC) is on a per column basis
        if (_ordering[i]) {
          result = v1.compareTo(v2);
        } else {
          result = v2.compareTo(v1);
        }
        if (result != 0) {
          return result;
        }
      }
      return 0;
    }
  }

  /**
   * Helper method to construct a OrderByValueExtractor based on the given expression.
   */
  private OrderByValueExtractor getOrderByValueExtractor(ExpressionContext expression) {
    if (expression.getType() == ExpressionContext.Type.LITERAL) {
      return new LiteralExtractor(expression.getLiteral());
    }
    Integer groupByExpressionIndex = _groupByExpressionIndexMap.get(expression);
    if (groupByExpressionIndex != null) {
      // Group-by expression
      return new GroupByExpressionExtractor(groupByExpressionIndex);
    }
    FunctionContext function = expression.getFunction();
    Preconditions
        .checkState(function != null, "Failed to find ORDER-BY expression: %s in the GROUP-BY clause", expression);
    if (function.getType() == FunctionContext.Type.AGGREGATION) {
      // Aggregation function
      return new AggregationFunctionExtractor(_aggregationFunctionIndexMap.get(function));
    } else {
      // Post-aggregation function
      return new PostAggregationFunctionExtractor(function);
    }
  }

  /**
   * Trim recordsMap to trimToSize, based on order by information
   * Resize only if number of records is greater than trimToSize
   * The resizer smartly chooses to create PQ of records to evict or records to retain, based on the number of records and the number of records to evict
   */
  public void resizeRecordsMap(Map<Key, Record> recordsMap, int trimToSize) {
    int numRecordsToEvict = recordsMap.size() - trimToSize;
    if (numRecordsToEvict > 0) {
      if (numRecordsToEvict < trimToSize) {
        // num records to evict is smaller than num records to retain
        // make PQ of records to evict
        PriorityQueue<Record> priorityQueue = convertToRecordsPQ(recordsMap, numRecordsToEvict, _recordComparator);
        for (Record evictRecord : priorityQueue) {
          recordsMap.remove(evictRecord.getKey());
        }
      } else {
        // num records to retain is smaller than num records to evict
        // make PQ of records to retain
        Comparator<Record> comparator = _recordComparator.reversed();
        PriorityQueue<Record> priorityQueue = convertToRecordsPQ(recordsMap, trimToSize, comparator);
        ObjectOpenHashSet<Key> keysToRetain = new ObjectOpenHashSet<>(priorityQueue.size());
        for (Record retainRecord : priorityQueue) {
          keysToRetain.add(retainRecord.getKey());
        }
        recordsMap.keySet().retainAll(keysToRetain);
      }
    }
  }

  private PriorityQueue<Record> convertToRecordsPQ(Map<Key, Record> recordsMap, int size, Comparator recordComparator) {
    PriorityQueue<Record> priorityQueue = new PriorityQueue<>(size, recordComparator);
    for (Map.Entry<Key, Record> entry : recordsMap.entrySet()) {
      Record record = entry.getValue();
      if (priorityQueue.size() < size) {
        priorityQueue.offer(record);
      } else {
        Record peek = priorityQueue.peek();
        if (_recordComparator.compare(peek, record) < 0) {
          priorityQueue.poll();
          priorityQueue.offer(record);
        }
      }
    }
    return priorityQueue;
  }

  /**
   * Resizes the recordsMap and returns a sorted list of records.
   * This method is to be called from IndexedTable::finish, if both resize and sort is needed
   *
   * If numRecordsToEvict > numRecordsToRetain, resize with PQ of records to evict, and then sort
   * Else, resize with PQ of record to retain, then use the PQ to create sorted list
   */
  public List<Record> resizeAndSortRecordsMap(Map<Key, Record> recordsMap, int trimToSize) {
    int numRecords = recordsMap.size();
    if (numRecords == 0) {
      return Collections.emptyList();
    }
    int numRecordsToRetain = Math.min(numRecords, trimToSize);
    PriorityQueue<Record> priorityQueue = convertToRecordsPQ(recordsMap, numRecordsToRetain, _recordComparator);
    Record[] sortedArray = new Record[numRecordsToRetain];
    int count = 0;
    while (!priorityQueue.isEmpty()) {
      Record record = priorityQueue.poll();
      sortedArray[count++] = record;
    }
    return Arrays.asList(sortedArray);
  }

  /**
   * Extractor for the order-by value from a Record.
   */
  private interface OrderByValueExtractor {

    /**
     * Returns the ColumnDataType of the value extracted.
     */
    ColumnDataType getValueType();

    /**
     * Extracts the value from the given Record.
     */
    Comparable extract(Record record);
  }

  /**
   * Extractor for a literal.
   */
  private static class LiteralExtractor implements OrderByValueExtractor {
    final String _literal;

    LiteralExtractor(String literal) {
      _literal = literal;
    }

    @Override
    public ColumnDataType getValueType() {
      return ColumnDataType.STRING;
    }

    @Override
    public String extract(Record record) {
      return _literal;
    }
  }

  /**
   * Extractor for a group-by expression.
   */
  private class GroupByExpressionExtractor implements OrderByValueExtractor {
    final int _index;

    GroupByExpressionExtractor(int groupByExpressionIndex) {
      _index = groupByExpressionIndex;
    }

    @Override
    public ColumnDataType getValueType() {
      return _dataSchema.getColumnDataType(_index);
    }

    @Override
    public Comparable extract(Record record) {
      return (Comparable) record.getValues()[_index];
    }
  }

  /**
   * Extractor for an aggregation function.
   */
  private class AggregationFunctionExtractor implements OrderByValueExtractor {
    final int _index;
    final AggregationFunction _aggregationFunction;

    AggregationFunctionExtractor(int aggregationFunctionIndex) {
      _index = aggregationFunctionIndex + _numGroupByExpressions;
      _aggregationFunction = _aggregationFunctions[aggregationFunctionIndex];
    }

    @Override
    public ColumnDataType getValueType() {
      return _aggregationFunction.getFinalResultColumnType();
    }

    @Override
    public Comparable extract(Record record) {
      return _aggregationFunction.extractFinalResult(record.getValues()[_index]);
    }
  }

  /**
   * Extractor for a post-aggregation function.
   */
  private class PostAggregationFunctionExtractor implements OrderByValueExtractor {
    final Object[] _arguments;
    final OrderByValueExtractor[] _argumentExtractors;
    final PostAggregationFunction _postAggregationFunction;

    PostAggregationFunctionExtractor(FunctionContext function) {
      assert function.getType() == FunctionContext.Type.TRANSFORM;

      List<ExpressionContext> arguments = function.getArguments();
      int numArguments = arguments.size();
      _arguments = new Object[numArguments];
      _argumentExtractors = new OrderByValueExtractor[numArguments];
      ColumnDataType[] argumentTypes = new ColumnDataType[numArguments];
      for (int i = 0; i < numArguments; i++) {
        OrderByValueExtractor argumentExtractor = getOrderByValueExtractor(arguments.get(i));
        _argumentExtractors[i] = argumentExtractor;
        argumentTypes[i] = argumentExtractor.getValueType();
      }
      _postAggregationFunction = new PostAggregationFunction(function.getFunctionName(), argumentTypes);
    }

    @Override
    public ColumnDataType getValueType() {
      return _postAggregationFunction.getResultType();
    }

    @Override
    public Comparable extract(Record record) {
      int numArguments = _arguments.length;
      for (int i = 0; i < numArguments; i++) {
        _arguments[i] = _argumentExtractors[i].extract(record);
      }
      Object result = _postAggregationFunction.invoke(_arguments);
      if (_postAggregationFunction.getResultType() == ColumnDataType.BYTES) {
        return new ByteArray((byte[]) result);
      } else {
        return (Comparable) result;
      }
    }
  }
}
