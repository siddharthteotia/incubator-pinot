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
package org.apache.pinot.core.query.reduce;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.HavingFilterQuery;
import org.apache.pinot.common.request.HavingFilterQueryMap;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.GroupByResult;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.response.broker.SelectionResults;
import org.apache.pinot.common.utils.BytesUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.aggregation.DistinctTable;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByTrimmingService;
import org.apache.pinot.core.query.selection.SelectionOperatorService;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.core.util.QueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>BrokerReduceService</code> class provides service to reduce data tables gathered from multiple servers
 * to {@link BrokerResponseNative}.
 */
@ThreadSafe
public class BrokerReduceService {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerReduceService.class);

  public BrokerResponseNative reduceOnDataTable(BrokerRequest brokerRequest,
      Map<ServerRoutingInstance, DataTable> dataTableMap, @Nullable BrokerMetrics brokerMetrics) {
    if (dataTableMap.size() == 0) {
      // Empty response.
      return BrokerResponseNative.empty();
    }

    BrokerResponseNative brokerResponseNative = new BrokerResponseNative();
    List<QueryProcessingException> processingExceptions = brokerResponseNative.getProcessingExceptions();
    long numDocsScanned = 0L;
    long numEntriesScannedInFilter = 0L;
    long numEntriesScannedPostFilter = 0L;
    long numSegmentsQueried = 0L;
    long numSegmentsProcessed = 0L;
    long numSegmentsMatched = 0L;
    long numConsumingSegmentsProcessed = 0L;
    long minConsumingFreshnessTimeMs = Long.MAX_VALUE;
    long numTotalRawDocs = 0L;
    boolean numGroupsLimitReached = false;

    // Cache a data schema from data tables (try to cache one with data rows associated with it).
    DataSchema cachedDataSchema = null;

    // Process server response metadata.
    Iterator<Map.Entry<ServerRoutingInstance, DataTable>> iterator = dataTableMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<ServerRoutingInstance, DataTable> entry = iterator.next();
      DataTable dataTable = entry.getValue();
      Map<String, String> metadata = dataTable.getMetadata();

      // Reduce on trace info.
      if (brokerRequest.isEnableTrace()) {
        brokerResponseNative.getTraceInfo()
            .put(entry.getKey().getHostname(), metadata.get(DataTable.TRACE_INFO_METADATA_KEY));
      }

      // Reduce on exceptions.
      for (String key : metadata.keySet()) {
        if (key.startsWith(DataTable.EXCEPTION_METADATA_KEY)) {
          processingExceptions.add(new QueryProcessingException(Integer.parseInt(key.substring(9)), metadata.get(key)));
        }
      }

      // Reduce on execution statistics.
      String numDocsScannedString = metadata.get(DataTable.NUM_DOCS_SCANNED_METADATA_KEY);
      if (numDocsScannedString != null) {
        numDocsScanned += Long.parseLong(numDocsScannedString);
      }
      String numEntriesScannedInFilterString = metadata.get(DataTable.NUM_ENTRIES_SCANNED_IN_FILTER_METADATA_KEY);
      if (numEntriesScannedInFilterString != null) {
        numEntriesScannedInFilter += Long.parseLong(numEntriesScannedInFilterString);
      }
      String numEntriesScannedPostFilterString = metadata.get(DataTable.NUM_ENTRIES_SCANNED_POST_FILTER_METADATA_KEY);
      if (numEntriesScannedPostFilterString != null) {
        numEntriesScannedPostFilter += Long.parseLong(numEntriesScannedPostFilterString);
      }
      String numSegmentsQueriedString = metadata.get(DataTable.NUM_SEGMENTS_QUERIED);
      if (numSegmentsQueriedString != null) {
        numSegmentsQueried += Long.parseLong(numSegmentsQueriedString);
      }

      String numSegmentsProcessedString = metadata.get(DataTable.NUM_SEGMENTS_PROCESSED);
      if (numSegmentsProcessedString != null) {
        numSegmentsProcessed += Long.parseLong(numSegmentsProcessedString);
      }
      String numSegmentsMatchedString = metadata.get(DataTable.NUM_SEGMENTS_MATCHED);
      if (numSegmentsMatchedString != null) {
        numSegmentsMatched += Long.parseLong(numSegmentsMatchedString);
      }

      String numConsumingString = metadata.get(DataTable.NUM_CONSUMING_SEGMENTS_PROCESSED);
      if (numConsumingString != null) {
        numConsumingSegmentsProcessed += Long.parseLong(numConsumingString);
      }

      String minConsumingFreshnessTimeMsString = metadata.get(DataTable.MIN_CONSUMING_FRESHNESS_TIME_MS);
      if (minConsumingFreshnessTimeMsString != null) {
        minConsumingFreshnessTimeMs =
            Math.min(Long.parseLong(minConsumingFreshnessTimeMsString), minConsumingFreshnessTimeMs);
      }

      String numTotalRawDocsString = metadata.get(DataTable.TOTAL_DOCS_METADATA_KEY);
      if (numTotalRawDocsString != null) {
        numTotalRawDocs += Long.parseLong(numTotalRawDocsString);
      }
      numGroupsLimitReached |= Boolean.parseBoolean(metadata.get(DataTable.NUM_GROUPS_LIMIT_REACHED_KEY));

      // After processing the metadata, remove data tables without data rows inside.
      DataSchema dataSchema = dataTable.getDataSchema();
      if (dataSchema == null) {
        iterator.remove();
      } else {
        // Try to cache a data table with data rows inside, or cache one with data schema inside.
        if (dataTable.getNumberOfRows() == 0) {
          if (cachedDataSchema == null) {
            cachedDataSchema = dataSchema;
          }
          iterator.remove();
        } else {
          cachedDataSchema = dataSchema;
        }
      }
    }

    // Set execution statistics.
    brokerResponseNative.setNumDocsScanned(numDocsScanned);
    brokerResponseNative.setNumEntriesScannedInFilter(numEntriesScannedInFilter);
    brokerResponseNative.setNumEntriesScannedPostFilter(numEntriesScannedPostFilter);
    brokerResponseNative.setNumSegmentsQueried(numSegmentsQueried);
    brokerResponseNative.setNumSegmentsProcessed(numSegmentsProcessed);
    brokerResponseNative.setNumSegmentsMatched(numSegmentsMatched);
    brokerResponseNative.setTotalDocs(numTotalRawDocs);
    brokerResponseNative.setNumGroupsLimitReached(numGroupsLimitReached);
    if (numConsumingSegmentsProcessed > 0) {
      brokerResponseNative.setNumConsumingSegmentsQueried(numConsumingSegmentsProcessed);
      brokerResponseNative.setMinConsumingFreshnessTimeMs(minConsumingFreshnessTimeMs);
    }

    // Update broker metrics.
    String tableName = brokerRequest.getQuerySource().getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    if (brokerMetrics != null) {
      brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.DOCUMENTS_SCANNED, numDocsScanned);
      brokerMetrics
          .addMeteredTableValue(rawTableName, BrokerMeter.ENTRIES_SCANNED_IN_FILTER, numEntriesScannedInFilter);
      brokerMetrics
          .addMeteredTableValue(rawTableName, BrokerMeter.ENTRIES_SCANNED_POST_FILTER, numEntriesScannedPostFilter);

      if (numConsumingSegmentsProcessed > 0 && minConsumingFreshnessTimeMs > 0) {
        brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.FRESHNESS_LAG_MS,
            System.currentTimeMillis() - minConsumingFreshnessTimeMs, TimeUnit.MILLISECONDS);
      }
    }

    // Parse the option from request whether to preserve the type
    QueryOptions queryOptions = new QueryOptions(brokerRequest.getQueryOptions());
    Selection selection = brokerRequest.getSelections();
    if (dataTableMap.isEmpty()) {
      // For empty data table map, construct empty result using the cached data schema for selection query if exists
      if (cachedDataSchema != null) {
        if (brokerRequest.isSetSelections()) {
          List<String> selectionColumns = SelectionOperatorUtils
              .getSelectionColumns(brokerRequest.getSelections().getSelectionColumns(), cachedDataSchema);
          brokerResponseNative.setSelectionResults(new SelectionResults(selectionColumns, new ArrayList<>(0)));
        } else if (brokerRequest.isSetGroupBy() && queryOptions.isGroupByModeSQL() && queryOptions
            .isResponseFormatSQL()) {
          setSQLGroupByOrderByResults(brokerResponseNative, cachedDataSchema, brokerRequest.getAggregationsInfo(),
              brokerRequest.getGroupBy(), brokerRequest.getOrderBy(), Collections.emptyList());
        }
      }
    } else {
      // Reduce server responses data and set query results into the broker response
      assert cachedDataSchema != null;

      if (selection != null) {
        // Selection query

        // For data table map with more than one data tables, remove conflicting data tables
        if (dataTableMap.size() > 1) {
          List<ServerRoutingInstance> droppedServers = removeConflictingResponses(cachedDataSchema, dataTableMap);
          if (!droppedServers.isEmpty()) {
            String errorMessage =
                QueryException.MERGE_RESPONSE_ERROR.getMessage() + ": responses for table: " + tableName
                    + " from servers: " + droppedServers + " got dropped due to data schema inconsistency.";
            LOGGER.warn(errorMessage);
            if (brokerMetrics != null) {
              brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.RESPONSE_MERGE_EXCEPTIONS, 1L);
            }
            brokerResponseNative
                .addToExceptions(new QueryProcessingException(QueryException.MERGE_RESPONSE_ERROR_CODE, errorMessage));
          }
        }

        setSelectionResults(brokerResponseNative, selection, dataTableMap.values(), cachedDataSchema,
            queryOptions.isPreserveType());
      } else {
        // Aggregation query

        AggregationFunction[] aggregationFunctions = AggregationFunctionUtils.getAggregationFunctions(brokerRequest);
        if (!brokerRequest.isSetGroupBy()) {
          // Aggregation only query.
          setAggregationResults(brokerRequest, brokerResponseNative, aggregationFunctions, dataTableMap.values(), cachedDataSchema,
              queryOptions.isPreserveType());
        } else {
          // Aggregation group-by query.
          // read results as records if  GROUP_BY_MODE is explicitly set to SQL

          if (queryOptions.isGroupByModeSQL()) {
            // sql + order by

            int resultSize = 0;

            // if RESPONSE_FORMAT is SQL, return results in {@link ResultTable}
            if (queryOptions.isResponseFormatSQL()) {
              setSQLGroupByOrderByResults(brokerResponseNative, cachedDataSchema, brokerRequest.getAggregationsInfo(),
                  brokerRequest.getGroupBy(), brokerRequest.getOrderBy(), dataTableMap.values());
              resultSize = brokerResponseNative.getResultTable().getRows().size();
            } else {
              setPQLGroupByOrderByResults(brokerResponseNative, cachedDataSchema, brokerRequest.getAggregationsInfo(),
                  brokerRequest.getGroupBy(), brokerRequest.getOrderBy(), dataTableMap.values(),
                  queryOptions.isPreserveType());
              if (!brokerResponseNative.getAggregationResults().isEmpty()) {
                resultSize = brokerResponseNative.getAggregationResults().get(0).getGroupByResult().size();
              }
            }
            if (brokerMetrics != null && resultSize > 0) {
              brokerMetrics.addMeteredQueryValue(brokerRequest, BrokerMeter.GROUP_BY_SIZE, resultSize);
            }
          } else {

            boolean[] aggregationFunctionSelectStatus =
                AggregationFunctionUtils.getAggregationFunctionsSelectStatus(brokerRequest.getAggregationsInfo());
            setGroupByHavingResults(brokerResponseNative, aggregationFunctions, aggregationFunctionSelectStatus,
                brokerRequest.getGroupBy(), dataTableMap.values(), brokerRequest.getHavingFilterQuery(),
                brokerRequest.getHavingFilterSubQueryMap(), queryOptions.isPreserveType());
            if (brokerMetrics != null && (!brokerResponseNative.getAggregationResults().isEmpty())) {
              // We emit the group by size when the result isn't empty. All the sizes among group-by results should be the same.
              // Thus, we can just emit the one from the 1st result.
              brokerMetrics.addMeteredQueryValue(brokerRequest, BrokerMeter.GROUP_BY_SIZE,
                  brokerResponseNative.getAggregationResults().get(0).getGroupByResult().size());
            }
          }
        }
      }
    }
    return brokerResponseNative;
  }

  /**
   * Given a data schema, remove data tables that are not compatible with this data schema.
   * <p>Upgrade the data schema passed in to cover all remaining data schemas.
   *
   * @param dataSchema data schema.
   * @param dataTableMap map from server to data table.
   * @return list of server names where the data table got removed.
   */
  private List<ServerRoutingInstance> removeConflictingResponses(DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap) {
    List<ServerRoutingInstance> droppedServers = new ArrayList<>();
    Iterator<Map.Entry<ServerRoutingInstance, DataTable>> iterator = dataTableMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<ServerRoutingInstance, DataTable> entry = iterator.next();
      DataSchema dataSchemaToCompare = entry.getValue().getDataSchema();
      assert dataSchemaToCompare != null;
      if (!dataSchema.isTypeCompatibleWith(dataSchemaToCompare)) {
        droppedServers.add(entry.getKey());
        iterator.remove();
      } else {
        dataSchema.upgradeToCover(dataSchemaToCompare);
      }
    }
    return droppedServers;
  }

  /**
   * Reduce selection results from multiple servers and set them into BrokerResponseNative passed in.
   *
   * @param brokerResponseNative broker response.
   * @param selection selection information.
   * @param dataTables Collection of data tables
   * @param dataSchema data schema.
   */
  private void setSelectionResults(BrokerResponseNative brokerResponseNative, Selection selection,
      Collection<DataTable> dataTables, DataSchema dataSchema, boolean preserveType) {
    int selectionSize = selection.getSize();
    if (selectionSize > 0 && selection.isSetSelectionSortSequence()) {
      // Selection order-by
      SelectionOperatorService selectionService = new SelectionOperatorService(selection, dataSchema);
      selectionService.reduceWithOrdering(dataTables);
      brokerResponseNative.setSelectionResults(selectionService.renderSelectionResultsWithOrdering(preserveType));
    } else {
      // Selection only
      List<String> selectionColumns =
          SelectionOperatorUtils.getSelectionColumns(selection.getSelectionColumns(), dataSchema);
      brokerResponseNative.setSelectionResults(SelectionOperatorUtils.renderSelectionResultsWithoutOrdering(
          SelectionOperatorUtils.reduceWithoutOrdering(dataTables, selectionSize), dataSchema, selectionColumns,
          preserveType));
    }
  }

  private boolean isDistinct(final AggregationFunction[] aggregationFunctions) {
    return aggregationFunctions.length == 1 && aggregationFunctions[0].getType() == AggregationFunctionType.DISTINCT;
  }

  /**
   * Reduce aggregation results from multiple servers and set them into BrokerResponseNative passed in.
   *
   * @param brokerResponseNative broker response.
   * @param aggregationFunctions array of aggregation functions.
   * @param dataTables Collection of data tables
   * @param dataSchema data schema.
   */
  @SuppressWarnings("unchecked")
  private void setAggregationResults(BrokerRequest brokerRequest, BrokerResponseNative brokerResponseNative,
      AggregationFunction[] aggregationFunctions, Collection<DataTable> dataTables, DataSchema dataSchema,
      boolean preserveType) {
    if (isDistinct(aggregationFunctions)) {
      // Special handling for DISTINCT aggregation function
      setDistinctQueryResults(brokerRequest, brokerResponseNative, dataTables, dataSchema, aggregationFunctions[0]);
    } else {
      // handle all other aggregation functions
      int numAggregationFunctions = aggregationFunctions.length;
      // Merge results from all data tables.
      Object[] intermediateResults = new Object[numAggregationFunctions];
      for (DataTable dataTable : dataTables) {
        for (int i = 0; i < numAggregationFunctions; i++) {
          Object intermediateResultToMerge;
          ColumnDataType columnDataType = dataSchema.getColumnDataType(i);
          switch (columnDataType) {
            case LONG:
              intermediateResultToMerge = dataTable.getLong(0, i);
              break;
            case DOUBLE:
              intermediateResultToMerge = dataTable.getDouble(0, i);
              break;
            case OBJECT:
              intermediateResultToMerge = dataTable.getObject(0, i);
              break;
            default:
              throw new IllegalStateException("Illegal column data type in aggregation results: " + columnDataType);
          }
          Object mergedIntermediateResult = intermediateResults[i];
          if (mergedIntermediateResult == null) {
            intermediateResults[i] = intermediateResultToMerge;
          } else {
            intermediateResults[i] = aggregationFunctions[i].merge(mergedIntermediateResult, intermediateResultToMerge);
          }
        }
      }

      // Extract final results and set them into the broker response.
      List<AggregationResult> reducedAggregationResults = new ArrayList<>(numAggregationFunctions);
      for (int i = 0; i < numAggregationFunctions; i++) {
        Serializable resultValue = AggregationFunctionUtils
            .getSerializableValue(aggregationFunctions[i].extractFinalResult(intermediateResults[i]));

        // Format the value into string if required
        if (!preserveType) {
          resultValue = AggregationFunctionUtils.formatValue(resultValue);
        }
        reducedAggregationResults.add(new AggregationResult(dataSchema.getColumnName(i), resultValue));
      }
      brokerResponseNative.setAggregationResults(reducedAggregationResults);
    }
  }

  private void setDistinctQueryResults(BrokerRequest brokerRequest, BrokerResponseNative brokerResponseNative,
      Collection<DataTable> dataTables, DataSchema dataSchema, AggregationFunction aggregationFunction) {
    // DISTINCT is implemented as an aggregation function in the execution engine. Just like
    // other aggregation functions, DISTINCT returns its result as a single object
    // (of type DistinctTable) serialized by the server into the DataTable and deserialized
    // by the broker from the DataTable. So there should be exactly 1 row and 1 column and that
    // column value should be the serialized DistinctTable -- so essentially it is a DataTable
    // inside a DataTable
    Preconditions.checkState(dataSchema.size() == 1,
        "DataTable from server for DISTINCT should have exactly one row");
    Preconditions.checkState(dataSchema.getColumnDataType(0) == ColumnDataType.OBJECT,
        "DistinctAggregationFunction should return result of type OBJECT");
    Object mergedIntermediateResult = null;
    // go over all the data tables from servers
    for (DataTable dataTable : dataTables) {
      Preconditions.checkState(dataTable.getNumberOfRows() == 1);
      // deserialize the DistinctTable
      Object intermediateResultToMerge = dataTable.getObject(0, 0);
      Preconditions.checkState(intermediateResultToMerge instanceof DistinctTable);
      DistinctTable distinctTable = (DistinctTable)intermediateResultToMerge;
      // since DistinctTable uses the Table interface and during deserialization, we didn't
      // have all the necessary information w.r.t ORDER BY, limit etc, we set it now
      // before merging so that resize/trimming/sorting happens correctly
      distinctTable.addLimitAndOrderByInfo(brokerRequest);
      if (mergedIntermediateResult == null) {
        mergedIntermediateResult = intermediateResultToMerge;
      } else {
        aggregationFunction.merge(mergedIntermediateResult, intermediateResultToMerge);
      }
    }

    DistinctTable distinctTable =  (DistinctTable)mergedIntermediateResult;
    // finish the merging, sort (if ORDER BY), get iterator
    distinctTable.finish(true);

    List<Serializable[]> resultSet = new ArrayList<>(distinctTable.size());
    String[] columnNames = distinctTable.getDataSchema().getColumnNames();
    Iterator<Record> iterator = distinctTable.iterator();
    while (iterator.hasNext()) {
      Record record = iterator.next();
      Object[] columns = record.getValues();
      Serializable[] distinctRow = new Serializable[columns.length];
      for (int col = 0; col < columns.length; col++) {
        final Serializable columnValue = AggregationFunctionUtils.getSerializableValue(columns[col]);
        distinctRow[col] = columnValue;
      }
      resultSet.add(distinctRow);
    }

    // Up until now, we have treated DISTINCT similar to another aggregation function even in terms
    // of the result from function and merging results.
    // However, the DISTINCT query is just another SELECTION style query from the user's point
    // of view and will return one or records in the result table for the column(s) selected and so
    // for that reason, response from broker should be a selection query result.
    brokerResponseNative.setSelectionResults((new SelectionResults(Arrays.asList(columnNames), resultSet)));
  }

  /**
   * Extract group by order by results and set into {@link ResultTable}
   * @param brokerResponseNative broker response
   * @param dataSchema data schema
   * @param aggregationInfos aggregations info
   * @param groupBy group by info
   * @param orderBy order by info
   * @param dataTables Collection of data tables
   */
  private void setSQLGroupByOrderByResults(BrokerResponseNative brokerResponseNative, DataSchema dataSchema,
      List<AggregationInfo> aggregationInfos, GroupBy groupBy, List<SelectionSort> orderBy,
      Collection<DataTable> dataTables) {
    List<String> columnNames = new ArrayList<>(dataSchema.size());
    for (int i = 0; i < dataSchema.size(); i++) {
      columnNames.add(dataSchema.getColumnName(i));
    }

    int numGroupBy = groupBy.getExpressionsSize();
    int numAggregations = aggregationInfos.size();

    IndexedTable indexedTable = getIndexedTable(groupBy, aggregationInfos, orderBy, dataSchema, dataTables);

    AggregationFunction[] aggregationFunctions = new AggregationFunction[numAggregations];
    for (int i = 0; i < numAggregations; i++) {
      aggregationFunctions[i] =
          AggregationFunctionUtils.getAggregationFunctionContext(aggregationInfos.get(i)).getAggregationFunction();
    }

    List<Object[]> rows = new ArrayList<>();
    int numColumns = columnNames.size();
    Iterator<Record> sortedIterator = indexedTable.iterator();
    int numRows = 0;
    while (numRows < groupBy.getTopN() && sortedIterator.hasNext()) {

      Record nextRecord = sortedIterator.next();
      Object[] values = nextRecord.getValues();

      int index = numGroupBy;
      int aggNum = 0;
      while (index < numColumns) {
        values[index] = aggregationFunctions[aggNum++].extractFinalResult(values[index]);
        index++;
      }
      rows.add(values);
      numRows++;
    }

    brokerResponseNative.setResultTable(new ResultTable(dataSchema, rows));
  }

  private IndexedTable getIndexedTable(GroupBy groupBy, List<AggregationInfo> aggregationInfos,
      List<SelectionSort> orderBy, DataSchema dataSchema, Collection<DataTable> dataTables) {

    int numColumns = dataSchema.size();
    int indexedTableCapacity = GroupByUtils.getTableCapacity(groupBy, orderBy);
    IndexedTable indexedTable = new ConcurrentIndexedTable(dataSchema, aggregationInfos, orderBy, indexedTableCapacity);

    for (DataTable dataTable : dataTables) {
      BiFunction[] functions = new BiFunction[numColumns];
      for (int i = 0; i < numColumns; i++) {
        ColumnDataType columnDataType = dataSchema.getColumnDataType(i);
        BiFunction<Integer, Integer, Object> function;
        switch (columnDataType) {

          case INT:
            function = dataTable::getInt;
            break;
          case LONG:
            function = dataTable::getLong;
            break;
          case FLOAT:
            function = dataTable::getFloat;
            break;
          case DOUBLE:
            function = dataTable::getDouble;
            break;
          case STRING:
            function = dataTable::getString;
            break;
          case BYTES:
            // FIXME: support BYTES in DataTable instead of converting to string
            function = (row, col) -> BytesUtils.toByteArray(dataTable.getString(row, col));
            break;
          default:
            function = dataTable::getObject;
        }
        functions[i] = function;
      }

      for (int row = 0; row < dataTable.getNumberOfRows(); row++) {
        Object[] columns = new Object[numColumns];
        for (int col = 0; col < numColumns; col++) {
          columns[col] = functions[col].apply(row, col);
        }
        Record record = new Record(columns);
        indexedTable.upsert(record);
      }
    }
    indexedTable.finish(true);
    return indexedTable;
  }

  /**
   * Extract the results of group by order by into a List of {@link AggregationResult}
   * There will be 1 aggregation result per aggregation. The group by keys will be the same across all aggregations
   * @param brokerResponseNative broker response
   * @param dataSchema data schema
   * @param aggregationInfos aggregations info
   * @param groupBy group by info
   * @param orderBy order by info
   * @param dataTables Collection of data tables
   */
  private void setPQLGroupByOrderByResults(BrokerResponseNative brokerResponseNative, DataSchema dataSchema,
      List<AggregationInfo> aggregationInfos, GroupBy groupBy, List<SelectionSort> orderBy,
      Collection<DataTable> dataTables, boolean preserveType) {
    int numGroupBy = groupBy.getExpressionsSize();
    int numAggregations = aggregationInfos.size();
    int numColumns = numGroupBy + numAggregations;

    List<String> groupByColumns = new ArrayList<>(numGroupBy);
    int idx = 0;
    while (idx < numGroupBy) {
      groupByColumns.add(dataSchema.getColumnName(idx));
      idx++;
    }

    List<String> aggregationColumns = new ArrayList<>(numAggregations);
    AggregationFunction[] aggregationFunctions = new AggregationFunction[aggregationInfos.size()];
    List<List<GroupByResult>> groupByResults = new ArrayList<>(numAggregations);
    int aggIdx = 0;
    while (idx < numColumns) {
      aggregationColumns.add(dataSchema.getColumnName(idx));
      aggregationFunctions[aggIdx] =
          AggregationFunctionUtils.getAggregationFunctionContext(aggregationInfos.get(aggIdx)).getAggregationFunction();
      groupByResults.add(new ArrayList<>());
      idx++;
      aggIdx++;
    }

    if (!dataTables.isEmpty()) {
      IndexedTable indexedTable = getIndexedTable(groupBy, aggregationInfos, orderBy, dataSchema, dataTables);

      Iterator<Record> sortedIterator = indexedTable.iterator();
      int numRows = 0;
      while (numRows < groupBy.getTopN() && sortedIterator.hasNext()) {

        Record nextRecord = sortedIterator.next();
        Object[] values = nextRecord.getValues();

        int index = 0;
        List<String> group = new ArrayList<>(numGroupBy);
        while (index < numGroupBy) {
          group.add(values[index].toString());
          index++;
        }

        int aggNum = 0;
        while (index < numColumns) {
          Serializable serializableValue =
              getSerializableValue(aggregationFunctions[aggNum].extractFinalResult(values[index]));
          if (!preserveType) {
            serializableValue = AggregationFunctionUtils.formatValue(serializableValue);
          }
          GroupByResult groupByResult = new GroupByResult();
          groupByResult.setGroup(group);
          groupByResult.setValue(serializableValue);
          groupByResults.get(aggNum).add(groupByResult);
          index++;
          aggNum++;
        }
        numRows++;
      }
    }

    List<AggregationResult> aggregationResults = new ArrayList<>(numAggregations);
    for (int i = 0; i < numAggregations; i++) {
      AggregationResult aggregationResult =
          new AggregationResult(groupByResults.get(i), groupByColumns, aggregationColumns.get(i));
      aggregationResults.add(aggregationResult);
    }
    brokerResponseNative.setAggregationResults(aggregationResults);
  }

  private Serializable getSerializableValue(Object value) {
    if (value instanceof Number) {
      return (Number) value;
    } else {
      return value.toString();
    }
  }

  /**
   * Reduce group-by results from multiple servers and set them into BrokerResponseNative passed in.
   *
   * @param brokerResponseNative broker response.
   * @param aggregationFunctions array of aggregation functions.
   * @param groupBy group-by information.
   * @param dataTables Collection of data tables
   * @param havingFilterQuery having filter query
   * @param havingFilterQueryMap having filter query map
   */
  @SuppressWarnings("unchecked")
  private void setGroupByHavingResults(BrokerResponseNative brokerResponseNative,
      AggregationFunction[] aggregationFunctions, boolean[] aggregationFunctionsSelectStatus, GroupBy groupBy,
      Collection<DataTable> dataTables, HavingFilterQuery havingFilterQuery, HavingFilterQueryMap havingFilterQueryMap,
      boolean preserveType) {
    int numAggregationFunctions = aggregationFunctions.length;

    // Merge results from all data tables.
    String[] columnNames = new String[numAggregationFunctions];
    Map<String, Object>[] intermediateResultMaps = new Map[numAggregationFunctions];
    for (DataTable dataTable : dataTables) {
      for (int i = 0; i < numAggregationFunctions; i++) {
        if (columnNames[i] == null) {
          columnNames[i] = dataTable.getString(i, 0);
          intermediateResultMaps[i] = dataTable.getObject(i, 1);
        } else {
          Map<String, Object> mergedIntermediateResultMap = intermediateResultMaps[i];
          Map<String, Object> intermediateResultMapToMerge = dataTable.getObject(i, 1);
          for (Map.Entry<String, Object> entry : intermediateResultMapToMerge.entrySet()) {
            String groupKey = entry.getKey();
            Object intermediateResultToMerge = entry.getValue();
            if (mergedIntermediateResultMap.containsKey(groupKey)) {
              Object mergedIntermediateResult = mergedIntermediateResultMap.get(groupKey);
              mergedIntermediateResultMap
                  .put(groupKey, aggregationFunctions[i].merge(mergedIntermediateResult, intermediateResultToMerge));
            } else {
              mergedIntermediateResultMap.put(groupKey, intermediateResultToMerge);
            }
          }
        }
      }
    }

    // Extract final result maps from the merged intermediate result maps.
    Map<String, Comparable>[] finalResultMaps = new Map[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      Map<String, Object> intermediateResultMap = intermediateResultMaps[i];
      Map<String, Comparable> finalResultMap = new HashMap<>();
      for (String groupKey : intermediateResultMap.keySet()) {
        Object intermediateResult = intermediateResultMap.get(groupKey);
        finalResultMap.put(groupKey, aggregationFunctions[i].extractFinalResult(intermediateResult));
      }
      finalResultMaps[i] = finalResultMap;
    }
    //If HAVING clause is set, we further filter the group by results based on the HAVING predicate
    if (havingFilterQuery != null) {
      HavingClauseComparisonTree havingClauseComparisonTree =
          HavingClauseComparisonTree.buildHavingClauseComparisonTree(havingFilterQuery, havingFilterQueryMap);
      //Applying close policy
      //We just keep those groups (from different aggregation functions) that are exist in the result set of all aggregation functions.
      //In other words, we just keep intersection of groups of different aggregation functions.
      //Here we calculate the intersection of group key sets of different aggregation functions
      Set<String> intersectionOfKeySets = finalResultMaps[0].keySet();
      for (int i = 1; i < numAggregationFunctions; i++) {
        intersectionOfKeySets.retainAll(finalResultMaps[i].keySet());
      }

      //Now it is time to remove those groups that do not validate HAVING clause predicate
      //We use TreeMap which supports CASE_INSENSITIVE_ORDER
      Map<String, Comparable> singleGroupAggResults = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      Map<String, Comparable>[] finalFilteredResultMaps = new Map[numAggregationFunctions];
      for (int i = 0; i < numAggregationFunctions; i++) {
        finalFilteredResultMaps[i] = new HashMap<>();
      }

      for (String groupKey : intersectionOfKeySets) {
        for (int i = 0; i < numAggregationFunctions; i++) {
          singleGroupAggResults.put(columnNames[i], finalResultMaps[i].get(groupKey));
        }
        //if this group validate HAVING predicate keep it in the new map
        if (havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults)) {
          for (int i = 0; i < numAggregationFunctions; i++) {
            finalFilteredResultMaps[i].put(groupKey, singleGroupAggResults.get(columnNames[i]));
          }
        }
      }
      //update the final results
      finalResultMaps = finalFilteredResultMaps;
    }

    int aggregationNumsInFinalResult = 0;
    for (int i = 0; i < numAggregationFunctions; i++) {
      if (aggregationFunctionsSelectStatus[i]) {
        aggregationNumsInFinalResult++;
      }
    }

    if (aggregationNumsInFinalResult > 0) {
      String[] finalColumnNames = new String[aggregationNumsInFinalResult];
      Map<String, Comparable>[] finalOutResultMaps = new Map[aggregationNumsInFinalResult];
      AggregationFunction[] finalAggregationFunctions = new AggregationFunction[aggregationNumsInFinalResult];
      int count = 0;
      for (int i = 0; i < numAggregationFunctions; i++) {
        if (aggregationFunctionsSelectStatus[i]) {
          finalColumnNames[count] = columnNames[i];
          finalOutResultMaps[count] = finalResultMaps[i];
          finalAggregationFunctions[count] = aggregationFunctions[i];
          count++;
        }
      }
      // Trim the final result maps to topN and set them into the broker response.
      AggregationGroupByTrimmingService aggregationGroupByTrimmingService =
          new AggregationGroupByTrimmingService(finalAggregationFunctions, (int) groupBy.getTopN());
      List<GroupByResult>[] groupByResultLists = aggregationGroupByTrimmingService.trimFinalResults(finalOutResultMaps);

      // Format the value into string if required
      if (!preserveType) {
        for (List<GroupByResult> groupByResultList : groupByResultLists) {
          for (GroupByResult groupByResult : groupByResultList) {
            groupByResult.setValue(AggregationFunctionUtils.formatValue(groupByResult.getValue()));
          }
        }
      }

      List<AggregationResult> aggregationResults = new ArrayList<>(count);
      for (int i = 0; i < aggregationNumsInFinalResult; i++) {
        List<GroupByResult> groupByResultList = groupByResultLists[i];
        aggregationResults.add(new AggregationResult(groupByResultList, groupBy.getExpressions(), finalColumnNames[i]));
      }
      brokerResponseNative.setAggregationResults(aggregationResults);
    } else {
      throw new IllegalStateException(
          "There should be minimum one aggregation function in the select list of a Group by query");
    }
  }
}
