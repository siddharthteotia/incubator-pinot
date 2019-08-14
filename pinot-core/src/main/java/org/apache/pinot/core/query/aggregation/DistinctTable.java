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
package org.apache.pinot.core.query.aggregation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.core.data.table.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This serves the following purposes:
 *
 * (1) Intermediate result object for Distinct aggregation function
 * (2) The same object is serialized by the server inside the data table
 * for sending the results to broker. Broker deserializes it.
 */
public class DistinctTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistinctTable.class);
  private final static int INITIAL_CAPACITY = 64000;
  private FieldSpec.DataType[] _projectedColumnTypes;
  private String[] _projectedColumnNames;
  private int _recordLimit;
  private static Object DUMMY = new Object();
  private Set<Key> _table;

  /**
   * Add a row to hash table
   * @param key multi-column key to add
   */
  public void addKey(final Key key) {
    if (_table.size() >= _recordLimit) {
      //System.out.println("dropped");
      //LOGGER.info("Distinct table Reached allowed max cardinality of {}", _recordLimit);
      return;
    }

    _table.add(key);
  }

  public DistinctTable(int recordLimit) {
    //_recordLimit = recordLimit;
    _recordLimit = 10_000_000;
    _table = new HashSet<>(INITIAL_CAPACITY);
  }

  /**
   * DESERIALIZE: Broker side
   * @param byteBuffer data to deserialize
   * @throws IOException
   */
  public DistinctTable(final ByteBuffer byteBuffer) throws IOException {
    final DataTable dataTable = DataTableFactory.getDataTable(byteBuffer);
    final DataSchema dataSchema = dataTable.getDataSchema();
    final int numRows = dataTable.getNumberOfRows();
    final int numColumns = dataSchema.size();

    _table = new HashSet<>();

    // extract rows from the datatable
    for (int rowIndex  = 0; rowIndex < numRows; rowIndex++) {
     Object[] columnValues = new Object[numColumns];
      for (int colIndex = 0; colIndex < numColumns; colIndex++) {
        DataSchema.ColumnDataType columnDataType = dataSchema.getColumnDataType(colIndex);
        switch (columnDataType) {
          case INT:
            columnValues[colIndex] = dataTable.getInt(rowIndex, colIndex);
            break;
          case LONG:
            columnValues[colIndex] = dataTable.getLong(rowIndex, colIndex);
            break;
          case STRING:
            columnValues[colIndex] = dataTable.getString(rowIndex, colIndex);
            break;
          case FLOAT:
            columnValues[colIndex] = dataTable.getFloat(rowIndex, colIndex);
            break;
          case DOUBLE:
            columnValues[colIndex] = dataTable.getDouble(rowIndex, colIndex);
            break;
          default:
            throw new UnsupportedOperationException("Unexpected column data type " + columnDataType + " in data table for DISTINCT query");
        }
      }

      _table.add(new Key(columnValues));
    }

    _projectedColumnNames = dataSchema.getColumnNames();
    _projectedColumnTypes = buildDataTypesFromColumnTypes(dataSchema);
  }

  private FieldSpec.DataType[] buildDataTypesFromColumnTypes(final DataSchema dataSchema) {
    final int numColumns = dataSchema.size();
    final FieldSpec.DataType[] columnTypes = new FieldSpec.DataType[numColumns];
    for (int colIndex = 0; colIndex < numColumns; colIndex++) {
      DataSchema.ColumnDataType columnDataType = dataSchema.getColumnDataType(colIndex);
      switch (columnDataType) {
        case INT:
          columnTypes[colIndex] = FieldSpec.DataType.INT;
          break;
        case LONG:
          columnTypes[colIndex] = FieldSpec.DataType.INT;
          break;
        case STRING:
          columnTypes[colIndex] = FieldSpec.DataType.INT;
          break;
        case FLOAT:
          columnTypes[colIndex] = FieldSpec.DataType.INT;
          break;
        case DOUBLE:
          columnTypes[colIndex] = FieldSpec.DataType.INT;
          break;
        default:
          throw new UnsupportedOperationException("Unexpected column data type " + columnDataType + " in data table for DISTINCT query");
      }
    }

    return columnTypes;
  }

  /**
   * SERIALIZE: Server side
   * @return serialized bytes
   * @throws IOException
   */
  public byte[] toBytes() throws IOException {
    final String[] columnNames = new String[_projectedColumnNames.length];
    final DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[_projectedColumnNames.length];

    // set actual column names and column data types in data schema
    for (int i = 0; i < _projectedColumnNames.length; i++) {
      columnNames[i] = _projectedColumnNames[i];
      switch (_projectedColumnTypes[i]) {
        case INT:
          columnDataTypes[i] = DataSchema.ColumnDataType.INT;
          break;
        case LONG:
          columnDataTypes[i] = DataSchema.ColumnDataType.LONG;
          break;
        case STRING:
          columnDataTypes[i] = DataSchema.ColumnDataType.STRING;
          break;
        case FLOAT:
          columnDataTypes[i] = DataSchema.ColumnDataType.FLOAT;
          break;
        case DOUBLE:
          columnDataTypes[i] = DataSchema.ColumnDataType.DOUBLE;
          break;
      }
    }

    // build rows for data table
    DataTableBuilder dataTableBuilder = new DataTableBuilder(new DataSchema(columnNames, columnDataTypes));

    final Iterator<Key> iterator = _table.iterator();
    while (iterator.hasNext()) {
      dataTableBuilder.startRow();
      final Key key = iterator.next();
      serializeColumns(key.getColumns(), columnDataTypes, dataTableBuilder);
      dataTableBuilder.finishRow();
    }

    final DataTable dataTable = dataTableBuilder.build();
    return dataTable.toBytes();
  }

  private void serializeColumns(final Object[] columns, final DataSchema.ColumnDataType[] columnDataTypes,
      final DataTableBuilder dataTableBuilder) {
    for (int colIndex = 0; colIndex < columns.length; colIndex++) {
      switch (columnDataTypes[colIndex]) {
        case INT:
          dataTableBuilder.setColumn(colIndex, ((Number)columns[colIndex]).intValue());
          break;
        case LONG:
          dataTableBuilder.setColumn(colIndex, ((Number)columns[colIndex]).longValue());
          break;
        case STRING:
          dataTableBuilder.setColumn(colIndex, ((String)columns[colIndex]));
          break;
        case FLOAT:
          dataTableBuilder.setColumn(colIndex, ((Number)columns[colIndex]).floatValue());
          break;
        case DOUBLE:
          dataTableBuilder.setColumn(colIndex, ((Number)columns[colIndex]).doubleValue());
          break;
      }
    }
  }

  public String[] getProjectedColumnNames() {
    return _projectedColumnNames;
  }

  public FieldSpec.DataType[] getProjectedColumnTypes() {
    return _projectedColumnTypes;
  }

  public int size() {
    return _table.size();
  }

  public Iterator<Key> getIterator() {
    return _table.iterator();
  }

  public void setProjectedColumnNames(String[] projectedColumnNames) {
    _projectedColumnNames = projectedColumnNames;
  }

  public void setProjectedColumnTypes(FieldSpec.DataType[] projectedColumnTypes) {
    _projectedColumnTypes = projectedColumnTypes;
  }
}
