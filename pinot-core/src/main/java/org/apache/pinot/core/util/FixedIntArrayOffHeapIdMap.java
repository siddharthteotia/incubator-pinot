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
package org.apache.pinot.core.util;

import java.io.IOException;
import java.util.Arrays;
import javax.annotation.Nonnull;
import org.apache.pinot.common.Utils;
import org.apache.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.core.io.readerwriter.impl.FixedByteSingleValueMultiColumnReaderWriter;
import org.apache.pinot.core.realtime.impl.dictionary.BaseOffHeapMutableDictionary;


/**
 * Implementation of {@link IdMap} with {@link FixedIntArray} as key.
 *
 * This implementation also extends the {@link BaseOffHeapMutableDictionary} for code-reuse of off-heap functionality.
 * However, it is not a full dictionary implementation (for example, does not implement getMin/Max etc).
 *
 */
public class FixedIntArrayOffHeapIdMap extends BaseOffHeapMutableDictionary implements IdMap<FixedIntArray> {
  private final FixedByteSingleValueMultiColumnReaderWriter _dictIdToValue;
  private final int _numColumns;

  public FixedIntArrayOffHeapIdMap(int estimatedCardinality, int maxOverflowHashSize, int numColumns,
      PinotDataBufferMemoryManager memoryManager, String allocationContext) {
    super(estimatedCardinality, maxOverflowHashSize, memoryManager, allocationContext);

    int initialSize = nearestPowerOf2(estimatedCardinality);
    int[] columnSizesInBytes = new int[numColumns];
    Arrays.fill(columnSizesInBytes, Integer.BYTES);

    _dictIdToValue = new FixedByteSingleValueMultiColumnReaderWriter(initialSize, columnSizesInBytes, memoryManager,
        allocationContext);
    _numColumns = numColumns;
  }

  @Override
  public int put(FixedIntArray fixedIntArray) {
    index(fixedIntArray);
    return indexOf(fixedIntArray);
  }

  @Override
  public int getId(FixedIntArray fixedIntArray) {
    int id = indexOf(fixedIntArray);
    return (id != NULL_VALUE_INDEX) ? id : INVALID_ID;
  }

  @Override
  public FixedIntArray getKey(int id) {
    return (FixedIntArray) get(id);
  }

  @Override
  public int size() {
    return length();
  }

  @Override
  public void clear() {
    try {
      close();
      init();
    } catch (IOException e) {
      Utils.rethrowException(e);
    }
  }

  @Override
  public FixedIntArray get(int dictId) {
    int[] value = new int[_numColumns];
    for (int col = 0; col < _numColumns; col++) {
      value[col] = _dictIdToValue.getInt(dictId, col);
    }
    return new FixedIntArray(value);
  }

  @Override
  public int indexOf(Object rawValue) {
    return getDictId(rawValue, null);
  }

  @Override
  public void doClose()
      throws IOException {
    _dictIdToValue.close();
  }

  @Override
  protected void setRawValueAt(int dictId, Object value, byte[] serializedValue) {
    FixedIntArray intArray = (FixedIntArray) value; // Avoiding type check for efficiency.
    int[] values = intArray.elements();

    for (int col = 0; col < values.length; col++) {
      _dictIdToValue.setInt(dictId, col, values[col]);
    }
  }

  @Override
  public void index(@Nonnull Object value) {
    indexValue(value, null);
  }

  @Override
  public boolean inRange(@Nonnull String lower, @Nonnull String upper, int dictIdToCompare, boolean includeLower,
      boolean includeUpper) {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  public Object getMinVal() {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  public Object getMaxVal() {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  public Object getSortedValues() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getAvgValueSize() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int compare(int dictId1, int dictId2) {
    throw new UnsupportedOperationException();
  }
}
