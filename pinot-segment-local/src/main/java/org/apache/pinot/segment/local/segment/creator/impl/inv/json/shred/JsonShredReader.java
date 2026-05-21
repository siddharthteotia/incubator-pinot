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
package org.apache.pinot.segment.local.segment.creator.impl.inv.json.shred;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * F5 POC — reads a STRING-typed JSON shred file produced by {@link JsonShredWriter}.
 *
 * <p>The reader builds a per-doc offset index on construction (O(numDocs) walk) so {@link #getString(int)}
 * is O(1). For the POC this is fine; production should switch to a fixed-width or block-compressed format
 * that supports random access without a side index — see design doc § 2.4.
 *
 * <p>Not thread-safe at construction; the per-call {@link #getString(int)} is read-only and may be invoked
 * concurrently after the constructor returns since it only reads immutable state and a positional
 * {@link ByteBuffer} slice. Callers that share a reader across threads should still synchronize externally
 * if they intend to {@link #close()} concurrently with reads.
 */
public class JsonShredReader implements Closeable {
  private final RandomAccessFile _raf;
  private final FileChannel _channel;
  private final ByteBuffer _buffer;
  private final FieldSpec.DataType _dataType;
  private final int _numDocs;
  /** Byte offset (in the file) of each per-doc length prefix. */
  private final int[] _offsets;

  public JsonShredReader(File shredFile)
      throws IOException {
    _raf = new RandomAccessFile(shredFile, "r");
    _channel = _raf.getChannel();
    _buffer = _channel.map(FileChannel.MapMode.READ_ONLY, 0, _channel.size()).order(ByteOrder.BIG_ENDIAN);
    int magic = _buffer.getInt(0);
    Preconditions.checkState(magic == JsonShredConstants.MAGIC,
        "Bad shred file magic: 0x%s (expected 0x%s)", Integer.toHexString(magic),
        Integer.toHexString(JsonShredConstants.MAGIC));
    int version = _buffer.getInt(4);
    Preconditions.checkState(version == JsonShredConstants.FORMAT_VERSION,
        "Unsupported shred file version: %s", version);
    int dataTypeOrdinal = _buffer.getInt(8);
    _dataType = FieldSpec.DataType.values()[dataTypeOrdinal];
    Preconditions.checkState(_dataType == FieldSpec.DataType.STRING,
        "F5 POC only supports STRING shreds; got %s", _dataType);
    _numDocs = _buffer.getInt(12);
    _offsets = new int[_numDocs];
    int cursor = 16;
    for (int doc = 0; doc < _numDocs; doc++) {
      _offsets[doc] = cursor;
      int len = _buffer.getInt(cursor);
      cursor += Integer.BYTES;
      if (len != JsonShredConstants.NULL_LENGTH) {
        cursor += len;
      }
    }
  }

  /// Returns the shredded STRING value for {@code docId}, or {@code null} for a null entry.
  @Nullable
  public String getString(int docId) {
    Preconditions.checkPositionIndex(docId, _numDocs);
    int offset = _offsets[docId];
    int len = _buffer.getInt(offset);
    if (len == JsonShredConstants.NULL_LENGTH) {
      return null;
    }
    byte[] bytes = new byte[len];
    // ByteBuffer.get(int, byte[], int, int) is the absolute-positional read; safe under concurrent use.
    ByteBuffer slice = _buffer.duplicate().order(ByteOrder.BIG_ENDIAN);
    slice.position(offset + Integer.BYTES);
    slice.get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public int getNumDocs() {
    return _numDocs;
  }

  public FieldSpec.DataType getDataType() {
    return _dataType;
  }

  @Override
  public void close()
      throws IOException {
    _channel.close();
    _raf.close();
  }
}
