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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.JsonIndexConfig.TypedPathSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * F5 POC — writes a single typed shred file for one {@link TypedPathSpec}, one entry per host-segment doc id.
 *
 * <p>Only the {@link FieldSpec.DataType#STRING} slice is implemented; numeric / binary slices are documented as
 * future work in {@code design/F5-typed-json-shredding.md} and intentionally throw on construction so we surface
 * a clear error rather than silently producing useless files. This is the projection-routing skeleton, not a
 * production-grade shred encoder.
 *
 * <p>Usage: construct, call {@link #add(String)} once per host doc, then {@link #close()}. The class is not
 * thread-safe and is intended to be invoked single-threaded from the segment-creation pipeline. The output file
 * is named via {@link JsonShredConstants#slugFor(String, String, FieldSpec.DataType)} and lives next to the host
 * column's {@code .json.idx}.
 */
public class JsonShredWriter implements Closeable {
  private final TypedPathSpec _spec;
  private final String[] _pathSegments;
  private final DataOutputStream _out;
  private final File _outputFile;
  private int _numDocsWritten;
  private boolean _headerWritten;
  private boolean _closed;

  public JsonShredWriter(File indexDir, String columnName, TypedPathSpec spec)
      throws IOException {
    Preconditions.checkArgument(spec.getDataType() == FieldSpec.DataType.STRING,
        "F5 POC supports only STRING typed shreds; got %s", spec.getDataType());
    _spec = spec;
    _pathSegments = parsePath(spec.getPath());
    _outputFile = new File(indexDir, JsonShredConstants.slugFor(columnName, spec.getPath(), spec.getDataType()));
    _out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_outputFile)));
  }

  /// Parses a simple {@code $.a.b.c} path into segments. POC only supports object navigation; array indexing and
  /// wildcards are deferred to v2 (see design doc § 7).
  private static String[] parsePath(String path) {
    Preconditions.checkArgument(path.startsWith("$"), "Path must start with '$', got: %s", path);
    String trimmed = path.length() == 1 ? "" : path.substring(1);
    if (trimmed.isEmpty()) {
      return new String[0];
    }
    Preconditions.checkArgument(trimmed.charAt(0) == '.', "Expected '.' after '$' in path: %s", path);
    String rest = trimmed.substring(1);
    String[] parts = rest.split("\\.");
    for (String part : parts) {
      Preconditions.checkArgument(!part.isEmpty(), "Empty path segment in: %s", path);
      Preconditions.checkArgument(!part.contains("[") && !part.contains("]") && !part.contains("*"),
          "F5 POC does not support array/wildcard segments; got: %s", path);
    }
    return parts;
  }

  /// Adds the typed value extracted from {@code jsonString} (or a null marker if the path is missing).
  public void add(@Nullable String jsonString)
      throws IOException {
    if (!_headerWritten) {
      // The header is written lazily so callers can construct a writer without committing to a doc count
      // until they actually start emitting. numDocs is finalized in close().
      writeHeaderPlaceholder();
      _headerWritten = true;
    }
    String extracted = extractStringOrNull(jsonString);
    if (extracted == null) {
      _out.writeInt(JsonShredConstants.NULL_LENGTH);
    } else {
      byte[] bytes = extracted.getBytes(StandardCharsets.UTF_8);
      _out.writeInt(bytes.length);
      _out.write(bytes);
    }
    _numDocsWritten++;
  }

  /// Extracts the path target as a String. Returns {@code null} if the path is missing, the document is null,
  /// the document is unparseable, or the target value itself is the JSON null literal.
  @Nullable
  private String extractStringOrNull(@Nullable String jsonString) {
    if (jsonString == null) {
      return null;
    }
    JsonNode node;
    try {
      node = JsonUtils.stringToJsonNode(jsonString);
    } catch (IOException e) {
      return null;
    }
    for (String segment : _pathSegments) {
      if (node == null || node.isNull() || node.isMissingNode()) {
        return null;
      }
      node = node.get(segment);
    }
    if (node == null || node.isNull() || node.isMissingNode()) {
      return null;
    }
    return node.isValueNode() ? node.asText() : node.toString();
  }

  private void writeHeaderPlaceholder()
      throws IOException {
    _out.writeInt(JsonShredConstants.MAGIC);
    _out.writeInt(JsonShredConstants.FORMAT_VERSION);
    _out.writeInt(_spec.getDataType().ordinal());
    // numDocs placeholder; rewritten in close() via RAF.
    _out.writeInt(0);
  }

  @Override
  public void close()
      throws IOException {
    if (_closed) {
      return;
    }
    _closed = true;
    if (!_headerWritten) {
      // Nothing was added; still emit a valid empty file so readers find something deterministic.
      writeHeaderPlaceholder();
    }
    _out.flush();
    _out.close();
    // Patch the numDocs field at byte offset 12 (3 ints * 4 bytes).
    try (RandomAccessFile raf = new RandomAccessFile(_outputFile, "rw")) {
      raf.seek(12);
      raf.writeInt(_numDocsWritten);
    }
  }

  /// Returns the on-disk file produced by this writer. Exposed for tests / handler wiring.
  public File getOutputFile() {
    return _outputFile;
  }
}
