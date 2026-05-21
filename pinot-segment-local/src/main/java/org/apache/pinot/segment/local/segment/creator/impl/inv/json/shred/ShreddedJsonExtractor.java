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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.JsonIndexConfig.TypedPathSpec;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * F5 POC — projection-side router that decides whether a {@code jsonExtractScalar(col, path, type)}-style call
 * can be satisfied directly from a {@link JsonShredReader}.
 *
 * <p>In a real integration this lives inside (or wraps) {@code JsonExtractScalarTransformFunction}'s init path,
 * and the per-row {@code transformTo*ValuesSV} methods read the shred via {@link JsonShredReader#getString(int)}.
 * Here we expose a tiny standalone API:
 *
 * <pre>
 *   ShreddedJsonExtractor.Routed routed = router.routeStringProjection("$.x");
 *   if (routed != null) {
 *     return routed.getString(docId);          // fast path: shred read
 *   }
 *   // ... fall back to the canonical JsonExtractScalar implementation.
 * </pre>
 *
 * <p>This class is intentionally narrow: it only proves that "we have the right shred for this projection"
 * is a one-key map lookup, and that the bytes flowing through the shred faithfully reproduce what the canonical
 * extractor would have parsed. Predicate routing, type coercion past STRING, and per-segment routing across mixed
 * old/new segments are deferred to v2 (see design doc § 5 and § 8).
 *
 * <p>Thread-safety: instances are immutable after construction; callers must externally synchronize
 * {@link #close()} with concurrent reads if they intend to release the underlying buffers.
 */
public class ShreddedJsonExtractor implements AutoCloseable {
  private final Map<String, JsonShredReader> _readersByPath;

  public ShreddedJsonExtractor(List<TypedPathSpec> specs, Map<TypedPathSpec, JsonShredReader> readers) {
    Map<String, JsonShredReader> indexed = new HashMap<>();
    for (TypedPathSpec spec : specs) {
      JsonShredReader reader = readers.get(spec);
      if (reader != null) {
        indexed.put(routingKey(spec.getPath(), spec.getDataType()), reader);
      }
    }
    _readersByPath = indexed;
  }

  /// Returns a routed accessor if {@code path} has a STRING-typed shred attached; otherwise null.
  /// The fall-back is the caller's responsibility — that's the POC's whole point.
  @Nullable
  public Routed routeStringProjection(String path) {
    JsonShredReader reader = _readersByPath.get(routingKey(path, FieldSpec.DataType.STRING));
    return reader == null ? null : new Routed(reader);
  }

  private static String routingKey(String path, FieldSpec.DataType dataType) {
    return path + "|" + dataType.name();
  }

  @Override
  public void close() {
    for (JsonShredReader reader : _readersByPath.values()) {
      try {
        reader.close();
      } catch (Exception ignored) {
        // Best-effort cleanup; POC scope.
      }
    }
  }

  /**
   * Handle returned by a successful route; reads the typed value directly from the shred.
   */
  public static class Routed {
    private final JsonShredReader _reader;

    Routed(JsonShredReader reader) {
      _reader = reader;
    }

    @Nullable
    public String getString(int docId) {
      return _reader.getString(docId);
    }
  }
}
