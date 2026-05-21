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

import org.apache.pinot.spi.data.FieldSpec;


/**
 * Constants for the F5 typed-JSON-shred file format. The POC implements only the STRING-shred slice; constants here
 * are namespaced for future numeric / binary shred extensions documented in
 * {@code design/F5-typed-json-shredding.md}.
 *
 * <p>File layout (current version):
 * <pre>
 *   header:
 *     int32 magic            = MAGIC
 *     int32 version          = FORMAT_VERSION
 *     int32 dataTypeOrdinal  = {@link FieldSpec.DataType#ordinal()}
 *     int32 numDocs
 *   body (numDocs entries):
 *     int32 length           — bytes that follow; {@code -1} == null
 *     bytes payload          — UTF-8 for STRING dataType
 * </pre>
 *
 * <p>The file extension is {@code .json.shred} and is layered alongside the existing {@code .json.idx} so that
 * an older binary that does not recognize the new extension simply ignores it.
 *
 * <p>Class is not thread-safe; it has no state.
 */
public final class JsonShredConstants {
  public static final String FILE_EXTENSION = ".json.shred";

  /** Arbitrary 4-byte signature: ASCII "JSHR". Lets readers fail fast on a stale or truncated file. */
  public static final int MAGIC = 0x4A534852;

  public static final int FORMAT_VERSION = 1;

  /** Sentinel length value indicating the per-doc payload is null. */
  public static final int NULL_LENGTH = -1;

  private JsonShredConstants() {
  }

  /**
   * Produces the on-disk filename for a shred of {@code column} on JSON {@code path} stored as {@code dataType}.
   * The slug is deterministic and reversible from the meta-file mapping (not stored on disk in the POC; the test
   * matches by the same construction).
   */
  public static String slugFor(String column, String path, FieldSpec.DataType dataType) {
    String pathSlug = path.replace('$', '_').replace('.', '_').replace('[', '_').replace(']', '_').replace('*', '_');
    return column + "." + pathSlug + "." + dataType.name() + FILE_EXTENSION;
  }
}
