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
package org.apache.pinot.segment.local.segment.index.json.shred;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.shred.JsonShredConstants;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.shred.JsonShredReader;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.shred.JsonShredWriter;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.shred.ShreddedJsonExtractor;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.JsonIndexConfig.TypedPathSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * F5 POC — proves end-to-end that a STRING-typed JSON shred carries the same values as the canonical
 * {@code jsonExtractScalar(col, '$.x', 'STRING')} would produce, and that the routing skeleton picks it up.
 *
 * <p>Intentionally tiny: 4 documents, one shredded path, parity check against a freshly-parsed Jackson tree
 * walk that mirrors what {@code JsonExtractScalarTransformFunction.transformToStringValuesSV} would emit.
 *
 * <p>Out of scope for this test (and documented as such in the design doc): predicate pushdown, range index,
 * mutable-segment shred maintenance, schema discovery, migration.
 */
public class JsonShredPocTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "JsonShredPocTest");
  private static final String COLUMN = "payload";

  @BeforeMethod
  public void setUp()
      throws Exception {
    if (INDEX_DIR.exists()) {
      FileUtils.deleteDirectory(INDEX_DIR);
    }
    FileUtils.forceMkdir(INDEX_DIR);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Test
  public void testTypedPathSpecConfigRoundTrip()
      throws Exception {
    JsonIndexConfig config = new JsonIndexConfig();
    List<TypedPathSpec> typed = new ArrayList<>();
    typed.add(new TypedPathSpec("$.name", FieldSpec.DataType.STRING));
    typed.add(new TypedPathSpec("$.region", FieldSpec.DataType.STRING));
    config.setTypedPaths(typed);

    String serialized = JsonUtils.objectToString(config);
    assertTrue(serialized.contains("\"typedPaths\""),
        "Serialized JsonIndexConfig must expose typedPaths: " + serialized);
    JsonIndexConfig roundTripped = JsonUtils.stringToObject(serialized, JsonIndexConfig.class);
    assertEquals(roundTripped.getTypedPaths(), typed);
    assertEquals(roundTripped, config);
  }

  @Test
  public void testStringShredRoundTripAndRouting()
      throws Exception {
    String[] records = new String[]{
        "{\"name\":\"adam\",\"region\":\"us\",\"age\":20}",
        "{\"name\":\"bob\",\"region\":\"ca\",\"age\":25}",
        "{\"region\":\"us\",\"age\":30}",                  // missing $.name
        "{\"name\":null,\"region\":\"us\",\"age\":40}"     // explicit null at $.name
    };

    TypedPathSpec nameSpec = new TypedPathSpec("$.name", FieldSpec.DataType.STRING);

    // 1. Write the shred.
    File shredFile;
    try (JsonShredWriter writer = new JsonShredWriter(INDEX_DIR, COLUMN, nameSpec)) {
      for (String record : records) {
        writer.add(record);
      }
      shredFile = writer.getOutputFile();
    }
    assertTrue(shredFile.exists(), "Shred file must be produced: " + shredFile);
    assertEquals(shredFile.getName(), JsonShredConstants.slugFor(COLUMN, "$.name", FieldSpec.DataType.STRING));

    // 2. Read it back directly.
    String[] expectedFromShred = {"adam", "bob", null, null};
    try (JsonShredReader reader = new JsonShredReader(shredFile)) {
      assertEquals(reader.getNumDocs(), records.length);
      assertEquals(reader.getDataType(), FieldSpec.DataType.STRING);
      for (int i = 0; i < records.length; i++) {
        assertEquals(reader.getString(i), expectedFromShred[i], "Mismatch at docId " + i);
      }
    }

    // 3. Parity vs. an independent JSON tree walk that mirrors what JsonExtractScalarTransformFunction would
    //    produce for $.name with STRING result type: missing-path → null; JSON-null literal → null; otherwise
    //    the value rendered via Jackson's asText(). Implemented inline so we don't pull jayway into the
    //    segment-local test classpath only for this test.
    try (JsonShredReader reader = new JsonShredReader(shredFile)) {
      for (int i = 0; i < records.length; i++) {
        String fromShred = reader.getString(i);
        String fromCanonical = walkAsStringOrNull(records[i], "name");
        assertEquals(fromShred, fromCanonical,
            "Shred and reference walk disagree at docId " + i + " for record " + records[i]);
      }
    }

    // 4. Routing skeleton: ShreddedJsonExtractor picks up the shred for $.name|STRING.
    JsonShredReader reader2 = new JsonShredReader(shredFile);
    Map<TypedPathSpec, JsonShredReader> readers = new HashMap<>();
    readers.put(nameSpec, reader2);
    try (ShreddedJsonExtractor router = new ShreddedJsonExtractor(Collections.singletonList(nameSpec), readers)) {
      ShreddedJsonExtractor.Routed routed = router.routeStringProjection("$.name");
      assertNotNull(routed, "Router must route $.name to the shred");
      for (int i = 0; i < records.length; i++) {
        assertEquals(routed.getString(i), expectedFromShred[i]);
      }
      // A path that wasn't shredded must NOT route.
      assertNull(router.routeStringProjection("$.region"),
          "Router must decline an unshredded path so the caller falls back to the canonical extractor");
    }
  }

  /// Mirrors the STRING / SV behavior of {@code JsonExtractScalarTransformFunction.transformToStringValuesSV}
  /// for a top-level object field {@code fieldName} (the only shape this POC test exercises): missing field → null,
  /// JSON-null literal → null, value node → {@code asText()}, container → {@code toString()}.
  private static String walkAsStringOrNull(String jsonString, String fieldName)
      throws Exception {
    JsonNode root = JsonUtils.stringToJsonNode(jsonString);
    if (root == null || root.isNull() || root.isMissingNode()) {
      return null;
    }
    JsonNode child = root.get(fieldName);
    if (child == null || child.isNull() || child.isMissingNode()) {
      return null;
    }
    return child.isValueNode() ? child.asText() : child.toString();
  }
}
