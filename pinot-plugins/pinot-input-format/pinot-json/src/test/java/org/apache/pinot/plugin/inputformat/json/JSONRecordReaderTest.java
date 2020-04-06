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
package org.apache.pinot.plugin.inputformat.json;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.AbstractRecordReaderTest;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.JsonUtils;


public class JSONRecordReaderTest extends AbstractRecordReaderTest {
  private final File _dateFile = new File(_tempDir, "data.json");

  @Override
  protected RecordReader createRecordReader()
      throws Exception {
    JSONRecordReader recordReader = new JSONRecordReader();
    recordReader.init(_dateFile, getPinotSchema(), null);
    return recordReader;
  }

  @Override
  protected void writeRecordsToFile(List<Map<String, Object>> recordsToWrite)
      throws Exception {
    try (FileWriter fileWriter = new FileWriter(_dateFile)) {
      for (Map<String, Object> r : recordsToWrite) {
        ObjectNode jsonRecord = JsonUtils.newObjectNode();
        for (String key : r.keySet()) {
          jsonRecord.set(key, JsonUtils.objectToJsonNode(r.get(key)));
        }
        fileWriter.write(jsonRecord.toString());
      }
    }
  }

  public static void main (String[] args) throws Exception {
    JSONRecordReader jsonRecordReader = new JSONRecordReader();
    Schema schema = new Schema.SchemaBuilder().addNested("person", FieldSpec.DataType.STRUCT).build();
    File jsonFile = new File("/Users/steotia/Desktop/persons.json");
    jsonRecordReader.init(jsonFile, schema, null);
    GenericRow row = new GenericRow();
    while (jsonRecordReader.hasNext()) {
      row = jsonRecordReader.next(row);
      System.out.println(row);
    }
  }
}
