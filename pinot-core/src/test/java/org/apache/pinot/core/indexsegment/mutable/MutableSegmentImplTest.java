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
package org.apache.pinot.core.indexsegment.mutable;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.core.common.BlockMultiValIterator;
import org.apache.pinot.core.common.BlockSingleValIterator;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.common.DataSourceMetadata;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.io.reader.BaseSingleColumnSingleValueReader;
import org.apache.pinot.core.io.reader.DataFileReader;
import org.apache.pinot.core.io.reader.SingleColumnSingleValueReader;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.index.datasource.ListDataSource;
import org.apache.pinot.core.segment.index.datasource.MutableDataSource;
import org.apache.pinot.core.segment.index.datasource.StructDataSource;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadata;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.plugin.inputformat.json.JSONRecordReader;
import org.apache.pinot.segments.v1.creator.SegmentTestUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MutableSegmentImplTest {
  private static final String AVRO_FILE = "data/test_data-mv.avro";
  private static final String JSON_FILE = "data/person.json";
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "MutableSegmentImplTest");

  private Schema _schema;
  private MutableSegmentImpl _mutableSegmentImpl;
  private ImmutableSegment _immutableSegment;
  private long _lastIndexedTs;
  private long _lastIngestionTimeMs;
  private long _startTimeMs;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);

    URL resourceUrl = MutableSegmentImplTest.class.getClassLoader().getResource(AVRO_FILE);
    Assert.assertNotNull(resourceUrl);
    File avroFile = new File(resourceUrl.getFile());

    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(avroFile, TEMP_DIR, "testTable");
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();
    _immutableSegment = ImmutableSegmentLoader.load(new File(TEMP_DIR, driver.getSegmentName()), ReadMode.mmap);

    _schema = config.getSchema();
    VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSegmentSchema(_schema, "testSegment");
    _mutableSegmentImpl = MutableSegmentImplTestUtils
        .createMutableSegmentImpl(_schema, Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
            false);
    _lastIngestionTimeMs = System.currentTimeMillis();
    StreamMessageMetadata defaultMetadata = new StreamMessageMetadata(_lastIngestionTimeMs);
    _startTimeMs = System.currentTimeMillis();

    try (RecordReader recordReader = RecordReaderFactory.getRecordReader(FileFormat.AVRO, avroFile, _schema, null)) {
      GenericRow reuse = new GenericRow();
      while (recordReader.hasNext()) {
        _mutableSegmentImpl.index(recordReader.next(reuse), defaultMetadata);
        _lastIndexedTs = System.currentTimeMillis();
      }
    }
  }

  @Test
  public void testMetadata() {
    SegmentMetadata actualSegmentMetadata = _mutableSegmentImpl.getSegmentMetadata();
    SegmentMetadata expectedSegmentMetadata = _immutableSegment.getSegmentMetadata();
    Assert.assertEquals(actualSegmentMetadata.getTotalDocs(), expectedSegmentMetadata.getTotalDocs());

    // assert that the last indexed timestamp is close to what we expect
    long actualTs = _mutableSegmentImpl.getSegmentMetadata().getLastIndexedTimestamp();
    Assert.assertTrue(actualTs >= _startTimeMs);
    Assert.assertTrue(actualTs <= _lastIndexedTs);

    Assert.assertEquals(_mutableSegmentImpl.getSegmentMetadata().getLatestIngestionTimestamp(), _lastIngestionTimeMs);

    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      DataSourceMetadata actualDataSourceMetadata = _mutableSegmentImpl.getDataSource(column).getDataSourceMetadata();
      DataSourceMetadata expectedDataSourceMetadata = _immutableSegment.getDataSource(column).getDataSourceMetadata();
      Assert.assertEquals(actualDataSourceMetadata.getDataType(), expectedDataSourceMetadata.getDataType());
      Assert.assertEquals(actualDataSourceMetadata.isSingleValue(), expectedDataSourceMetadata.isSingleValue());
      Assert.assertEquals(actualDataSourceMetadata.getNumDocs(), expectedDataSourceMetadata.getNumDocs());
      if (!expectedDataSourceMetadata.isSingleValue()) {
        Assert.assertEquals(actualDataSourceMetadata.getMaxNumValuesPerMVEntry(),
            expectedDataSourceMetadata.getMaxNumValuesPerMVEntry());
      }
    }
  }

  @Test
  public void testDataSourceForSVColumns() {
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      if (fieldSpec.isSingleValueField()) {
        String column = fieldSpec.getName();
        DataSource actualDataSource = _mutableSegmentImpl.getDataSource(column);
        DataSource expectedDataSource = _immutableSegment.getDataSource(column);

        Dictionary actualDictionary = actualDataSource.getDictionary();
        Dictionary expectedDictionary = expectedDataSource.getDictionary();
        Assert.assertEquals(actualDictionary.length(), expectedDictionary.length());

        BlockSingleValIterator actualSVIterator =
            (BlockSingleValIterator) actualDataSource.nextBlock().getBlockValueSet().iterator();
        BlockSingleValIterator expectedSVIterator =
            (BlockSingleValIterator) expectedDataSource.nextBlock().getBlockValueSet().iterator();

        while (expectedSVIterator.hasNext()) {
          Assert.assertTrue(actualSVIterator.hasNext());

          int actualDictId = actualSVIterator.nextIntVal();
          int expectedDictId = expectedSVIterator.nextIntVal();
          // Only allow the default segment name to be different
          if (!column.equals(CommonConstants.Segment.BuiltInVirtualColumn.SEGMENTNAME)) {
            Assert.assertEquals(actualDictionary.get(actualDictId), expectedDictionary.get(expectedDictId));
          }
        }
        Assert.assertFalse(actualSVIterator.hasNext());
      }
    }
  }

  @Test
  public void testDataSourceForMVColumns() {
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      if (!fieldSpec.isSingleValueField()) {
        String column = fieldSpec.getName();
        DataSource actualDataSource = _mutableSegmentImpl.getDataSource(column);
        DataSource expectedDataSource = _immutableSegment.getDataSource(column);

        Dictionary actualDictionary = actualDataSource.getDictionary();
        Dictionary expectedDictionary = expectedDataSource.getDictionary();
        Assert.assertEquals(actualDictionary.length(), expectedDictionary.length());

        BlockMultiValIterator actualMVIterator =
            (BlockMultiValIterator) actualDataSource.nextBlock().getBlockValueSet().iterator();
        BlockMultiValIterator expectedMVIterator =
            (BlockMultiValIterator) expectedDataSource.nextBlock().getBlockValueSet().iterator();

        int maxNumValuesPerMVEntry = expectedDataSource.getDataSourceMetadata().getMaxNumValuesPerMVEntry();
        int[] actualDictIds = new int[maxNumValuesPerMVEntry];
        int[] expectedDictIds = new int[maxNumValuesPerMVEntry];

        while (expectedMVIterator.hasNext()) {
          Assert.assertTrue(actualMVIterator.hasNext());

          int actualNumMultiValues = actualMVIterator.nextIntVal(actualDictIds);
          int expectedNumMultiValues = expectedMVIterator.nextIntVal(expectedDictIds);
          Assert.assertEquals(actualNumMultiValues, expectedNumMultiValues);

          for (int i = 0; i < expectedNumMultiValues; i++) {
            Assert.assertEquals(actualDictionary.get(actualDictIds[i]), expectedDictionary.get(expectedDictIds[i]));
          }
        }
        Assert.assertFalse(actualMVIterator.hasNext());
      }
    }
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @Test
  public void testNestedDataSource() throws Exception {
    URL resourceUrl = MutableSegmentImplTest.class.getClassLoader().getResource(JSON_FILE);
    Assert.assertNotNull(resourceUrl);
    File jsonFile = new File(resourceUrl.getFile());
    Schema schema = new Schema.SchemaBuilder().addNested("person", FieldSpec.DataType.STRUCT).build();
    VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSegmentSchema(schema, "segmentWithNestedColumn");
    MutableSegmentImpl mutableSegmentImpl = MutableSegmentImplTestUtils
        .createMutableSegmentImpl(schema, Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
            false);
    try (RecordReader recordReader = RecordReaderFactory.getRecordReader(FileFormat.JSON, jsonFile, schema, null)) {
      GenericRow reuse = new GenericRow();
      while (recordReader.hasNext()) {
        mutableSegmentImpl.index(recordReader.next(reuse), null);
      }
    }

    DataSource rootDataSource = mutableSegmentImpl.getDataSource("person");
    Assert.assertNotNull(rootDataSource);
    Assert.assertTrue(rootDataSource instanceof StructDataSource);
    DataSource nameDataSource = rootDataSource.getDataSource("person.name");
    Assert.assertNotNull(nameDataSource);
    Assert.assertTrue(nameDataSource instanceof MutableDataSource);
    DataSource ageDataSource = rootDataSource.getDataSource("person.age");
    Assert.assertNotNull(ageDataSource);
    Assert.assertTrue(ageDataSource instanceof MutableDataSource);
    DataSource salaryDataSource = rootDataSource.getDataSource("person.salary");
    Assert.assertNotNull(salaryDataSource);
    Assert.assertTrue(salaryDataSource instanceof MutableDataSource);
    DataSource addressesDataSource = rootDataSource.getDataSource("person.addresses");
    Assert.assertNotNull(addressesDataSource);
    Assert.assertTrue(addressesDataSource instanceof ListDataSource);
    DataSource addressDataSource = addressesDataSource.getDataSource("");
    Assert.assertNotNull(addressDataSource);
    Assert.assertTrue(addressDataSource instanceof StructDataSource);
    DataSource aptDataSource = addressDataSource.getDataSource("person.addresses.apt");
    Assert.assertNotNull(aptDataSource);
    Assert.assertTrue(aptDataSource instanceof MutableDataSource);
    DataSource zipDataSource = addressDataSource.getDataSource("person.addresses.zip");
    Assert.assertNotNull(ageDataSource);
    Assert.assertTrue(zipDataSource instanceof MutableDataSource);
    DataSource phonesDataSource = addressDataSource.getDataSource("person.addresses.phones");
    Assert.assertNotNull(phonesDataSource);
    Assert.assertTrue(phonesDataSource instanceof ListDataSource);
    DataSource phoneDataSource = phonesDataSource.getDataSource("");
    Assert.assertNotNull(phoneDataSource);
    Assert.assertTrue(phoneDataSource instanceof StructDataSource);
    DataSource phoneNumberDataSource = phoneDataSource.getDataSource("person.addresses.phones.phonenumber");
    Assert.assertNotNull(phoneNumberDataSource);
    Assert.assertTrue(phoneNumberDataSource instanceof MutableDataSource);
    DataSource typeDataSource = phoneDataSource.getDataSource("person.addresses.phones.type");
    Assert.assertNotNull(typeDataSource);
    Assert.assertTrue(typeDataSource instanceof MutableDataSource);

    Assert.assertEquals(mutableSegmentImpl.getNumDocsIndexed(), 3);
    Assert.assertEquals(mutableSegmentImpl.getNumFlattenedDocsIndexed(0), 6);
    Assert.assertEquals(mutableSegmentImpl.getNumFlattenedDocsIndexed(1), 1);
    Assert.assertEquals(mutableSegmentImpl.getNumFlattenedDocsIndexed(2), 3);

    // test dictionary and forward index for phone-number
    SingleColumnSingleValueReader fwdIndex = (SingleColumnSingleValueReader)phoneNumberDataSource.getForwardIndex();
    Dictionary dictionary = phoneNumberDataSource.getDictionary();

    Assert.assertEquals(dictionary.getIntValue(0), 123456);
    Assert.assertEquals(dictionary.getIntValue(1), 565676);
    Assert.assertEquals(dictionary.getIntValue(2), 212123244);
    Assert.assertEquals(dictionary.getIntValue(3), 212121);
    Assert.assertEquals(dictionary.getIntValue(4), 32322);
    Assert.assertEquals(dictionary.getIntValue(5), 232424);
    Assert.assertEquals(dictionary.getIntValue(6), 412482);
    Assert.assertEquals(dictionary.getIntValue(7), 4374637);
    Assert.assertEquals(dictionary.getIntValue(8), 132323);

    Assert.assertEquals(fwdIndex.getInt(0), 0);
    Assert.assertEquals(fwdIndex.getInt(1), 1);
    Assert.assertEquals(fwdIndex.getInt(2), 2);
    Assert.assertEquals(fwdIndex.getInt(3), 3);
    Assert.assertEquals(fwdIndex.getInt(4), 4);
    Assert.assertEquals(fwdIndex.getInt(5), 5);
    Assert.assertEquals(fwdIndex.getInt(6), 6);
    // 4374637 phonenumber is repeated
    Assert.assertEquals(fwdIndex.getInt(7), 7);
    Assert.assertEquals(fwdIndex.getInt(8), 7);
    Assert.assertEquals(fwdIndex.getInt(9), 8);

    // test dictionary and forward index for phone-type
    fwdIndex = (SingleColumnSingleValueReader)typeDataSource.getForwardIndex();
    dictionary = typeDataSource.getDictionary();

    Assert.assertEquals(dictionary.getStringValue(0), "mobile");
    Assert.assertEquals(dictionary.getStringValue(1), "work");
    Assert.assertEquals(dictionary.getStringValue(2), "emergency");
    Assert.assertEquals(dictionary.getStringValue(3), "landline");

    // john
    Assert.assertEquals(fwdIndex.getInt(0), 0); // mobile
    Assert.assertEquals(fwdIndex.getInt(1), 1); // work
    Assert.assertEquals(fwdIndex.getInt(2), 2); // landline
    Assert.assertEquals(fwdIndex.getInt(3), 0); // mobile
    Assert.assertEquals(fwdIndex.getInt(4), 1); // work
    Assert.assertEquals(fwdIndex.getInt(5), 3); // emergency
    // sidd
    Assert.assertEquals(fwdIndex.getInt(6), 0); // mobile
    // abc
    Assert.assertEquals(fwdIndex.getInt(7), 0); // mobile
    Assert.assertEquals(fwdIndex.getInt(8), 0); // mobile
    Assert.assertEquals(fwdIndex.getInt(9), 1); // work

    // test dictionary and forward index for apt
    fwdIndex = (SingleColumnSingleValueReader)aptDataSource.getForwardIndex();
    dictionary = aptDataSource.getDictionary();

    Assert.assertEquals(dictionary.getIntValue(0), 1012);
    Assert.assertEquals(dictionary.getIntValue(1), 1022);
    Assert.assertEquals(dictionary.getIntValue(2), 1000);
    Assert.assertEquals(dictionary.getIntValue(3), 1073);
    Assert.assertEquals(dictionary.getIntValue(4), 1017);

    // john
    Assert.assertEquals(fwdIndex.getInt(0), 0); // 1012
    Assert.assertEquals(fwdIndex.getInt(1), 0);
    Assert.assertEquals(fwdIndex.getInt(2), 0);
    Assert.assertEquals(fwdIndex.getInt(3), 1); // 1022
    Assert.assertEquals(fwdIndex.getInt(4), 1);
    Assert.assertEquals(fwdIndex.getInt(5), 1);
    // sidd
    Assert.assertEquals(fwdIndex.getInt(6), 2); // 1000
    // abc
    Assert.assertEquals(fwdIndex.getInt(7), 3); // 1073
    Assert.assertEquals(fwdIndex.getInt(8), 4); // 1017
    Assert.assertEquals(fwdIndex.getInt(9), 4); // 1017

    // test dictionary and forward index for zip
    fwdIndex = (SingleColumnSingleValueReader)zipDataSource.getForwardIndex();
    dictionary = zipDataSource.getDictionary();

    Assert.assertEquals(dictionary.getIntValue(0), 94404);
    Assert.assertEquals(dictionary.getIntValue(1), 94402);
    Assert.assertEquals(dictionary.getIntValue(2), 95136);

    // john
    Assert.assertEquals(fwdIndex.getInt(0), 0); // 94404
    Assert.assertEquals(fwdIndex.getInt(1), 0);
    Assert.assertEquals(fwdIndex.getInt(2), 0);
    Assert.assertEquals(fwdIndex.getInt(3), 1); // 94402
    Assert.assertEquals(fwdIndex.getInt(4), 1);
    Assert.assertEquals(fwdIndex.getInt(5), 1);
    // sidd
    Assert.assertEquals(fwdIndex.getInt(6), 2); // 95136
    // abc
    Assert.assertEquals(fwdIndex.getInt(7), 0); // 94404
    Assert.assertEquals(fwdIndex.getInt(8), 0); // 94404
    Assert.assertEquals(fwdIndex.getInt(9), 0); // 94404

    // test dictionary and forward index for salary
    fwdIndex = (SingleColumnSingleValueReader)salaryDataSource.getForwardIndex();
    dictionary = salaryDataSource.getDictionary();

    Assert.assertEquals(dictionary.getIntValue(0), 100000);
    Assert.assertEquals(dictionary.getIntValue(1), 200000);

    // john
    Assert.assertEquals(fwdIndex.getInt(0), 0); // 100,000
    Assert.assertEquals(fwdIndex.getInt(1), 0);
    Assert.assertEquals(fwdIndex.getInt(2), 0);
    Assert.assertEquals(fwdIndex.getInt(3), 0);
    Assert.assertEquals(fwdIndex.getInt(4), 0);
    Assert.assertEquals(fwdIndex.getInt(5), 0);
    // sidd
    Assert.assertEquals(fwdIndex.getInt(6), 1); // 200,000
    // abc
    Assert.assertEquals(fwdIndex.getInt(7), 1); // 200,000
    Assert.assertEquals(fwdIndex.getInt(8), 1);
    Assert.assertEquals(fwdIndex.getInt(9), 1);

    // test dictionary and forward index for age
    fwdIndex = (SingleColumnSingleValueReader)ageDataSource.getForwardIndex();
    dictionary = ageDataSource.getDictionary();

    Assert.assertEquals(dictionary.getIntValue(0), 25);
    Assert.assertEquals(dictionary.getIntValue(1), 26);

    // john
    Assert.assertEquals(fwdIndex.getInt(0), 0); // 25
    Assert.assertEquals(fwdIndex.getInt(1), 0);
    Assert.assertEquals(fwdIndex.getInt(2), 0);
    Assert.assertEquals(fwdIndex.getInt(3), 0);
    Assert.assertEquals(fwdIndex.getInt(4), 0);
    Assert.assertEquals(fwdIndex.getInt(5), 0);
    // sidd
    Assert.assertEquals(fwdIndex.getInt(6), 1); // 26
    // abc
    Assert.assertEquals(fwdIndex.getInt(7), 0); // 25
    Assert.assertEquals(fwdIndex.getInt(8), 0);
    Assert.assertEquals(fwdIndex.getInt(9), 0);

    // test dictionary and forward index for name
    fwdIndex = (SingleColumnSingleValueReader)nameDataSource.getForwardIndex();
    dictionary = nameDataSource.getDictionary();

    Assert.assertEquals(dictionary.getStringValue(0), "john");
    Assert.assertEquals(dictionary.getStringValue(1), "sidd");
    Assert.assertEquals(dictionary.getStringValue(2), "abc");

    // john
    Assert.assertEquals(fwdIndex.getInt(0), 0);
    Assert.assertEquals(fwdIndex.getInt(1), 0);
    Assert.assertEquals(fwdIndex.getInt(2), 0);
    Assert.assertEquals(fwdIndex.getInt(3), 0);
    Assert.assertEquals(fwdIndex.getInt(4), 0);
    Assert.assertEquals(fwdIndex.getInt(5), 0);
    // sidd
    Assert.assertEquals(fwdIndex.getInt(6), 1);
    // abc
    Assert.assertEquals(fwdIndex.getInt(7), 2);
    Assert.assertEquals(fwdIndex.getInt(8), 2);
    Assert.assertEquals(fwdIndex.getInt(9), 2);
  }
}
