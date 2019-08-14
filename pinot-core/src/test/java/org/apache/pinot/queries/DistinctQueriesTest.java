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
package org.apache.pinot.queries;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.common.data.DimensionFieldSpec;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.MetricFieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.TimeFieldSpec;
import org.apache.pinot.common.data.TimeGranularitySpec;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.SelectionResults;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.common.utils.time.TimeUtils;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.data.readers.RecordReader;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.query.aggregation.DistinctTable;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Class to test DISTINCT queries.
 * Generates custom data set with explicitly generating
 * duplicate rows/keys
 */
public class DistinctQueriesTest extends BaseQueriesTest {
  private static final int NUM_ROWS = 5_000_000;

  private List<GenericRow> _rows;

  private static String D1 = "STRING_COL1";
  private static String D2 = "STRING_COL2";
  private static String M1 = "INT_COL";
  private static String M2 = "LONG_COL";
  private static String TIME = "TIME_COL";

  // in the custom data set, each row is repeated after 20 rows
  private static final int TUPLE_REPEAT_INTERVAL = 20;
  // in the custom data set, each row is repeated 5 times, total 1 million unique rows in dataset
  private static final int PER_TUPLE_REPEAT_FREQUENCY = 5;
  private static final int NUM_UNIQUE_TUPLES = NUM_ROWS/PER_TUPLE_REPEAT_FREQUENCY;

  private static final int INT_BASE_VALUE = 10000;
  private static final int INT_INCREMENT = 500;
  private static final long LONG_BASE_VALUE = 100000000;
  private static final long LONG_INCREMENT = 5500;
  private static final String STRING_BASE_VALUE1 = "Distinct";
  private static final String STRING_BASE_VALUE2 = "PinotFeature";

  private static final String TABLE_NAME = "DistinctTestTable";
  private static final int NUM_SEGMENTS = 2;
  private static final String SEGMENT_NAME_1 = TABLE_NAME + "_100000000_200000000";
  private static final String SEGMENT_NAME_2 = TABLE_NAME + "_300000000_400000000";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "DistinctQueryTest");

  private List<IndexSegment> _indexSegments = new ArrayList<>(NUM_SEGMENTS);
  private List<SegmentDataManager> _segmentDataManagers;
  private final Set<Key> _expectedAddTransformResults = new HashSet<>();
  private final Set<Key> _expectedSubTransformResults = new HashSet<>();
  private final Set<Key> _expectedAddSubTransformResults = new HashSet<>();
  private final Set<Key> _expectedResults = new HashSet<>();
  private final List<FieldSpec> _fieldSpecs = new ArrayList<>();

  private Schema _schema;

  @BeforeClass
  public void setUp() {
    Pql2Compiler.ENABLE_DISTINCT = true;
    _schema = createPinotTableSchema();
    _rows = createTestData(_schema);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private Schema createPinotTableSchema() {
    Schema testSchema = new Schema();
    testSchema.setSchemaName(TABLE_NAME);

    final DimensionFieldSpec d1Spec = new DimensionFieldSpec(D1, FieldSpec.DataType.STRING, true);
    _fieldSpecs.add(d1Spec);
    testSchema.addField(d1Spec);

    final DimensionFieldSpec d2Spec = new DimensionFieldSpec(D2, FieldSpec.DataType.STRING, true);
    _fieldSpecs.add(d2Spec);
    testSchema.addField(d2Spec);

    final MetricFieldSpec m1Spec = new MetricFieldSpec(M1, FieldSpec.DataType.INT);
    _fieldSpecs.add(m1Spec);
    testSchema.addField(m1Spec);

    final MetricFieldSpec m2Spec = new MetricFieldSpec(M2, FieldSpec.DataType.LONG);
    _fieldSpecs.add(m2Spec);
    testSchema.addField(m2Spec);

    final TimeFieldSpec tSpec = new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, TIME));
    _fieldSpecs.add(tSpec);
    testSchema.addField(tSpec);

    return testSchema;
  }

  /**
   * Custom data generator that explicitly generates duplicate
   * rows in dataset for purpose of testing DISTINCT functionality
   * @param schema Pinot table schema
   * @return List of {@link GenericRow}
   */
  private List<GenericRow> createTestData(Schema schema) {
    List<GenericRow> rows = new ArrayList<>();
    ThreadLocalRandom random = ThreadLocalRandom.current();
    Map<String, Object> fields;
    int pos = 0;
    for (int rowIndex = 0; rowIndex < NUM_ROWS; rowIndex++) {
      double addition = 0.0;
      double subtraction = 0.0;
      Object[] columnValues = new Object[schema.size() - 1];
      // generate each row
      fields = new HashMap<>();
      boolean duplicate = false;
      int col = 0;
      for (FieldSpec fieldSpec : _fieldSpecs) {
        // generate each column for the row
        Object value = null;
        if (fieldSpec instanceof TimeFieldSpec) {
          final long milliMin = TimeUtils.getValidMinTimeMillis();
          final long milliMax = TimeUtils.getValidMaxTimeMillis();
          // time column is simply there, we don't use it in DISTINCT query
          value = random.nextLong(milliMin, milliMax);
        } else {
          if (rowIndex == 0) {
            switch (fieldSpec.getDataType()) {
              case INT:
                value= INT_BASE_VALUE;
                break;
              case LONG:
                value = LONG_BASE_VALUE;
                break;
              case STRING:
                if (fieldSpec.getName().equals(D1)) {
                  value = STRING_BASE_VALUE1;
                } else {
                  value = STRING_BASE_VALUE2;
                }
                break;
            }
          } else {
            if (rowIndex == pos + (TUPLE_REPEAT_INTERVAL * PER_TUPLE_REPEAT_FREQUENCY)) {
              pos = rowIndex;
            }
            if (rowIndex < pos + TUPLE_REPEAT_INTERVAL) {
              // generate unique row
              switch (fieldSpec.getDataType()) {
                case INT:
                  value = (Integer)rows.get(rowIndex - 1).getValue(fieldSpec.getName()) + INT_INCREMENT;
                  break;
                case LONG:
                  value = (Long)rows.get(rowIndex - 1).getValue(fieldSpec.getName()) + LONG_INCREMENT;
                  break;
                case STRING:
                  if (fieldSpec.getName().equals(D1)) {
                    value = STRING_BASE_VALUE1 + RandomStringUtils.randomAlphabetic(2);
                  } else {
                    value = STRING_BASE_VALUE2 + RandomStringUtils.randomAlphabetic(2);
                  }
                  break;
              }
            } else {
              // generate duplicate row
              duplicate = true;
              switch (fieldSpec.getDataType()) {
                case INT:
                  value = rows.get(rowIndex - TUPLE_REPEAT_INTERVAL).getValue(fieldSpec.getName());
                  break;
                case LONG:
                  value = rows.get(rowIndex - TUPLE_REPEAT_INTERVAL).getValue(fieldSpec.getName());
                  break;
                case STRING:
                  value = rows.get(rowIndex - TUPLE_REPEAT_INTERVAL).getValue(fieldSpec.getName());
                  break;
              }
            }
          }
        }

        fields.put(fieldSpec.getName(), value);
        if (!(fieldSpec instanceof TimeFieldSpec)) {
          // we are not using the segment time column in distinct query
          // it is simply there in the segment
          columnValues[col++] = value;
        }
      } // end of generating all column values

      // now build GenericRow from column values
      GenericRow row = new GenericRow();
      row.init(fields);
      rows.add(row);

      addition = ((Integer)columnValues[2]) + ((Long)columnValues[3]);
      subtraction = ((Long)columnValues[3]) - ((Integer)columnValues[2]);

      // also keep building the expected result table
      if (!duplicate) {
        Key key = new Key(columnValues);
        _expectedResults.add(key);
      }

      _expectedAddTransformResults.add(new Key(new Object[]{addition}));
      _expectedSubTransformResults.add(new Key(new Object[]{subtraction}));
      _expectedAddSubTransformResults.add(new Key(new Object[]{addition, subtraction}));
    }

    return rows;
  }

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegments.get(0);
  }

  @Override
  protected List<SegmentDataManager> getSegmentDataManagers() {
    return _segmentDataManagers;
  }

  private void createSegment(
      Schema schema,
      RecordReader recordReader,
      String segmentName,
      String tableName)
      throws Exception {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(schema);
    segmentGeneratorConfig.setTableName(tableName);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    segmentGeneratorConfig.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, recordReader);
    driver.build();

    File segmentIndexDir = new File(INDEX_DIR.getAbsolutePath(), segmentName);
    if (!segmentIndexDir.exists()) {
      throw new IllegalStateException("Segment generation failed");
    }
  }

  private ImmutableSegment loadSegment(String segmentName)
      throws Exception {
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), ReadMode.heap);
  }

  /**
   * Test DISTINCT query with multiple columns on generated data set.
   * All the generated dataset is put into a single segment
   * and we directly run the {@link AggregationOperator} to
   * get segment level execution results.
   * The results are then compared to the expected result table
   * that was build during data generation
   * @throws Exception
   */
  @Test
  public void testDistinctInnerSegment() throws Exception {
    try {
      // put all the generated dataset in a single segment
      try (RecordReader _recordReader = new GenericRowRecordReader(_rows, _schema)) {
        createSegment(_schema, _recordReader, SEGMENT_NAME_1, TABLE_NAME);
        final ImmutableSegment immutableSegment = loadSegment(SEGMENT_NAME_1);
        _indexSegments.add(immutableSegment);

        // All unique 1 million rows should be returned
        String query = "SELECT DISTINCT(STRING_COL1, STRING_COL2, INT_COL, LONG_COL) FROM DistinctTestTable LIMIT 2000000";
        innerSegmentTestHelper(query, NUM_UNIQUE_TUPLES);

        // All unique 1 million rows should be returned
        query = "SELECT DISTINCT(STRING_COL1, STRING_COL2, INT_COL, LONG_COL) FROM DistinctTestTable LIMIT 1000000";
        innerSegmentTestHelper(query, NUM_UNIQUE_TUPLES);

        // 500k rows should be returned
        query = "SELECT DISTINCT(STRING_COL1, STRING_COL2, INT_COL, LONG_COL) FROM DistinctTestTable LIMIT 500000";
        innerSegmentTestHelper(query, 500000);

        // default: 10 rows should be returned
        query = "SELECT DISTINCT(STRING_COL1, STRING_COL2, INT_COL, LONG_COL) FROM DistinctTestTable";
        innerSegmentTestHelper(query, 10);

        // default: 10 rows should be returned
        query = "SELECT DISTINCT(add(INT_COL,LONG_COL)) FROM DistinctTestTable";
        innerSegmentTransformQueryTestHelper(query, 10, 1,
            new String[]{"add(INT_COL,LONG_COL)"}, new FieldSpec.DataType[]{FieldSpec.DataType.DOUBLE});

        // default: 10 rows should be returned
        query = "SELECT DISTINCT(sub(LONG_COL,INT_COL)) FROM DistinctTestTable";
        innerSegmentTransformQueryTestHelper(query, 10, 2,
            new String[]{"sub(LONG_COL,INT_COL)"}, new FieldSpec.DataType[]{FieldSpec.DataType.DOUBLE});

        // 500k rows' rows should be returned
        query = "SELECT DISTINCT(add(INT_COL,LONG_COL),sub(LONG_COL,INT_COL)) FROM DistinctTestTable LIMIT 500000 ";
        innerSegmentTransformQueryTestHelper(query, 500000, 3,
            new String[]{"add(INT_COL,LONG_COL)", "sub(LONG_COL,INT_COL)"},
            new FieldSpec.DataType[]{FieldSpec.DataType.DOUBLE, FieldSpec.DataType.DOUBLE});
      }
    } finally {
      destroySegments();
    }
  }

  /**
   * Helper for inner segment query tests
   * @param query query to run
   * @param expectedSize expected result size
   */
  private void innerSegmentTestHelper(final String query, final int expectedSize) {
    // compile to broker request and directly run the operator
    AggregationOperator aggregationOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
    final List<Object> operatorResult = resultsBlock.getAggregationResult();

    // verify resultset
    Assert.assertNotNull(operatorResult);
    Assert.assertEquals(operatorResult.size(), 1);
    Assert.assertTrue(operatorResult.get(0) instanceof DistinctTable);

    final DistinctTable distinctTable = (DistinctTable) operatorResult.get(0);
    Assert.assertEquals(_expectedResults.size(), NUM_UNIQUE_TUPLES);
    Assert.assertEquals(distinctTable.size(), expectedSize);

    final String[] columnNames = distinctTable.getProjectedColumnNames();
    Assert.assertEquals(columnNames.length, 4);
    Assert.assertEquals(columnNames[0], D1);
    Assert.assertEquals(columnNames[1], D2);
    Assert.assertEquals(columnNames[2], M1);
    Assert.assertEquals(columnNames[3], M2);

    final FieldSpec.DataType[] dataTypes = distinctTable.getProjectedColumnTypes();
    Assert.assertEquals(dataTypes.length, 4);
    Assert.assertEquals(dataTypes[0], FieldSpec.DataType.STRING);
    Assert.assertEquals(dataTypes[1], FieldSpec.DataType.STRING);
    Assert.assertEquals(dataTypes[2], FieldSpec.DataType.INT);
    Assert.assertEquals(dataTypes[3], FieldSpec.DataType.LONG);

    Iterator<Key> iterator = distinctTable.getIterator();
    while (iterator.hasNext()) {
      Key key = iterator.next();
      Assert.assertEquals(key.getColumns().length, 4);
      Assert.assertTrue(_expectedResults.contains(key));
    }
  }

  /**
   * Helper for inner segment transform query tests
   * @param query query to run
   * @param expectedSize expected result size
   */
  private void innerSegmentTransformQueryTestHelper(
      final String query,
      final int expectedSize,
      final int op,
      final String[] columnNames,
      final FieldSpec.DataType[] columnTypes) {
    // compile to broker request and directly run the operator
    AggregationOperator aggregationOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
    final List<Object> operatorResult = resultsBlock.getAggregationResult();

    // verify resultset
    Assert.assertNotNull(operatorResult);
    Assert.assertEquals(operatorResult.size(), 1);
    Assert.assertTrue(operatorResult.get(0) instanceof DistinctTable);

    final DistinctTable distinctTable = (DistinctTable) operatorResult.get(0);
    Assert.assertEquals(distinctTable.size(), expectedSize);

    Assert.assertEquals(distinctTable.getProjectedColumnNames().length, columnNames.length);
    Assert.assertEquals(distinctTable.getProjectedColumnNames(), columnNames);
    Assert.assertEquals(distinctTable.getProjectedColumnTypes().length, columnNames.length);
    Assert.assertEquals(distinctTable.getProjectedColumnTypes(), columnTypes);

    Iterator<Key> iterator = distinctTable.getIterator();
    while (iterator.hasNext()) {
      Key key = iterator.next();
      Assert.assertEquals(key.getColumns().length, columnNames.length);
      if (op == 1) {
        Assert.assertTrue(_expectedAddTransformResults.contains(key));
      } else if (op == 2) {
        Assert.assertTrue(_expectedSubTransformResults.contains(key));
      } else {
        Assert.assertTrue(_expectedAddSubTransformResults.contains(key));
      }
    }
  }

  /**
   * Test DISTINCT query with multiple columns on generated data set.
   * The generated dataset is divided into two segments.
   * We exercise the entire execution from broker ->
   * server -> segment. The server combines the results
   * from segments and sends the data table to broker.
   *
   * Currently the base class mimics the broker level
   * execution by duplicating the data table to mimic
   * two servers and then doing the merge
   *
   * The results are then compared to the expected result table
   * that was build during data generation
   * @throws Exception
   */
  @Test(dependsOnMethods={"testDistinctInnerSegment"})
  public void testDistinctInterSegmentInterServer() throws Exception {
    try {
      final List<GenericRow> randomRows = new ArrayList<>();
      final List<GenericRow> copiedRows = new ArrayList<>(_rows);
      final int size = copiedRows.size();
      for (int row = size - 1 ; row >= size/2; row--) {
        randomRows.add(copiedRows.remove(row));
      }

      try (RecordReader recordReader1 = new GenericRowRecordReader(copiedRows, _schema);
          RecordReader recordReader2 = new GenericRowRecordReader(randomRows, _schema)) {
        createSegment(_schema, recordReader1, SEGMENT_NAME_1, TABLE_NAME);
        createSegment(_schema, recordReader2, SEGMENT_NAME_2, TABLE_NAME);
        final ImmutableSegment segment1 = loadSegment(SEGMENT_NAME_1);
        final ImmutableSegment segment2 = loadSegment(SEGMENT_NAME_2);

        _indexSegments.add(segment1);
        _indexSegments.add(segment2);
        _segmentDataManagers = Arrays.asList(new ImmutableSegmentDataManager(segment1), new ImmutableSegmentDataManager(segment2));

        // All unique 1 million rows should be returned
        String query = "SELECT DISTINCT(STRING_COL1, STRING_COL2, INT_COL, LONG_COL) FROM DistinctTestTable LIMIT 2000000";
        interSegmentInterServerTestHelper(query, NUM_UNIQUE_TUPLES);

        // All unique 1 million rows should be returned
        query = "SELECT DISTINCT(STRING_COL1, STRING_COL2, INT_COL, LONG_COL) FROM DistinctTestTable LIMIT 1000000";
        interSegmentInterServerTestHelper(query, NUM_UNIQUE_TUPLES);

        // 500k unique rows should be returned
        query = "SELECT DISTINCT(STRING_COL1, STRING_COL2, INT_COL, LONG_COL) FROM DistinctTestTable LIMIT 500000";
        interSegmentInterServerTestHelper(query, 500000);

        // Default: 10 unique rows should be returned
        query = "SELECT DISTINCT(STRING_COL1, STRING_COL2, INT_COL, LONG_COL) FROM DistinctTestTable";
        interSegmentInterServerTestHelper(query, 10);
      }
    } finally {
      destroySegments();
    }
  }

  /**
   * Helper for inter segment, inter server query tests
   * @param query query to run
   * @param expectedSize expected result size
   */
  private void interSegmentInterServerTestHelper(String query, int expectedSize) {
    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    final SelectionResults selectionResults = brokerResponse.getSelectionResults();

    Assert.assertEquals(selectionResults.getColumns().size(), 4);
    Assert.assertEquals(selectionResults.getColumns().get(0), D1);
    Assert.assertEquals(selectionResults.getColumns().get(1), D2);
    Assert.assertEquals(selectionResults.getColumns().get(2), M1);
    Assert.assertEquals(selectionResults.getColumns().get(3), M2);

    Assert.assertEquals(_expectedResults.size(), NUM_UNIQUE_TUPLES);
    Assert.assertEquals(selectionResults.getRows().size(), expectedSize);

    for (Serializable[] row : selectionResults.getRows()) {
      Assert.assertEquals(row.length, 4);
      Key key = new Key(row);
      Assert.assertTrue(_expectedResults.contains(key));
    }
  }

  /**
   * Test DISTINCT queries on multiple columns with FILTER.
   * A simple hand-written data set of 10 rows in a single segment
   * is used for FILTER based queries as opposed to generated data set.
   * The results are compared to expected table.
   *
   * Runs 4 different queries with predicates.
   * @throws Exception
   */
  @Test(dependsOnMethods={"testDistinctInterSegmentInterServer"})
  public void testDistinctInnerSegmentWithFilter() throws Exception {
    try {
      final Schema schema = new Schema();
      final String tableName = TABLE_NAME + "WithFilter";
      schema.setSchemaName(tableName);

      final DimensionFieldSpec d1Spec = new DimensionFieldSpec("State", FieldSpec.DataType.STRING, true);
      _fieldSpecs.add(d1Spec);
      schema.addField(d1Spec);

      final DimensionFieldSpec d2Spec = new DimensionFieldSpec("City", FieldSpec.DataType.STRING, true);
      _fieldSpecs.add(d2Spec);
      schema.addField(d2Spec);

      final MetricFieldSpec m1Spec = new MetricFieldSpec("SaleAmount", FieldSpec.DataType.INT);
      _fieldSpecs.add(m1Spec);
      schema.addField(m1Spec);

      final TimeFieldSpec tSpec = new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, TIME));
      _fieldSpecs.add(tSpec);
      schema.addField(tSpec);

      final String query1 = "SELECT DISTINCT(State, City) FROM " + tableName + " WHERE SaleAmount >= 200000";
      final String query2 = "SELECT DISTINCT(State, City) FROM " + tableName + " WHERE SaleAmount >= 400000";
      final String query3 = "SELECT DISTINCT(State, City, SaleAmount) FROM " + tableName + " WHERE SaleAmount >= 200000";
      final String query4 = "SELECT DISTINCT(State, City, SaleAmount) FROM " + tableName + " WHERE SaleAmount >= 400000";

      final Set<Key> q1ExpectedResults = new HashSet<>();
      final Set<Key> q2ExpectedResults = new HashSet<>();
      final Set<Key> q3ExpectedResults = new HashSet<>();
      final Set<Key> q4ExpectedResults = new HashSet<>();

      final List<GenericRow> rows = createSimpleTable(q1ExpectedResults, q2ExpectedResults, q3ExpectedResults, q4ExpectedResults);

      try (RecordReader recordReader = new GenericRowRecordReader(rows, schema)) {
        createSegment(schema, recordReader, SEGMENT_NAME_1, tableName);
        final ImmutableSegment segment = loadSegment(SEGMENT_NAME_1);
        _indexSegments.add(segment);

        runAndVerifyFilterQuery(q1ExpectedResults, query1, new String[]{"State", "City"},
            new FieldSpec.DataType[]{FieldSpec.DataType.STRING, FieldSpec.DataType.STRING});
        runAndVerifyFilterQuery(q2ExpectedResults, query2, new String[]{"State", "City"},
            new FieldSpec.DataType[]{FieldSpec.DataType.STRING, FieldSpec.DataType.STRING});
        runAndVerifyFilterQuery(q3ExpectedResults, query3, new String[]{"State", "City", "SaleAmount"},
            new FieldSpec.DataType[]{FieldSpec.DataType.STRING, FieldSpec.DataType.STRING, FieldSpec.DataType.INT});
        runAndVerifyFilterQuery(q4ExpectedResults, query4, new String[]{"State", "City", "SaleAmount"},
            new FieldSpec.DataType[]{FieldSpec.DataType.STRING, FieldSpec.DataType.STRING, FieldSpec.DataType.INT});
      }
    } finally {
      destroySegments();
    }
  }

  /**
   * Helper for testing filter queries
   * @param expectedTable expected result set
   * @param query query to run
   * @param columnNames name of columns
   * @param types data types
   */
  private void runAndVerifyFilterQuery(
      final Set<Key> expectedTable,
      final String query,
      String[] columnNames,
      FieldSpec.DataType[] types) {
    AggregationOperator aggregationOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
    List<Object> operatorResult = resultsBlock.getAggregationResult();

    Assert.assertNotNull(operatorResult);
    Assert.assertEquals(operatorResult.size(), 1);
    Assert.assertTrue(operatorResult.get(0) instanceof DistinctTable);

    final DistinctTable distinctTable = (DistinctTable)operatorResult.get(0);
    Assert.assertEquals(distinctTable.size(), expectedTable.size());
    Assert.assertEquals(distinctTable.getProjectedColumnNames(), columnNames);
    Assert.assertEquals(distinctTable.getProjectedColumnTypes(), types);

    Iterator<Key> iterator = distinctTable.getIterator();

    while (iterator.hasNext()) {
      Key key = iterator.next();
      Assert.assertEquals(key.getColumns().length, columnNames.length);
      Assert.assertTrue(expectedTable.contains(key));
    }
  }

  /**
   * Create a segment with simple table of (State, City, SaleAmount, Time)
   * @param q1ExpectedResults expected results of filter query 1
   * @param q2ExpectedResults expected results of filter query 1
   * @param q3ExpectedResults expected results of filter query 1
   * @param q4ExpectedResults expected results of filter query 1
   * @return list of generic rows
   */
  private List<GenericRow> createSimpleTable(
      final Set<Key> q1ExpectedResults,
      final Set<Key> q2ExpectedResults,
      final Set<Key> q3ExpectedResults,
      final Set<Key> q4ExpectedResults) {
    final ThreadLocalRandom random = ThreadLocalRandom.current();
    final int numRows = 10;
    final List<GenericRow> rows = new ArrayList<>(numRows);
    Object[] columns;

    // ROW 1
    GenericRow row = new GenericRow();
    columns = new Object[]{"California", "San Mateo", 500000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    row.putField(TIME, random.nextLong(TimeUtils.getValidMinTimeMillis(), TimeUtils.getValidMaxTimeMillis()));
    rows.add(row);

    Key key = new Key(new Object[]{columns[0], columns[1]});
    q1ExpectedResults.add(key);
    q2ExpectedResults.add(key);
    key = new Key(new Object[]{columns[0], columns[1], columns[2]});
    q3ExpectedResults.add(key);
    q4ExpectedResults.add(key);

    // ROW 2
    row = new GenericRow();
    columns = new Object[]{"California", "San Mateo", 400000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    row.putField(TIME, random.nextLong(TimeUtils.getValidMinTimeMillis(), TimeUtils.getValidMaxTimeMillis()));
    rows.add(row);

    key = new Key(new Object[]{columns[0], columns[1], columns[2]});
    q3ExpectedResults.add(key);
    q4ExpectedResults.add(key);

    // ROW 3
    row = new GenericRow();
    columns = new Object[]{"California", "Sunnyvale", 300000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    row.putField(TIME, random.nextLong(TimeUtils.getValidMinTimeMillis(), TimeUtils.getValidMaxTimeMillis()));
    rows.add(row);

    key = new Key(new Object[]{columns[0], columns[1]});
    q1ExpectedResults.add(key);
    key = new Key(new Object[]{columns[0], columns[1], columns[2]});
    q3ExpectedResults.add(key);

    // ROW 4
    row = new GenericRow();
    columns = new Object[]{"California", "Sunnyvale", 300000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    row.putField(TIME, random.nextLong(TimeUtils.getValidMinTimeMillis(), TimeUtils.getValidMaxTimeMillis()));
    rows.add(row);

    // ROW 5
    row = new GenericRow();
    columns = new Object[]{"California", "Mountain View", 700000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    row.putField(TIME, random.nextLong(TimeUtils.getValidMinTimeMillis(), TimeUtils.getValidMaxTimeMillis()));
    rows.add(row);

    key = new Key(new Object[]{columns[0], columns[1]});
    q1ExpectedResults.add(key);
    q2ExpectedResults.add(key);
    key = new Key(new Object[]{columns[0], columns[1], columns[2]});
    q3ExpectedResults.add(key);
    q4ExpectedResults.add(key);

    // ROW 6
    row = new GenericRow();
    columns = new Object[]{"California", "Mountain View", 700000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    row.putField(TIME, random.nextLong(TimeUtils.getValidMinTimeMillis(), TimeUtils.getValidMaxTimeMillis()));
    rows.add(row);

    // ROW 7
    row = new GenericRow();
    columns = new Object[]{"California", "Mountain View", 200000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    row.putField(TIME, random.nextLong(TimeUtils.getValidMinTimeMillis(), TimeUtils.getValidMaxTimeMillis()));
    rows.add(row);

    key = new Key(new Object[]{columns[0], columns[1], columns[2]});
    q3ExpectedResults.add(key);

    // ROW 8
    row = new GenericRow();
    columns = new Object[]{"Washington", "Seattle", 100000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    row.putField(TIME, random.nextLong(TimeUtils.getValidMinTimeMillis(), TimeUtils.getValidMaxTimeMillis()));
    rows.add(row);

    // ROW 9
    row = new GenericRow();
    columns = new Object[]{"Washington", "Bellevue", 100000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    row.putField(TIME, random.nextLong(TimeUtils.getValidMinTimeMillis(), TimeUtils.getValidMaxTimeMillis()));
    rows.add(row);

    // ROW 10
    row = new GenericRow();
    columns = new Object[]{"Oregon", "Portland", 50000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    row.putField(TIME, random.nextLong(TimeUtils.getValidMinTimeMillis(), TimeUtils.getValidMaxTimeMillis()));
    rows.add(row);

    return rows;
  }

  private void destroySegments() {
    for (IndexSegment indexSegment : _indexSegments) {
      if (indexSegment != null) {
        indexSegment.destroy();
      }
    }
    _indexSegments.clear();
  }
}
