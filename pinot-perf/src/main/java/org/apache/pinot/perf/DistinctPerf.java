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
package org.apache.pinot.perf;

import com.google.common.base.Stopwatch;
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javafx.scene.paint.Stop;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.common.data.DimensionFieldSpec;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.MetricFieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.TimeFieldSpec;
import org.apache.pinot.common.data.TimeGranularitySpec;
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
import org.apache.pinot.core.operator.query.DictionaryBasedAggregationOperator;
import org.apache.pinot.core.operator.query.SelectionOperator;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.aggregation.DistinctTable;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.queries.BaseQueriesTest;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
@State(Scope.Thread)
public class DistinctPerf extends BaseQueriesTest  {
  private static final int NUM_ROWS = 100_000;

  private static String D1 = "STRING_COL1";
  private static String D2 = "STRING_COL2";
  private static String M1 = "INT_COL";
  private static String M2 = "LONG_COL";
  private static String TIME = "TIME_COL";

  private static final int TUPLE_REPEAT_INTERVAL = 20;
  private static final int PER_TUPLE_REPEAT_FREQUENCY = 1;
  private static final int NUM_UNIQUE_TUPLES = NUM_ROWS/PER_TUPLE_REPEAT_FREQUENCY;

  private static int SMALL_STRING_LENGTH_1 = 10;
  private static int SMALL_STRING_LENGTH_2 = 15;
  private static int MEDIUM_STRING_LENGTH_1 = 25;
  private static int MEDIUM_STRING_LENGTH_2 = 50;

  private static final int INT_BASE_VALUE = 10000;
  private static final int INT_INCREMENT = 500;
  private static final long LONG_BASE_VALUE = 100000000;
  private static final long LONG_INCREMENT = 5500;
  private static final String SMALL_STRING1 = "Distinct01";
  private static final String SMALL_STRING2 = "PinotFeature221";
  private static final String MEDIUM_STRING1 = "Distinct in Pinot Sql";
  private static final String MEDIUM_STRING2 = "Feature to increase compatibility with standard Sql";
  private static final String STRING_BASE_VALUE1 = SMALL_STRING1;
  private static final String STRING_BASE_VALUE2 = SMALL_STRING2;

  private static final String TABLE_NAME = "DistinctTestTable";
  private static final int NUM_SEGMENTS = 2;
  private static final String SEGMENT_NAME_1 = TABLE_NAME + "_100000000_200000000";
  private static final String SEGMENT_NAME_2 = TABLE_NAME + "_300000000_400000000";
  private final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "DistinctPerfTest");

  private List<IndexSegment> _indexSegments = new ArrayList<>(NUM_SEGMENTS);
  private List<SegmentDataManager> _segmentDataManagers;
  private final List<FieldSpec> _fieldSpecs = new ArrayList<>();
  private List<GenericRow> _rows = new ArrayList<>();
  private Schema _schema;

  private final Set<Key> _expectedResults = new HashSet<>();
  private final Set<Key> _expectedQ1Results = new HashSet<>();
  private final Set<Key> _expectedQ2Results = new HashSet<>();

  private static final String QUERY_1 = "SELECT DISTINCT(STRING_COL1) FROM DistinctTestTable";
  private static final String QUERY_2 = "SELECT DISTINCT(STRING_COL1, STRING_COL2) FROM DistinctTestTable";
  private static final String QUERY_3 = "SELECT DISTINCT(STRING_COL1, STRING_COL2, INT_COL, LONG_COL) FROM DistinctTestTable";
  private static final String QUERY_4 = "SELECT STRING_COL1 FROM DistinctTestTable";

  AggregationOperator _opForQ1;
  AggregationOperator _opForQ2;
  AggregationOperator _opForQ3;
  DictionaryBasedAggregationOperator _dictAgg;
  SelectionOperator _selectionOperator;
  private boolean init = false;

  @Setup(Level.Trial)
  public void setupTable() throws Exception {
    if (!init) {
      //System.out.println(Thread.currentThread().getName() + " " + Thread.currentThread().getId());
      _schema = createPinotTableSchema();
      createTestData(_schema);
      final RecordReader recordReader1 = new GenericRowRecordReader(_rows, _schema);
      createSegment(_schema, recordReader1, SEGMENT_NAME_1, TABLE_NAME);
      final ImmutableSegment segment1 = loadSegment(SEGMENT_NAME_1);
      _indexSegments.add(segment1);
      init = true;
    }
  }

  @Setup(Level.Invocation)
  public void setupOperators() throws Exception {
    Pql2Compiler.ENABLE_DISTINCT = true;
    _opForQ1 = getOperatorForQuery(QUERY_1);
    //_dictAgg = getOperatorForQuery(QUERY_1);
    //_opForQ2 = getOperatorForQuery(QUERY_2);
    //_opForQ3 = getOperatorForQuery(QUERY_3);
    _selectionOperator = getOperatorForQuery(QUERY_4);
    //System.out.println(Thread.currentThread().getName() + " " + Thread.currentThread().getId());
    //System.out.println("created operators");
  }

  @TearDown(Level.Trial)
  public void destroySegment() {
    delete();
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

    final TimeFieldSpec
        tSpec = new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, TIME));
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
    final ThreadLocalRandom random = ThreadLocalRandom.current();
    Map<String, Object> fields;
    int pos = 0;
    final Object[] columnValues = new Object[schema.size() - 1];
    for (int rowIndex = 0; rowIndex < NUM_ROWS; rowIndex++) {
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
                  value = RandomStringUtils.randomAlphabetic(MEDIUM_STRING_LENGTH_2);
                } else {
                  value = RandomStringUtils.randomAlphabetic(MEDIUM_STRING_LENGTH_2);
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
                  value = (Integer)_rows.get(rowIndex - 1).getValue(fieldSpec.getName()) + INT_INCREMENT;
                  break;
                case LONG:
                  value = (Long)_rows.get(rowIndex - 1).getValue(fieldSpec.getName()) + LONG_INCREMENT;
                  break;
                case STRING:
                  if (fieldSpec.getName().equals(D1)) {
                    value = RandomStringUtils.randomAlphabetic(MEDIUM_STRING_LENGTH_2);
                  } else {
                    value = RandomStringUtils.randomAlphabetic(MEDIUM_STRING_LENGTH_2);
                  }
                  break;
              }
            } else {
              // generate duplicate row
              duplicate = true;
              System.out.println("generating duplicate");
              switch (fieldSpec.getDataType()) {
                case INT:
                  value = _rows.get(rowIndex - TUPLE_REPEAT_INTERVAL).getValue(fieldSpec.getName());
                  break;
                case LONG:
                  value = _rows.get(rowIndex - TUPLE_REPEAT_INTERVAL).getValue(fieldSpec.getName());
                  break;
                case STRING:
                  value = _rows.get(rowIndex - TUPLE_REPEAT_INTERVAL).getValue(fieldSpec.getName());
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
      _rows.add(row);

      if (!duplicate) {
        _expectedQ1Results.add(new Key(new Object[]{columnValues[0]}));
      }
    }

    System.out.println("CARDINALITY : " + _expectedQ1Results.size());
    return _rows;
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

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void testDistinctQ1InnerSegment(DistinctPerf distinctPerf) {
    IntermediateResultsBlock resultsBlock = distinctPerf._opForQ1.nextBlock();
    //List<Object> operatorResult = resultsBlock.getAggregationResult();
    //DistinctTable distinctTable = (DistinctTable)operatorResult.get(0);
    //System.out.println("Result size: " + distinctTable.size());
  }

  private void delete() {
    for (IndexSegment indexSegment : _indexSegments) {
      if (indexSegment != null) {
        indexSegment.destroy();
      }
    }
    _indexSegments.clear();
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(DistinctPerf.class.getSimpleName()).warmupTime(TimeValue.seconds(60))
            .warmupIterations(5).measurementTime(TimeValue.seconds(60)).measurementIterations(5).forks(1);

    new Runner(opt.build()).run();
  }
}
