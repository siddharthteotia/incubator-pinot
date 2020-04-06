package org.apache.pinot.core.segment.index.datasource;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.common.DataSourceMetadata;
import org.apache.pinot.core.io.reader.DataFileReader;
import org.apache.pinot.core.io.reader.SingleColumnMultiValueReader;
import org.apache.pinot.core.io.reader.SingleColumnSingleValueReader;
import org.apache.pinot.core.operator.blocks.MultiValueBlock;
import org.apache.pinot.core.operator.blocks.SingleValueBlock;
import org.apache.pinot.core.segment.index.readers.BloomFilterReader;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.apache.pinot.core.segment.index.readers.NullValueVectorReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.NestedFieldSpec;


public class StructDataSource extends DataSource {

  private final Map<String, DataSource> _childDataSources;

  public StructDataSource(FieldSpec fieldSpec, int numDocs) {
    Preconditions.checkArgument(fieldSpec instanceof NestedFieldSpec);
    _childDataSources = new HashMap<>();
  }

  public void addChildDataSource(String child, DataSource dataSource) {
    _childDataSources.put(child, dataSource);
  }

  @Override
  public DataSource getDataSource(String column) {
    return _childDataSources.get(column);
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata() {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataFileReader getForwardIndex() {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public Dictionary getDictionary() {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public InvertedIndexReader getInvertedIndex() {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public BloomFilterReader getBloomFilter() {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public NullValueVectorReader getNullValueVector() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected Block getNextBlock() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getOperatorName() {
    throw new UnsupportedOperationException();
  }
}
