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
package org.apache.pinot.core.segment.creator.impl.inv.text;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.core.segment.creator.InvertedIndexCreator;
import org.slf4j.LoggerFactory;


/**
 * This is used to create Lucene based text index.
 * Used for both offline from {@link org.apache.pinot.core.segment.creator.impl.SegmentColumnarIndexCreator}
 * and realtime from {@link org.apache.pinot.core.realtime.impl.invertedindex.RealtimeLuceneTextIndexReader}
 */
public class LuceneTextIndexCreator implements InvertedIndexCreator {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(LuceneTextIndexCreator.class);
  // TODO: make buffer size configurable choosing a default value based on the heap usage results in design doc
  private static final int LUCENE_INDEX_MAX_BUFFER_SIZE_MB = 500;

  public static final String LUCENE_INDEX_DOC_ID_COLUMN_NAME = "DocID";
  public static final String LUCENE_TEXT_INDEX_FILE_EXTENSION = ".lucene.index";

  private final String _textColumn;
  private final Directory _indexDirectory;
  private final IndexWriter _indexWriter;

  public static final CharArraySet ENGLISH_STOP_WORDS_SET =
      new CharArraySet(Arrays.asList(
          "a", "an", "and", "are", "as", "at", "be", "but", "by",
          "for", "if", "in", "into", "is", "it",
          "no", "not", "of", "on", "or", "such",
          "that", "the", "their", "then", "than", "there", "these",
          "they", "this", "to", "was", "will", "with", "those"
      ), true);

  /**
   * Called by {@link org.apache.pinot.core.segment.creator.impl.SegmentColumnarIndexCreator}
   * when building an offline segment. Similar to how it creates per column
   * dictionary, forward and inverted index, a text index is also created
   * if text search is enabled on a column.
   * @param column column name
   * @param segmentIndexDir segment index directory
   * @param commit true if the index should be committed (at the end after all documents have
   *               been added), false if index should not be committed
   * Note on commit:
   *               Once {@link org.apache.pinot.core.segment.creator.impl.SegmentColumnarIndexCreator}
   *               finishes indexing all documents/rows for the segment, we need to commit and close
   *               the Lucene index which will internally persist the index on disk, do the necessary
   *               resource cleanup etc. We commit during {@link InvertedIndexCreator#seal()}
   *               and close during {@link InvertedIndexCreator#close()}.
   *               This lucene index writer is used by both offline and realtime (both during
   *               indexing in-memory MutableSegment and later during conversion to offline).
   *               Since realtime segment conversion is again going to go through the offline
   *               indexing path and will do everything (indexing, commit, close etc), there is
   *               no need to commit the index from the realtime side. So when the realtime segment
   *               is destroyed (which is after the realtime segment has been committed and converted
   *               to offline), we close this lucene index writer to release resources but don't commit.
   *               This is the reason to have commit flag part of the constructor.
   */
  public LuceneTextIndexCreator(String column, File segmentIndexDir, boolean commit) {
    _textColumn = column;
    try {
      // segment generation is always in V1 and later we convert (as part of post creation processing)
      // to V3 if segmentVersion is set to V3 in SegmentGeneratorConfig.
      File indexFile = getV1TextIndexFile(segmentIndexDir);
      _indexDirectory = FSDirectory.open(indexFile.toPath());
      StandardAnalyzer standardAnalyzer = new StandardAnalyzer(ENGLISH_STOP_WORDS_SET);
      IndexWriterConfig indexWriterConfig = new IndexWriterConfig(standardAnalyzer);
      indexWriterConfig.setRAMBufferSizeMB(LUCENE_INDEX_MAX_BUFFER_SIZE_MB);
      indexWriterConfig.setCommitOnClose(commit);
      _indexWriter = new IndexWriter(_indexDirectory, indexWriterConfig);
    } catch (Exception e) {
      LOGGER.error("Failed to instantiate Lucene text index creator for column {}, exception {}", column, e.getMessage());
      throw new RuntimeException(e);
    }
  }

  public IndexWriter getIndexWriter() {
    return _indexWriter;
  }

  @Override
  public void addDoc(Object document, int docIdCounter) {
    // text index on SV column
    Document docToIndex = new Document();
    docToIndex.add(new TextField(_textColumn, document.toString(), Field.Store.NO));
    docToIndex.add(new StoredField(LUCENE_INDEX_DOC_ID_COLUMN_NAME, docIdCounter));
    try {
      _indexWriter.addDocument(docToIndex);
    } catch (Exception e) {
      LOGGER.error("Failure while adding a new document to index for column {}, exception {}", _textColumn, e.getMessage());
      throw new RuntimeException(e);
    }
  }

  @Override
  public void seal() {
    try {
      // Do this one time operation of combining the multiple lucene index files (if any)
      // into a single index file. Based on flush threshold and size of data, Lucene
      // can create index in multiple files and then uses a merge criteria to decide
      // if a single compound file (similar to Pinot's V3 format) should be created
      // holding all the index data.
      // Depending on the size of data, flush threshold (during index building) and the
      // outcome of Lucene's internal merge criteria, the final lucene index can have
      // multiple files. Since these files will be kept opened during segment load/mmap,
      // we want to minimize the number of situations that can lead to "too many open files"
      // error.
      // Of course, there is a worst case where there could be a table with 20k segments
      // and each segment has 3 TEXT columns, thus 3 Lucene indexes. So even with a compound
      // file, we are likely to exhaust the number of open file descriptors. In future, we
      // should probably explore a global lucene index (a single index for all TEXT columns)
      // as opposed to per column index.
      _indexWriter.forceMerge(1);
    } catch (Exception e) {
      LOGGER.error("Lucene index writer failed to commit for column {}, exception {}", _textColumn, e.getMessage());
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      // based on the commit flag set in IndexWriterConfig, this will decide to commit or not
      _indexWriter.close();
      _indexDirectory.close();
    } catch (Exception e) {
      LOGGER.error("Lucene index writer failed to close for column {}, exception {}", _textColumn, e.getMessage());
      throw new RuntimeException(e);
    }
  }

  @Override
  public void add(int dictId) {
    throw new IllegalStateException("Lucene text inverted index is not dictionary based");
  }

  @Override
  public void add(int[] dictIds, int length) {
    throw new IllegalStateException("Lucene text inverted index is not dictionary based");
  }

  private File getV1TextIndexFile(File indexDir) {
    String luceneIndexDirectory = _textColumn + LuceneTextIndexCreator.LUCENE_TEXT_INDEX_FILE_EXTENSION;
    return new File(indexDir, luceneIndexDirectory);
  }
}
