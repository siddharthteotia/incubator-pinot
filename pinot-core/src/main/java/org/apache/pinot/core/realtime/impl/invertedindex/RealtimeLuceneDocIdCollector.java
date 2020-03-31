package org.apache.pinot.core.realtime.impl.invertedindex;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * DocID collector for Lucene search query. We have optimized
 * the lucene search on offline segments by maintaining
 * a pre-built luceneDocId -> pinotDocId mapping. Since that solution
 * is not directly applicable to realtime, we will separate the collector
 * for the time-being. Once we have optimized the realtime, we can
 */
public class RealtimeLuceneDocIdCollector implements Collector {

  private final MutableRoaringBitmap _docIDs;

  public RealtimeLuceneDocIdCollector(MutableRoaringBitmap docIDs) {
    _docIDs = docIDs;
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) {
    return new LeafCollector() {

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        // we don't use scoring, so this is NO-OP
      }

      @Override
      public void collect(int doc) throws IOException {
        _docIDs.add(doc);
      }
    };
  }
}
