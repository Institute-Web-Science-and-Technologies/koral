package de.uni_koblenz.west.koral.master.utils;

import de.uni_koblenz.west.koral.common.io.LongOutputWriter;

import java.io.IOException;
import java.util.BitSet;

/**
 * Merges all occurences of the smallest element.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface Merger extends AutoCloseable {

  public void startNextMergeLevel();

  public long[] readNextElement(LongIterator iterator) throws IOException;

  public void mergeAndWrite(BitSet indicesOfSmallestElement, long[][] elements,
          LongIterator[] iterators, LongOutputWriter out) throws IOException;

  @Override
  public void close();

}
