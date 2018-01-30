package de.uni_koblenz.west.koral.master.utils;

import de.uni_koblenz.west.koral.common.io.LongOutputWriter;

import java.io.IOException;
import java.util.Comparator;

/**
 * Produces an initial unsorted chunk in main memory.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface InitialChunkProducer extends AutoCloseable {

  /**
   * This method must be called first.
   */
  public void loadNextChunk() throws IOException;

  /**
   * {@link #loadNextChunk()} must be called first.
   * 
   * @return
   */
  public boolean hasNextChunk();

  public void sort(Comparator<long[]> comparator);

  public void writeChunk(LongOutputWriter output) throws IOException;

  @Override
  public void close();

}
