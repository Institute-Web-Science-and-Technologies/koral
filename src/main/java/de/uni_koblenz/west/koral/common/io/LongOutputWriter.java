package de.uni_koblenz.west.koral.common.io;

import java.io.IOException;

/**
 * Writes a long value.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface LongOutputWriter extends AutoCloseable {

  public void writeLong(long value) throws IOException;

  @Override
  public void close() throws IOException;

}
