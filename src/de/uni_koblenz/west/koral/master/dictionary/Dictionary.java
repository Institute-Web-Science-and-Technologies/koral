package de.uni_koblenz.west.koral.master.dictionary;

import java.io.Closeable;

/**
 * Declares all methods required by {@link DictionaryEncoder}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface Dictionary extends Closeable {

  /**
   * if value already exists, its id is returned. Otherwise if
   * <code>createNewEncodingForUnknownNodes == true</code> a new id is generated
   * whose first two bytes are 0 and if
   * <code>createNewEncodingForUnknownNodes == false</code>, 0 is returned.
   * 
   * @param value
   * @param createNewEncodingForUnknownNodes
   * @return
   * @throws RuntimeException
   *           if maximum number of strings (i.e., 2^48) have been encoded
   */
  public long encode(String value, boolean createNewEncodingForUnknownNodes);

  /**
   * @param id
   * @return <code>null</code> if no String has been encoded to this id, yet.
   */
  public String decode(long id);

  public void flush();

  public boolean isEmpty();

  public void clear();

  @Override
  public void close();

}
