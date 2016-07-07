package de.uni_koblenz.west.koral.common.io;

import java.io.EOFException;
import java.io.IOException;
import java.util.Iterator;

/**
 * Iterator over an input file
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class EncodedFileInputIterator implements Iterator<Statement> {

  private final EncodedFileInputStream input;

  private EncodingFileFormat format;

  private byte[][] next;

  public EncodedFileInputIterator(EncodedFileInputStream input) {
    this.input = input;
    next = new byte[4][];
    getNext();
  }

  private void getNext() {
    try {
      Statement statement = input.read();
      format = statement.getFormat();
      next[0] = statement.getSubject();
      next[1] = statement.getProperty();
      next[2] = statement.getObject();
      next[3] = statement.getContainment();
    } catch (EOFException e) {
      // the input is read completely
      next = null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public Statement next() {
    byte[] subject = next[0];
    byte[] property = next[1];
    byte[] object = next[2];
    byte[] containment = next[3];
    getNext();
    return Statement.getStatement(format, subject, property, object, containment);
  }

}
