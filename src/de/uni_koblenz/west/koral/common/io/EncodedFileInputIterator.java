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

  private Statement next;

  public EncodedFileInputIterator(EncodedFileInputStream input) {
    this.input = input;
    getNext();
  }

  private void getNext() {
    try {
      next = input.read();
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
    Statement nextS = next;
    getNext();
    return nextS;
  }

}
