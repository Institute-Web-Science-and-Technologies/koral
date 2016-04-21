package de.uni_koblenz.west.koral.common.config;

/**
 * Exception thrown when the deserialization of the configuration fails.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class DeserializationException extends RuntimeException {

  private static final long serialVersionUID = 7961496401353020402L;

  public DeserializationException() {
  }

  public DeserializationException(String message) {
    super(message);
  }

  public DeserializationException(Throwable cause) {
    super(cause);
  }

  public DeserializationException(String message, Throwable cause) {
    super(message, cause);
  }

}
