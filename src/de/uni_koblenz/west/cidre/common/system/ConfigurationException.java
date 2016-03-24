package de.uni_koblenz.west.cidre.common.system;

/**
 * Exception thrown when a CIDRE component has a erroneous configuration.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ConfigurationException extends Exception {

  private static final long serialVersionUID = 3087580307699520271L;

  public ConfigurationException() {
    super();
  }

  public ConfigurationException(String message) {
    super(message);
  }

  public ConfigurationException(Throwable cause) {
    super(cause);
  }

  public ConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConfigurationException(String message, Throwable cause, boolean enableSuppression,
          boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

}
