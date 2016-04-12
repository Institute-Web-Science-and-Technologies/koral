package de.uni_koblenz.west.cidre.common.measurement;

/**
 * Types of measured values
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public enum MeasurementType {

  /**
   * timestamp;clientAddress;clientID
   */
  CLIENT_STARTS_CONNECTION,

  /**
   * timestamp;clientAddress;clientID
   */
  CLIENT_CLOSES_CONNECTION,

  /**
   * timestamp;clientAddress;abortedCommand
   */
  CLIENT_ABORTS_CONNECTION,

  /**
   * timestamp;clientAddress;clientID
   */
  CLIENT_CONNECTION_TIMEOUT,

  /**
   * timestamp;clientID
   */
  CLIENT_DROP_START,

  /**
   * timestamp;clientID
   */
  CLIENT_DROP_END;

}
