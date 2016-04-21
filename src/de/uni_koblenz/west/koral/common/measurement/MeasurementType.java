package de.uni_koblenz.west.koral.common.measurement;

/**
 * Types of measured values
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public enum MeasurementType {

  /**
   * measured on master<br>
   * timestamp;clientAddress;clientID
   */
  CLIENT_STARTS_CONNECTION,

  /**
   * measured on master<br>
   * timestamp;clientAddress;clientID
   */
  CLIENT_CLOSES_CONNECTION,

  /**
   * measured on master<br>
   * timestamp;clientAddress;abortedCommand
   */
  CLIENT_ABORTS_CONNECTION,

  /**
   * measured on master<br>
   * timestamp;clientAddress;clientID
   */
  CLIENT_CONNECTION_TIMEOUT,

  /**
   * measured on master<br>
   * timestamp;clientID
   */
  CLIENT_DROP_START,

  /**
   * measured on master<br>
   * timestamp;clientID
   */
  CLIENT_DROP_END,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_MESSAGE_RECEIPTION,

  /**
   * measured on master<br>
   * timestamp;coverStrategy;nHopPathLength;numberOfChunks
   */
  LOAD_GRAPH_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_FILE_TRANSFER_TO_MASTER_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_FILE_TRANSFER_TO_MASTER_END,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_COVER_CREATION_START,

  /**
   * measured on master<br>
   * numberOfTriples
   */
  TOTAL_GRAPH_SIZE,

  /**
   * measured on master<br>
   * numberOfTriples*
   */
  INITIAL_CHUNK_SIZES,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_COVER_CREATION_END,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_NHOP_REPLICATION_START,

  /**
   * measured on master<br>
   * numberOfTriples*
   */
  REPLICATED_CHUNK_SIZES,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_NHOP_REPLICATION_END,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_ENCODING_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_ENCODING_END,

  /**
   * measured on slave<br>
   * timestamp
   */
  LOAD_GRAPH_FILE_TRANSFER_TO_SLAVES_START,

  /**
   * measured on slave<br>
   * timestamp
   */
  LOAD_GRAPH_FILE_TRANSFER_TO_SLAVES_END,

  /**
   * measured on slave<br>
   * timestamp
   */
  LOAD_GRAPH_STORING_TRIPLES_START,

  /**
   * measured on slave<br>
   * timestamp
   */
  LOAD_GRAPH_FILE_STORING_TRIPLES_END,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_FINISHED;

}
