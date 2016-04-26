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
   * timestamp
   */
  LOAD_GRAPH_COVER_CREATION_HIERARCHY_LEVEL_IDENTIFICATION_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_COVER_CREATION_HIERARCHY_LEVEL_IDENTIFICATION_END,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_COVER_CREATION_METIS_INPUT_FILE_CREATION_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_COVER_CREATION_METIS_INPUT_FILE_CREATION_END,

  /**
   * measured on master<br>
   * numberOfIgnoredTriples
   */
  LOAD_GRAPH_COVER_CREATION_METIS_IGNORED_TRIPLES,

  /**
   * measured on master<br>
   * numberOfTriples;numberOfVertices;numberOfEdges
   */
  LOAD_GRAPH_COVER_CREATION_METIS_INPUT_GRAPH_SIZE,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_COVER_CREATION_RUN_METIS_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_COVER_CREATION_RUN_METIS_END,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_COVER_CREATION_FILE_WRITE_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_COVER_CREATION_FILE_WRITE_END,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_COVER_CREATION_END,

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
  LOAD_GRAPH_NHOP_REPLICATION_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_NHOP_REPLICATION_INIT_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_NHOP_REPLICATION_INIT_END,

  /**
   * measured on master<br>
   * timestamp;stepNumber
   */
  LOAD_GRAPH_NHOP_REPLICATION_STEP_START,

  /**
   * measured on master<br>
   * timestamp;stepNumber
   */
  LOAD_GRAPH_NHOP_REPLICATION_STEP_END,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_NHOP_REPLICATION_CONTAINMENT_UPDATE_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_NHOP_REPLICATION_CONTAINMENT_UPDATE_END,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_NHOP_REPLICATION_FILEWRITE_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_NHOP_REPLICATION_FILEWRITE_END,

  /**
   * measured on master<br>
   * numberOfTriples*
   */
  LOAD_GRAPH_REPLICATED_CHUNK_SIZES,

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
  LOAD_GRAPH_ENCODING_ENCODING_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_ENCODING_ENCODING_END,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_ENCODING_OWNERSHIP_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_ENCODING_OWNERSHIP_END,

  /**
   * measured on master<br>
   * numberOfOwnedRessourcesPerSlave*
   */
  LOAD_GRAPH_ENCODING_OWNERLOAD,

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
  LOAD_GRAPH_STORING_TRIPLES_END,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_FINISHED,

  /**
   * measured on master<br>
   * timestamp
   */
  QUERY_MESSAGE_RECEIPTION,

  /**
   * measured on master<br>
   * timestamp;queryId;queryString
   */
  QUERY_COORDINATOR_START,

  /**
   * measured on master<br>
   * timestamp;queryId;implementationType;queryTreeType
   */
  QUERY_COORDINATOR_PARSE_START,

  /**
   * measured on master<br>
   * timestamp;queryId
   */
  QUERY_COORDINATOR_PARSE_END,

  /**
   * measured on master<br>
   * queryId;(nodeID_last2byte;OperationString)+
   */
  QUERY_COORDINATOR_QET_NODES,

  /**
   * measured on master<br>
   * timestamp;queryId
   */
  QUERY_COORDINATOR_SEND_QUERY_TO_SLAVE,

  /**
   * measured on master<br>
   * timestamp;queryId
   */
  QUERY_COORDINATOR_SEND_QUERY_START,

  /**
   * measured on master<br>
   * timestamp;queryId;firstResultNumber;lastResultNumber
   */
  QUERY_COORDINATOR_SEND_QUERY_RESULTS_TO_CLIENT,

  /**
   * measured on master<br>
   * timestamp;queryId
   */
  QUERY_SLAVE_QUERY_CREATION_START,

  /**
   * measured on master<br>
   * timestamp;queryId
   */
  QUERY_SLAVE_QUERY_CREATION_END,

  /**
   * measured on master<br>
   * timestamp;queryId
   */
  QUERY_SLAVE_QUERY_EXECUTION_START,

  /**
   * measured on master<br>
   * timestamp;queryId
   */
  QUERY_SLAVE_QUERY_EXECUTION_ABORT,

  /**
   * measured on master<br>
   * timestamp;queryId
   */
  QUERY_COORDINATOR_END;

}
