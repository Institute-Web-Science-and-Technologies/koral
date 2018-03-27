/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License
 * along with Koral.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
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
  LOAD_GRAPH_COVER_CREATION_COLORING_VERTEX_DEGREE_TRANSFORMATION_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_COVER_CREATION_COLORING_VERTEX_DEGREE_TRANSFORMATION_END,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_COVER_CREATION_COLORING_COLORING_CREATION_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_COVER_CREATION_COLORING_COLORING_CREATION_END,

  /**
   * measured on master<br>
   * numberOfColors
   */
  LOAD_GRAPH_COVER_CREATION_COLORING_NUMBER_OF_COLORS,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_COVER_CREATION_COLORING_EDGE_ASSIGNMENT_TRANSFORMATION_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_COVER_CREATION_COLORING_EDGE_ASSIGNMENT_TRANSFORMATION_END,

  /**
   * measured on master<br>
   * maximal molecule diameter
   */
  LOAD_GRAPH_COVER_CREATION_MOLECULE_MAXIMAL_MOLECULE_DIAMETER,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_COVER_CREATION_MOLECULE_VERTEX_DEGREE_TRANSFORMATION_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_COVER_CREATION_MOLECULE_VERTEX_DEGREE_TRANSFORMATION_END,

  /**
   * measured on master<br>
   * timestamp, iteration number
   */
  LOAD_GRAPH_COVER_CREATION_MOLECULE_ITERATION_START,

  /**
   * measured on master<br>
   * timestamp, iteration number, number of remaining vertices, number of filled
   * seeds, next frontier size
   */
  LOAD_GRAPH_COVER_CREATION_MOLECULE_ITERATION_END,

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
  LOAD_GRAPH_INITIAL_ENCODING_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_INITIAL_ENCODING_ENCODING_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_INITIAL_ENCODING_ENCODING_END,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_INITIAL_ENCODING_END,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_FINAL_ENCODING_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_FINAL_ENCODING_ENCODING_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_FINAL_ENCODING_ENCODING_END,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_FINAL_ENCODING_END,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_COLLECTING_STATISTICS_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_COLLECTING_STATISTICS_END,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_ADJUSTING_OWNERSHIP_START,

  /**
   * measured on master<br>
   * timestamp
   */
  LOAD_GRAPH_ADJUSTING_OWNERSHIP_END,

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
   * timestamp;queryId;taskId
   */
  QUERY_OPERATION_START,

  /**
   * measured on master<br>
   * queryId;taskId;(receiverSlave;numberOfMappings;)+
   * numberOfVariablesPerMapping
   */
  QUERY_OPERATION_SENT_MAPPINGS_TO_SLAVE,

  /**
   * measured on master<br>
   * queryId;(numberOfMappings;)+
   */
  SLAVE_SENT_MAPPING_BATCHES_TO_SLAVE,

  /**
   * measured on master<br>
   * queryId;taskId;numberOfFinishNotifications
   */
  QUERY_OPERATION_SENT_FINISH_NOTIFICATIONS_TO_OTHER_SLAVES,

  /**
   * measured on master<br>
   * queryId;taskId;numberOfComparisons
   */
  QUERY_OPERATION_JOIN_NUMBER_OF_COMPARISONS,

  /**
   * measured on slavesr<br>
   * timestamp;queryId;taskId
   */
  QUERY_OPERATION_LOCAL_FINISH,

  /**
   * measured on master<br>
   * timestamp;queryId;taskId;idleTime;processingTime;
   * numberOfMappingsEmmittedToMaster;numberOfMappingsEmmittedToSlaves+
   */
  QUERY_OPERATION_FINISH,

  /**
   * measured on master<br>
   * timestamp;queryId;taskId;idleTime;processingTime;
   * numberOfMappingsEmmittedToMaster;numberOfMappingsEmmittedToSlaves+
   */
  QUERY_OPERATION_CLOSED,

  /**
   * measured on master<br>
   * timestamp;queryId
   */
  QUERY_COORDINATOR_END;

}
