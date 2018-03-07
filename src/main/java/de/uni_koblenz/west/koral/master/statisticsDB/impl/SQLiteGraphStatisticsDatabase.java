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
package de.uni_koblenz.west.koral.master.statisticsDB.impl;

import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatisticsDatabase;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * {@link GraphStatisticsDatabase} realized via SQLite.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class SQLiteGraphStatisticsDatabase implements GraphStatisticsDatabase {

  private final int numberOfChunks;

  private File databaseFile;

  private final Connection dbConnection;

  private boolean isCommitted;

  private PreparedStatement incrementTupleFrequency;

  private PreparedStatement[] updateStatistics;

  private PreparedStatement[] insertStatistics;

  private int numberOfInsertions;

  private final static int MAX_BATCH_SIZE = 1_000_000;

  public SQLiteGraphStatisticsDatabase(String statisticsDir, short numberOfChunks) {
    this.numberOfChunks = numberOfChunks;
    File statisticsDirFile = new File(statisticsDir);
    if (!statisticsDirFile.exists()) {
      statisticsDirFile.mkdirs();
    }
    try {
      Class.forName("org.sqlite.JDBC");
      databaseFile = new File(
              statisticsDirFile.getAbsolutePath() + File.separator + "statistics.db");
      boolean doesDatabaseExist = databaseFile.exists();
      dbConnection = DriverManager.getConnection("jdbc:sqlite:" + databaseFile.getAbsolutePath());
      if (!doesDatabaseExist) {
        Statement statement = dbConnection.createStatement();
        // OS should write it to disk
        statement.executeUpdate("PRAGMA synchronous = OFF");
        statement.executeUpdate("PRAGMA page_size = 4096");
        statement.executeUpdate("PRAGMA cache_size = 2000");
        statement.executeUpdate("PRAGMA journal_mode = MEMORY");
        statement.executeUpdate("PRAGMA temp_store = MEMORY");
        statement.close();

        dbConnection.setAutoCommit(false);
        dbConnection.commit();
        initializeDatabase();
      } else {
        dbConnection.setAutoCommit(false);
        dbConnection.commit();
      }
      isCommitted = true;
      numberOfInsertions = 0;

    } catch (ClassNotFoundException | SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void initializeDatabase() {
    try {
      Statement statement = dbConnection.createStatement();

      // create table that counts the triples per chunk
      StringBuilder query = new StringBuilder();
      query.append("CREATE TABLE TRIPLES_PER_CHUNK ");
      query.append("(CHUNK_ID INTEGER PRIMARY KEY NOT NULL,");
      query.append(" NUMBER_OF_TRIPLES BIGINT);");
      statement.addBatch(query.toString());

      // initialize table
      query = new StringBuilder();
      query.append("INSERT INTO TRIPLES_PER_CHUNK (CHUNK_ID, NUMBER_OF_TRIPLES) VALUES ");
      for (int i = 0; i < numberOfChunks; i++) {
        if (i > 0) {
          query.append(",");
        }
        query.append("(").append(i).append(",").append(0).append(")");
      }
      query.append(";");
      statement.addBatch(query.toString());

      // create table for statistical information
      query = new StringBuilder();
      query.append("CREATE TABLE STATISTICS ");
      query.append("(RESOURCE_ID INTEGER PRIMARY KEY NOT NULL");
      for (int i = 0; i < numberOfChunks; i++) {
        query.append(", CHUNK_").append(i).append("_SUBJECT BIGINT DEFAULT 0");
        query.append(", CHUNK_").append(i).append("_PROPERTY BIGINT DEFAULT 0");
        query.append(", CHUNK_").append(i).append("_OBJECT BIGINT DEFAULT 0");
      }
      query.append(", OCCURENCES BIGINT DEFAULT 0);");
      statement.addBatch(query.toString());

      statement.executeBatch();
      statement.close();
      dbConnection.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void incrementSubjectCount(long subject, int chunk) {
    startTransaction();
    increment(subject, updateStatistics[(3 * chunk) + 0], insertStatistics[(3 * chunk) + 0]);
  }

  @Override
  public void incrementPropertyCount(long property, int chunk) {
    startTransaction();
    increment(property, updateStatistics[(3 * chunk) + 1], insertStatistics[(3 * chunk) + 1]);
  }

  @Override
  public void incrementObjectCount(long object, int chunk) {
    startTransaction();
    increment(object, updateStatistics[(3 * chunk) + 2], insertStatistics[(3 * chunk) + 2]);
  }

  public void incrementRessourceOccurrences(long resource, int chunk) {
    startTransaction();
    increment(resource, updateStatistics[updateStatistics.length - 1],
            insertStatistics[insertStatistics.length - 1]);
  }

  @Override
  public void incrementNumberOfTriplesPerChunk(int chunk) {
    startTransaction();
    increment(chunk, incrementTupleFrequency, null);
  }

  private void increment(long rowIndex, PreparedStatement updateStatement,
          PreparedStatement insertStatement) {
    try {
      updateStatement.setLong(1, rowIndex);
      int result = updateStatement.executeUpdate();
      numberOfInsertions++;
      if (result == 0) {
        insertStatement.setLong(1, rowIndex);
        insertStatement.executeUpdate();
        numberOfInsertions++;
      }
      if (numberOfInsertions > SQLiteGraphStatisticsDatabase.MAX_BATCH_SIZE) {
        dbConnection.commit();
        numberOfInsertions = 0;
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void startTransaction() {
    isCommitted = false;
    if (incrementTupleFrequency == null) {
      try {
        incrementTupleFrequency = dbConnection.prepareStatement(
                "UPDATE TRIPLES_PER_CHUNK SET NUMBER_OF_TRIPLES = NUMBER_OF_TRIPLES + 1 WHERE CHUNK_ID == ?;");

        updateStatistics = new PreparedStatement[(3 * numberOfChunks) + 1];
        insertStatistics = new PreparedStatement[(3 * numberOfChunks) + 1];
        for (int i = 0; i < numberOfChunks; i++) {
          updateStatistics[(3 * i) + 0] = dbConnection
                  .prepareStatement("UPDATE STATISTICS SET CHUNK_" + i + "_SUBJECT = CHUNK_" + i
                          + "_SUBJECT + 1 WHERE RESOURCE_ID == ?;");
          insertStatistics[(3 * i) + 0] = dbConnection.prepareStatement(
                  "INSERT INTO STATISTICS (RESOURCE_ID, CHUNK_" + i + "_SUBJECT) VALUES (?, 1);");
          updateStatistics[(3 * i) + 1] = dbConnection
                  .prepareStatement("UPDATE STATISTICS SET CHUNK_" + i + "_PROPERTY = CHUNK_" + i
                          + "_PROPERTY + 1 WHERE RESOURCE_ID == ?;");
          insertStatistics[(3 * i) + 1] = dbConnection.prepareStatement(
                  "INSERT INTO STATISTICS (RESOURCE_ID, CHUNK_" + i + "_PROPERTY) VALUES (?, 1);");
          updateStatistics[(3 * i) + 2] = dbConnection
                  .prepareStatement("UPDATE STATISTICS SET CHUNK_" + i + "_OBJECT = CHUNK_" + i
                          + "_OBJECT + 1 WHERE RESOURCE_ID == ?;");
          insertStatistics[(3 * i) + 2] = dbConnection.prepareStatement(
                  "INSERT INTO STATISTICS (RESOURCE_ID, CHUNK_" + i + "_OBJECT) VALUES (?, 1);");
        }
        updateStatistics[updateStatistics.length - 1] = dbConnection.prepareStatement(
                "UPDATE STATISTICS SET OCCURENCES = OCCURENCES + 1 WHERE RESOURCE_ID == ?;");
        insertStatistics[insertStatistics.length - 1] = dbConnection.prepareStatement(
                "INSERT INTO STATISTICS (RESOURCE_ID, OCCURENCES) VALUES (?, 1);");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public long[] getChunkSizes() {
    long[] chunkSizes = new long[numberOfChunks];
    String query = "SELECT * FROM TRIPLES_PER_CHUNK;";
    try {
      if (!isCommitted) {
        endTransaction();
      }
      Statement statement = dbConnection.createStatement();
      ResultSet result = statement.executeQuery(query);
      int nextChunk = 0;
      while (result.next()) {
        chunkSizes[nextChunk++] = result.getLong("NUMBER_OF_TRIPLES");
      }
      statement.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return chunkSizes;
  }

  @Override
  public long[] getStatisticsForResource(long id) {
    if (id == 0) {
      return null;
    }
    long[] statistics = new long[(numberOfChunks * 3) + 1];
    String query = "SELECT * FROM STATISTICS WHERE RESOURCE_ID == " + id + ";";
    try {
      if (!isCommitted) {
        endTransaction();
      }
      Statement statement = dbConnection.createStatement();
      ResultSet result = statement.executeQuery(query);
      result.next();
      for (int i = 0; i < numberOfChunks; i++) {
        statistics[(0 * numberOfChunks) + i] = result.getLong("CHUNK_" + i + "_SUBJECT");
        statistics[(1 * numberOfChunks) + i] = result.getLong("CHUNK_" + i + "_PROPERTY");
        statistics[(2 * numberOfChunks) + i] = result.getLong("CHUNK_" + i + "_OBJECT");
      }
      statistics[statistics.length - 1] = result.getLong("OCCURENCES");
      statement.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return statistics;
  }

  private void endTransaction() throws SQLException {
    if (incrementTupleFrequency != null) {
      incrementTupleFrequency.close();
    }
    if (updateStatistics != null) {
      for (PreparedStatement s : updateStatistics) {
        if (s != null) {
          s.close();
        }
      }
    }
    if (insertStatistics != null) {
      for (PreparedStatement s : insertStatistics) {
        if (s != null) {
          s.close();
        }
      }
    }
    dbConnection.commit();
    isCommitted = true;
  }

  @Override
  public void clear() {
    try {
      Statement statement = dbConnection.createStatement();
      statement.executeUpdate("DROP TABLE TRIPLES_PER_CHUNK;");
      statement.executeUpdate("DROP TABLE STATISTICS;");
      statement.close();
      endTransaction();
      initializeDatabase();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      endTransaction();
      dbConnection.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

}
