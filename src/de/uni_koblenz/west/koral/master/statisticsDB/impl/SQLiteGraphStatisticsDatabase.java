package de.uni_koblenz.west.koral.master.statisticsDB.impl;

import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatisticsDatabase;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
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

  private final Connection dbConnection;

  private File databaseFile;

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
        initializeDatabase();
      }
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
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void incrementSubjectCount(long subject, int chunk) {
    String tableName = "STATISTICS";
    String updateColumnName = "CHUNK_" + chunk + "_SUBJECT";
    String keyColumnName = "RESOURCE_ID";
    increment(subject, tableName, updateColumnName, keyColumnName);
  }

  @Override
  public void incrementPropertyCount(long property, int chunk) {
    String tableName = "STATISTICS";
    String updateColumnName = "CHUNK_" + chunk + "_PROPERTY";
    String keyColumnName = "RESOURCE_ID";
    increment(property, tableName, updateColumnName, keyColumnName);
  }

  @Override
  public void incrementObjectCount(long object, int chunk) {
    String tableName = "STATISTICS";
    String updateColumnName = "CHUNK_" + chunk + "_OBJECT";
    String keyColumnName = "RESOURCE_ID";
    increment(object, tableName, updateColumnName, keyColumnName);
  }

  @Override
  public void incrementRessourceOccurrences(long resource, int chunk) {
    String tableName = "STATISTICS";
    String updateColumnName = "OCCURENCES";
    String keyColumnName = "RESOURCE_ID";
    increment(resource, tableName, updateColumnName, keyColumnName);
  }

  @Override
  public void incrementNumberOfTriplesPerChunk(int chunk) {
    String tableName = "TRIPLES_PER_CHUNK";
    String updateColumnName = "NUMBER_OF_TRIPLES";
    String keyColumnName = "CHUNK_ID";
    increment(chunk, tableName, updateColumnName, keyColumnName);
  }

  private void increment(long rowKey, String tableName, String updateColumnName,
          String keyColumnName) {

    String updateQuery = "UPDATE " + tableName + " SET " + updateColumnName + " = "
            + updateColumnName + " + 1 WHERE " + keyColumnName + " == " + rowKey + ";";

    String insertQuery = "INSERT INTO " + tableName + " (" + keyColumnName + ", " + updateColumnName
            + ")  VALUES (" + rowKey + ", 1);";

    try {
      Statement statement = dbConnection.createStatement();
      int result = statement.executeUpdate(updateQuery.toString());
      System.out.println(result);
      if (result == 0) {
        statement.executeUpdate(insertQuery.toString());
      }
      statement.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long[] getChunkSizes() {
    long[] chunkSizes = new long[numberOfChunks];
    String query = "SELECT * FROM TRIPLES_PER_CHUNK;";
    try {
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
    long[] statistics = new long[(numberOfChunks * 3) + 1];
    String query = "SELECT * FROM STATISTICS WHERE RESOURCE_ID == " + id + ";";
    try {
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

  @Override
  public void clear() {
    try {
      Statement statement = dbConnection.createStatement();
      statement.executeUpdate("DROP TABLE TRIPLES_PER_CHUNK;");
      statement.executeUpdate("DROP TABLE STATISTICS;");
      statement.close();
      initializeDatabase();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      dbConnection.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

}
