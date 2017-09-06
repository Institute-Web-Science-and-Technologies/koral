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
package de.uni_koblenz.west.koral.master.utils;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Stores all adjacency lists in one file. Each entry in the list is a tuple
 * consisting of (i) the id of the adjacent vertex (ii) the offset of the
 * previous entry in the list
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class SingleFileAdjacencyMatrix extends AdjacencyMatrix implements AdjacencyList {

  /**
   * values are two long values: (i) offset of the head of the corresponding
   * adjacency list (ii) the size of the adjacency list
   */
  private RocksDB vertex2lastElementOffset;

  private File vertex2lastElementOffsetFolder;

  private File adjacencyMatrixFile;

  private long fileSize;

  private DataOutputStream adjacencyMatrix;

  public SingleFileAdjacencyMatrix(File workingDir) {
    this(workingDir,
            new File(workingDir.getAbsolutePath() + File.separator + "vertex2lastElementOffset"),
            new File(workingDir.getAbsolutePath() + File.separator + "adjacencyMatrix"));
  }

  private SingleFileAdjacencyMatrix(File workingDir, File rocksDBFolder, File adjacencyMatrixFile) {
    super(workingDir);
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setMaxOpenFiles(50);
    options.setWriteBufferSize(64 * 1024 * 1024);
    vertex2lastElementOffsetFolder = rocksDBFolder;
    if (!vertex2lastElementOffsetFolder.exists()) {
      vertex2lastElementOffsetFolder.mkdirs();
    }
    try {
      vertex2lastElementOffset = RocksDB.open(options,
              vertex2lastElementOffsetFolder.getAbsolutePath() + File.separator
                      + "vertex2lastElementOffset");
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    this.adjacencyMatrixFile = adjacencyMatrixFile;
    fileSize = 0;
    options.close(); // todo closing options
  }

  @Override
  protected AdjacencyList getInternalAdjacencyList(long vertex) {
    currentVertex = vertex;
    return this;
  }

  @Override
  public void close() {
    super.close();
    internalClose();
  }

  private void internalClose() {
    vertex2lastElementOffset.close();
    if (vertex2lastElementOffsetFolder.exists()) {
      for (File file : vertex2lastElementOffsetFolder.listFiles()) {
        file.delete();
      }
      vertex2lastElementOffsetFolder.delete();
    }
    try {
      if (adjacencyMatrix != null) {
        adjacencyMatrix.close();
      }
      adjacencyMatrixFile.delete();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected long removeDuplicatesFromAdjacencyLists(long numberOfVertices) {
    long numberOfEdges = 0;
    @SuppressWarnings("resource")
    SingleFileAdjacencyMatrix newMatrix = new SingleFileAdjacencyMatrix(workingDir,
            new File(
                    workingDir.getAbsolutePath() + File.separator + "vertex2lastElementOffset_new"),
            new File(workingDir.getAbsolutePath() + File.separator + "adjacencyMatrix_new"));

    DBMaker<?> dbmaker = MapDBStorageOptions.MEMORY_MAPPED_FILE
            .getDBMaker(workingDir.getAbsolutePath() + File.separator + "adjacencySets")
            .transactionDisable().closeOnJvmShutdown().asyncWriteEnable();
    dbmaker = MapDBCacheOptions.HASH_TABLE.setCaching(dbmaker);
    DB database = dbmaker.make();
    Set<Long> vertexSet = database.createHashSet("adjacencys").makeOrGet();
    for (long vertex = 1; vertex <= numberOfVertices; vertex++) {
      SingleFileAdjacencyMatrixLongIterator iterator = (SingleFileAdjacencyMatrixLongIterator) getInternalAdjacencyList(
              vertex).iterator();
      Set<Long> adjacencySet = iterator.size() < (1024 * 1024) ? new HashSet<>() : vertexSet;
      AdjacencyList list = newMatrix.getInternalAdjacencyList(vertex);
      while (iterator.hasNext()) {
        long adjacency = iterator.next();
        boolean isNew = adjacencySet.add(adjacency);
        if (isNew) {
          numberOfEdges++;
          list.append(adjacency);
        }
      }
      iterator.close();
      adjacencySet.clear();
    }
    internalClose();
    adjacencyMatrix = newMatrix.adjacencyMatrix;
    adjacencyMatrixFile = newMatrix.adjacencyMatrixFile;
    fileSize = newMatrix.fileSize;
    vertex2lastElementOffset = newMatrix.vertex2lastElementOffset;
    vertex2lastElementOffsetFolder = newMatrix.vertex2lastElementOffsetFolder;
    return numberOfEdges;
  }

  /*
   * The following class members are used to implement the adjacency list.
   */

  static final long TAIL_OFFSET_OF_LIST = -1;

  private long currentVertex;

  @Override
  public void append(long adjacency) {
    try {
      if (adjacencyMatrix == null) {
        adjacencyMatrix = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(adjacencyMatrixFile, true)));
      }

      long previous = SingleFileAdjacencyMatrix.TAIL_OFFSET_OF_LIST;
      long length = 0;
      byte[] vertexArray = NumberConversion.long2bytes(currentVertex);
      byte[] offsetLengthArray = vertex2lastElementOffset.get(vertexArray);
      if (offsetLengthArray != null) {
        previous = NumberConversion.bytes2long(offsetLengthArray, 0);
        length = NumberConversion.bytes2long(offsetLengthArray, Long.BYTES);
      } else {
        offsetLengthArray = new byte[Long.BYTES * 2];
      }
      long offset = fileSize;

      adjacencyMatrix.writeLong(adjacency);
      fileSize += Long.BYTES;
      adjacencyMatrix.writeLong(previous);
      fileSize += Long.BYTES;
      length++;

      NumberConversion.long2bytes(offset, offsetLengthArray, 0);
      NumberConversion.long2bytes(length, offsetLengthArray, Long.BYTES);
      vertex2lastElementOffset.put(vertexArray, offsetLengthArray);
    } catch (IOException | RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public LongIterator iterator() {
    try {
      if (adjacencyMatrix != null) {
        adjacencyMatrix.close();
        adjacencyMatrix = null;
      }
      byte[] vertexArray = NumberConversion.long2bytes(currentVertex);
      byte[] offsetLengthArray = vertex2lastElementOffset.get(vertexArray);
      if (offsetLengthArray != null) {
        return new SingleFileAdjacencyMatrixLongIterator(adjacencyMatrixFile,
                NumberConversion.bytes2long(offsetLengthArray, 0),
                NumberConversion.bytes2long(offsetLengthArray, Long.BYTES));
      } else {
        return new SingleFileAdjacencyMatrixLongIterator(adjacencyMatrixFile,
                SingleFileAdjacencyMatrix.TAIL_OFFSET_OF_LIST, 0);
      }
    } catch (IOException | RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

}
