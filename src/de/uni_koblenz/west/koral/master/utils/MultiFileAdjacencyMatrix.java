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

import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Stores one file for each adjacency list.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MultiFileAdjacencyMatrix extends AdjacencyMatrix {

  private final Queue<Long> lruCache;

  private final Map<Long, FileLongSet> vertex2adjacentList;

  public MultiFileAdjacencyMatrix(File workingDir) {
    super(workingDir);
    lruCache = new LinkedList<>();
    vertex2adjacentList = new HashMap<>();
  }

  @Override
  protected FileLongSet getInternalAdjacencyList(long vertex) {
    FileLongSet set = vertex2adjacentList.get(vertex);
    if (set == null) {
      set = new FileLongSet(new File(workingDir.getAbsolutePath() + File.separator + vertex));
      if (lruCache.size() == 100) {
        long oldestVertex = lruCache.poll();
        FileLongSet toBeRemoved = vertex2adjacentList.remove(oldestVertex);
        toBeRemoved.close();
      }
      vertex2adjacentList.put(vertex, set);
    } else {
      lruCache.remove(vertex);
    }
    lruCache.offer(vertex);
    return set;
  }

  @Override
  protected long removeDuplicatesFromAdjacencyLists(long numberOfVertices) {
    long numberOfEdges = 0;
    DBMaker<?> dbmaker = MapDBStorageOptions.MEMORY_MAPPED_FILE
            .getDBMaker(workingDir.getAbsolutePath() + File.separator + "adjacencySets")
            .transactionDisable().closeOnJvmShutdown().asyncWriteEnable();
    dbmaker = MapDBCacheOptions.HASH_TABLE.setCaching(dbmaker);
    DB database = dbmaker.make();
    Set<Long> vertexSet = database.createHashSet("adjacencys").makeOrGet();

    for (long vertex = 1; vertex <= numberOfVertices; vertex++) {
      FileLongSet adjacencyList = getInternalAdjacencyList(vertex);
      adjacencyList.close();
      File newFile = new File(adjacencyList.getFile().getAbsolutePath() + "_copy");
      adjacencyList.getFile().renameTo(newFile);
      adjacencyList = new FileLongSet(newFile);
      FileLongSet newAdjacencyList = getInternalAdjacencyList(vertex);
      boolean isSmall = newFile.length() < (1024 * 1024);
      // duplicates can be checked in memory
      Set<Long> alreadySeen = isSmall ? new HashSet<>() : vertexSet;
      LongIterator iterator = adjacencyList.iterator();
      while (iterator.hasNext()) {
        long next = iterator.next();
        boolean isNew = alreadySeen.add(next);
        if (isNew) {
          newAdjacencyList.append(next);
          numberOfEdges++;
        }
      }
      iterator.close();
      if (!isSmall) {
        vertexSet.clear();
      }
      newAdjacencyList.close();
      adjacencyList.close();
      newFile.delete();
    }
    return numberOfEdges;
  }

  @Override
  public void close() {
    super.close();
    for (FileLongSet set : vertex2adjacentList.values()) {
      set.close();
    }
  }

}
