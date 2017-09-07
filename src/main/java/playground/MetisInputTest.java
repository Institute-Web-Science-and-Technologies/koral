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
package playground;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;

/**
 * Tests the generated input file for METIS
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MetisInputTest {

  public static void main(String[] args) throws FileNotFoundException {
    long numberOfVertices = -1;
    long numberOfEdges = -1;
    Map<Long, Set<Long>> adjacencyLists = new HashMap<>();
    try (Scanner scanner = new Scanner(new File("/tmp/metisInput"));) {
      String[] line = scanner.nextLine().split("\\s+");
      numberOfVertices = Long.parseLong(line[0]);
      numberOfEdges = Long.parseLong(line[1]);
      long vertex = 1;
      while (scanner.hasNextLine()) {
        line = scanner.nextLine().split("\\s+");
        Set<Long> adjacencyList = new HashSet<>();
        for (String adjacency : line) {
          adjacencyList.add(Long.parseLong(adjacency));
        }
        adjacencyLists.put(vertex, adjacencyList);
        vertex++;
      }
      if ((vertex - 1) != numberOfVertices) {
        throw new RuntimeException(
                "Expected " + numberOfVertices + " vertices but only found " + (vertex - 1));
      }
    }
    long foundEdges = 0;
    for (Entry<Long, Set<Long>> entry : adjacencyLists.entrySet()) {
      for (Long adjacency : entry.getValue()) {
        Set<Long> otherAdjacencyList = adjacencyLists.get(adjacency);
        if (otherAdjacencyList == null) {
          throw new RuntimeException("The vertex " + adjacency + " could not be found!");
        }
        if (entry.getKey().equals(adjacency)) {
          throw new RuntimeException(
                  "There is a loop " + entry.getKey() + "->" + adjacency + " in the graph!");
        }
        boolean wasContained = otherAdjacencyList.remove(entry.getKey());
        if (!wasContained) {
          throw new RuntimeException(
                  "The edge " + entry.getKey() + "->" + adjacency + " is not bidirectional!");
        }
        foundEdges++;
      }
      entry.setValue(new HashSet<>());
    }
    for (Entry<Long, Set<Long>> entry : adjacencyLists.entrySet()) {
      if (!entry.getValue().isEmpty()) {
        throw new RuntimeException(
                "The vertex " + entry.getKey() + " has remaining edges " + entry.getValue());
      }
    }
    if (foundEdges != numberOfEdges) {
      throw new RuntimeException("Found " + foundEdges + " but expected " + numberOfEdges);
    }
  }

}
