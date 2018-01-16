package de.uni_koblenz.west.koral.master.utils;

import de.uni_koblenz.west.koral.common.io.EncodedLongFileOutputStream;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Stores the graph by:<br>
 * VertexId, number of outgoing edges, number of ingoing edges, list of outgoing
 * edges, list of ingoing edges
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class VertexIncidencentEdgesListFileCreator implements AutoCloseable {

  private static final int INITIAL_ARRAY_SIZE = 1;

  private final File storageFile;

  private Map<Long, Integer> vertexId2Index;

  /**
   * stores for vertex i:
   * vertexDegrees[i]={vertexId,outdegree,indegree,incidentEdgesIndex}
   */
  private List<long[]> vertexDegrees;

  /**
   * stores for vertex i: incidentEdges[vertexDegrees[i][3]][0] = list of
   * outgoing edges, incidentEdges[vertexDegrees[i][3]][1] list of ingoing edges
   */
  private List<long[][]> incidentEdges;

  public VertexIncidencentEdgesListFileCreator(File storageFile) {
    this.storageFile = storageFile;
    vertexId2Index = new HashMap<>();
    vertexDegrees = new ArrayList<>();
    incidentEdges = new ArrayList<>();
  }

  public int getSize() {
    return vertexId2Index == null ? 0 : vertexId2Index.size();
  }

  public boolean contains(long vertexId) {
    return vertexId2Index.containsKey(vertexId);
  }

  public void add(long vertexId, long edgeId, boolean isOutgoingEdge) {
    Integer index = vertexId2Index.get(vertexId);
    long[] vertexDegree = null;
    long[][] incidentEdge = null;
    if (index == null) {
      index = Integer.valueOf(vertexId2Index.size());
      vertexId2Index.put(vertexId, index);
      vertexDegree = new long[] { vertexId, 0, 0, index };
      vertexDegrees.add(vertexDegree);
      incidentEdge = new long[2][VertexIncidencentEdgesListFileCreator.INITIAL_ARRAY_SIZE];
      incidentEdges.add(incidentEdge);
    } else {
      vertexDegree = vertexDegrees.get(index);
      incidentEdge = incidentEdges.get((int) vertexDegree[3]);
    }
    incidentEdge[isOutgoingEdge ? 0 : 1] = addEdge(edgeId, incidentEdge[isOutgoingEdge ? 0 : 1],
            (int) vertexDegree[isOutgoingEdge ? 1 : 2]);
    vertexDegree[isOutgoingEdge ? 1 : 2]++;
  }

  private long[] addEdge(long edgeId, long[] edgeList, int insertionIndex) {
    if (insertionIndex >= edgeList.length) {
      int newSize = edgeList.length;
      while (newSize <= insertionIndex) {
        if (newSize < 100) {
          newSize *= 2;
        } else {
          newSize += 100;
        }
      }
      edgeList = Arrays.copyOf(edgeList, newSize);
    }
    edgeList[insertionIndex] = edgeId;
    return edgeList;
  }

  public void flush() {
    if (vertexId2Index == null) {
      // this file was already flushed
      return;
    }
    vertexId2Index = null;
    Collections.sort(vertexDegrees, VertexIncidentEdgesVertexIdComparator.getComparator(true));
    try (EncodedLongFileOutputStream output = new EncodedLongFileOutputStream(storageFile);) {
      for (long[] vertexDegree : vertexDegrees) {
        output.writeLong(vertexDegree[0]);
        output.writeLong(vertexDegree[1]);
        output.writeLong(vertexDegree[2]);
        for (long outEdge : incidentEdges.get((int) vertexDegree[3])[0]) {
          if (outEdge == 0) {
            break;
          }
          output.writeLong(outEdge);
        }
        for (long inEdge : incidentEdges.get((int) vertexDegree[3])[1]) {
          if (inEdge == 0) {
            break;
          }
          output.writeLong(inEdge);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    vertexDegrees = null;
    incidentEdges = null;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(storageFile.getAbsolutePath()).append("\n");
    String bigDelim = "";
    for (long[] vertex : vertexDegrees) {
      sb.append(bigDelim).append("vertex:").append(vertex[0]).append("\n");
      sb.append("\toutdegree:").append(vertex[1]).append(" {");
      String delim = "";
      for (long edge : incidentEdges.get((int) vertex[3])[0]) {
        if (edge == 0) {
          break;
        }
        sb.append(delim).append(edge);
        delim = ", ";
      }
      sb.append("}\n");
      sb.append("\tindegree:").append(vertex[2]).append(" {");
      delim = "";
      for (long edge : incidentEdges.get((int) vertex[3])[1]) {
        if (edge == 0) {
          break;
        }
        sb.append(delim).append(edge);
        delim = ", ";
      }
      sb.append("}");
      bigDelim = "\n";
    }
    return sb.toString();
  }

  @Override
  public void close() {
    flush();
  }

  public File getFile() {
    flush();
    close();
    return storageFile;
  }

}
