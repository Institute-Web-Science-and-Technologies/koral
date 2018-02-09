package de.uni_koblenz.west.koral.master.utils;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Keeps track of the coloring of the edges.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class ColoringManager implements AutoCloseable {

  private final File edge2colorFolder;

  private final RocksDB edge2color;

  private final File colorsFolder;

  /**
   * value % 2 == 1, color was recolored<br>
   * value % 2 == 0, size of color<br>
   * the actual colorId is retrieved by value&gt;&gt;&gt;1
   */
  private final RocksDB colors;

  private long nextColor;

  private long numberOfColors;

  private final long[] internalEdgeInfo;

  public ColoringManager(File internalWorkingDir, int numberOfOpenFiles) {
    edge2colorFolder = new File(internalWorkingDir + File.separator + "edge2colorMap");
    if (!edge2colorFolder.exists()) {
      edge2colorFolder.mkdirs();
    }
    edge2color = createRocksDBMap(edge2colorFolder, numberOfOpenFiles / 2);
    colorsFolder = new File(internalWorkingDir + File.separator + "colorsMap");
    if (!colorsFolder.exists()) {
      colorsFolder.mkdirs();
    }
    colors = createRocksDBMap(colorsFolder, numberOfOpenFiles / 2);
    nextColor = 1;
    internalEdgeInfo = new long[2];
    numberOfColors = 0;
  }

  private RocksDB createRocksDBMap(File mapFolder, int numberOfOpenFiles) {
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setMaxOpenFiles(numberOfOpenFiles);
    options.setWriteBufferSize(64 * 1024 * 1024);
    try {
      return RocksDB.open(options, mapFolder.getAbsolutePath());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param edges[i][1]==0
   *          iff edge is uncolored
   */
  public void fillColorInformation(long[][] edges) {
    fillColorInformation(edges, edges.length);
  }

  /**
   * @param edges
   *          edges[i][0]=edgeId, edges[i][1]=colorId, edges[i][2]=colorSize
   *          (the latter two will be filled)
   * @param lastExclusiveIndex
   *          last index (exclusive) to which the array is filled
   */
  public void fillColorInformation(long[][] edges, int lastExclusiveIndex) {
    for (int i = 0; i < lastExclusiveIndex; i++) {
      fillEdgeColor(edges[i][0], edges[i], 1);
    }
  }

  public void fillEdgeColor(long edge, long[] colorArray, int startIndex) {
    try {
      // TODO use multi get
      byte[] color = edge2color.get(NumberConversion.long2bytes(edge));
      if (color == null) {
        colorArray[startIndex] = 0;
        colorArray[startIndex + 1] = 0;
      } else {
        long[] colorInfo = getColor(color);
        if (colorInfo == null) {
          throw new RuntimeException("The color c" + color + " is unknown.");
        }
        colorArray[startIndex] = colorInfo[0];
        colorArray[startIndex + 1] = colorInfo[1];
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  private long[] getColor(byte[] colorId) {
    try {
      byte[] sizeByte = colors.get(colorId);
      if (sizeByte == null) {
        return null;
      }
      byte[] previousColor = null;
      Set<Long> recoloredColors = new HashSet<>();
      long size = NumberConversion.bytes2long(sizeByte);
      while ((size & 0x01L) == 1) {
        // the color was recolored
        previousColor = colorId;
        colorId = NumberConversion.long2bytes(size >>> 1);
        sizeByte = colors.get(colorId);
        if (sizeByte == null) {
          throw new RuntimeException("The recolored color c" + colorId + " is unknown.");
        }
        size = NumberConversion.bytes2long(sizeByte);
        if ((size & 0x01L) == 1) {
          recoloredColors.add(NumberConversion.bytes2long(previousColor));
        }
      }
      // shorten the search path for recolored edges,
      // i.e., c1->c2->c3->c4 is shortened to c1->c4; c2->c4; c3->c4
      for (Long recoloredColor : recoloredColors) {
        recolorColor(recoloredColor, NumberConversion.bytes2long(colorId));
      }
      long[] color = internalEdgeInfo;
      internalEdgeInfo[0] = NumberConversion.bytes2long(colorId);
      internalEdgeInfo[1] = size >>> 1;
      return color;
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * A new array must be returned
   * 
   * @return color[0]=colorId; color[1]=frequency of color
   */
  public long[] createNewColor() {
    long newColorId = nextColor++;
    setColor(newColorId, 0);
    numberOfColors++;
    return new long[] { newColorId, 0 };
  }

  private void setColor(long newColor, long size) {
    if (newColor == 0) {
      throw new RuntimeException("Attempt to create color c0.");
    }
    try {
      colors.put(NumberConversion.long2bytes(newColor), NumberConversion.long2bytes(size << 1));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public void colorEdge(long edge, long colorId) {
    if (colorId == 0) {
      throw new RuntimeException("Attempt to set color of edge e" + edge + " to c" + colorId + ".");
    }
    // TODO remove
    long[][] e = new long[][] { { edge, 0, 0 } };
    fillColorInformation(e);
    if ((e[0][1] != colorId) && (e[0][1] != 0)) {
      throw new RuntimeException("Edge e" + edge + " should be colored in c" + colorId
              + " but has already color c" + e[0][1] + ".");
    }
    // System.out.println(">>>>color e" + edge + "->c" + colorId);
    long[] color = getColor(NumberConversion.long2bytes(colorId));
    if (color[0] != colorId) {
      throw new RuntimeException("Attempt to color edge e" + edge + " in the color c" + colorId
              + " which was already recolored to c" + color[0]);
    }
    setEdgeColor(edge, color[0]);
    setColor(color[0], color[1] + 1);
    // TODO remove
    // System.out.println(this);
  }

  private void setEdgeColor(long edge, long color) {
    if (color == 0) {
      throw new RuntimeException("Attempt to set color of edge e" + edge + " to c" + color + ".");
    }
    try {
      edge2color.put(NumberConversion.long2bytes(edge), NumberConversion.long2bytes(color));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public void recolor(long oldColor, long oldColorSize, long newColor, long newColorSize) {
    // TODO remove
    // System.out.println(">>>>recolor c" + oldColor + "->c" + newColor);
    if (oldColor == 0) {
      throw new RuntimeException("Attempt to recolor color c" + oldColor + ".");
    }
    if (newColor == 0) {
      throw new RuntimeException("Attempt to recolor color c" + oldColor + " to color c0.");
    }
    // TODO remove
    long ocSize = getColor(NumberConversion.long2bytes(oldColor))[1];
    if (ocSize != oldColorSize) {
      throw new RuntimeException("Color c" + oldColor + " should have a size of " + oldColorSize
              + " but actually has a size of " + ocSize);
    }
    ocSize = getColor(NumberConversion.long2bytes(newColor))[1];
    if (ocSize != newColorSize) {
      throw new RuntimeException("Color c" + newColor + " should have a size of " + newColorSize
              + " but actually has a size of " + ocSize);
    }
    recolorColor(oldColor, newColor);
    setColor(newColor, oldColorSize + newColorSize);
    numberOfColors--;
    // TODO remove
    // System.out.println(colors);
    // System.out.println(this);
  }

  private void recolorColor(long recoloredColor, long colorId) {
    if (recoloredColor == 0) {
      throw new RuntimeException("Attempt to recolor color c" + recoloredColor + ".");
    }
    if (colorId == 0) {
      throw new RuntimeException("Attempt to recolor color c" + recoloredColor + " to color c0.");
    }
    try {
      colors.put(NumberConversion.long2bytes(recoloredColor),
              NumberConversion.long2bytes((colorId << 1) | 0x01L));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public long getNumberOfColors() {
    return numberOfColors;
  }

  /**
   * 
   * @return Iterator over all colors. Each color consists of: color[0]=colorId;
   *         color[1]=frequency of color
   */
  public Iterator<long[]> getIteratorOverAllColors() {
    return new Iterator<long[]>() {

      private final RocksIterator iterator;
      {
        iterator = colors.newIterator();
        iterator.seekToFirst();
      }

      private long[] next = getNext();

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public long[] next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        long[] n = next;
        next = getNext();
        return n;
      }

      private long[] getNext() {
        long color = 0;
        long size = 1;
        while (iterator.isValid()) {
          color = NumberConversion.bytes2long(iterator.key());
          size = NumberConversion.bytes2long(iterator.value());
          iterator.next();
          if ((size & 0x01L) == 0) {
            size = size >>> 1;
            return new long[] { color, size };
          }
        }
        iterator.close();
        return null;
      }
    };
  }

  /**
   * @return Iterator over long[]{edgeId, colorId}
   */
  public Iterator<long[]> getIteratorOverColoredEdges() {
    return new Iterator<long[]>() {

      private final RocksIterator iterator;
      {
        iterator = edge2color.newIterator();
        iterator.seekToFirst();
      }

      private long[] next = getNext();

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public long[] next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        long[] n = next;
        next = getNext();
        return n;
      }

      private long[] getNext() {
        long edge = 0;
        byte[] colorId = null;
        if (iterator.isValid()) {
          edge = NumberConversion.bytes2long(iterator.key());
          colorId = iterator.value();
          iterator.next();
          return new long[] { edge, getColor(colorId)[0] };
        } else {
          iterator.close();
          return null;
        }
      }
    };
  }

  @Override
  public void close() {
    edge2color.close();
    colors.close();
    deleteFolder(edge2colorFolder);
    deleteFolder(colorsFolder);
  }

  private void deleteFolder(File folder) {
    if (!folder.exists()) {
      return;
    }
    if (folder.isDirectory()) {
      Path path = FileSystems.getDefault().getPath(folder.getAbsolutePath());
      try {
        Files.walkFileTree(path, new FileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                  throws IOException {
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                  throws IOException {
            // here you have the files to process
            file.toFile().delete();
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            return FileVisitResult.TERMINATE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
          }
        });
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    folder.delete();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ColorManager:\n");
    sb.append("\tedges: {");
    String delim = "";
    Iterator<long[]> iterator = getIteratorOverColoredEdges();
    while (iterator.hasNext()) {
      long[] edgeColor = iterator.next();
      sb.append(delim).append("(e").append(edgeColor[0]).append(",c").append(edgeColor[1])
              .append(")");
      delim = ", ";
    }
    sb.append("}");
    sb.append("\n");
    delim = "\tcolors: {";
    iterator = getIteratorOverAllColors();
    while (iterator.hasNext()) {
      long[] color = iterator.next();
      sb.append(delim).append("(c").append(color[0]).append(",#").append(color[1]).append(")");
      delim = ", ";
    }
    sb.append("}");
    return sb.toString();
  }

}
