package playground;

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashSet;
import java.util.Set;

/**
 * Visualizes a graph chunk using graphviz.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class GraphCoverVisualizer {

  private static final String[] COLORS = { "gray", "blue", "greenyellow", "aliceblue",
          "antiquewhite", "aqua", "aquamarine", "azure", "beige", "bisque", "black",
          "blanchedalmond", "blueviolet", "brown", "burlywood", "cadetblue", "chartreuse",
          "chocolate", "coral", "cornflowerblue", "cornsilk", "crimson", "cyan", "darkblue",
          "darkcyan", "darkgoldenrod", "darkgray", "darkgreen", "darkgrey", "darkkhaki",
          "darkmagenta", "darkolivegreen", "darkorange", "darkorchid", "darkred", "darksalmon",
          "darkseagreen", "darkslateblue", "darkslategray", "darkslategrey", "darkturquoise",
          "darkviolet", "deeppink", "deepskyblue", "dimgray", "dimgrey", "dodgerblue", "firebrick",
          "floralwhite", "forestgreen", "fuchsia", "gainsboro", "ghostwhite", "gold", "goldenrod",
          "grey", "green", "honeydew", "hotpink", "indianred", "indigo", "ivory", "khaki",
          "lavender", "lavenderblush", "lawngreen", "lemonchiffon", "lightblue", "lightcoral",
          "lightcyan", "lightgoldenrodyellow", "lightgray", "lightgreen", "lightgrey", "lightpink",
          "lightsalmon", "lightseagreen", "lightskyblue", "lightslategray", "lightslategrey",
          "lightsteelblue", "lightyellow", "lime", "limegreen", "linen", "magenta", "maroon",
          "mediumaquamarine", "mediumblue", "mediumorchid", "mediumpurple", "mediumseagreen",
          "mediumslateblue", "mediumspringgreen", "mediumturquoise", "mediumvioletred",
          "midnightblue", "mintcream", "mistyrose", "moccasin", "navajowhite", "navy", "oldlace",
          "olive", "olivedrab", "orange", "orangered", "orchid", "palegoldenrod", "palegreen",
          "paleturquoise", "palevioletred", "papayawhip", "peachpuff", "peru", "pink", "plum",
          "powderblue", "purple", "red", "rosybrown", "royalblue", "saddlebrown", "salmon",
          "sandybrown", "seagreen", "seashell", "sienna", "silver", "skyblue", "slateblue",
          "slategray", "slategrey", "snow", "springgreen", "steelblue", "tan", "teal", "thistle",
          "tomato", "turquoise", "violet", "wheat", "yellow", "whitesmoke", "yellowgreen" };

  public static void main(String[] args) {
    if (args.length < 3) {
      System.out.println("usage: jave " + GraphCoverVisualizer.class.getName()
              + " <inputFolderForCover> <outputFile> <imageFormat> [-d]");
      return;
    }

    boolean duplicateObjects = false;
    for (String arg : args) {
      if (arg.equals("-d")) {
        duplicateObjects = true;
        break;
      }
    }

    String format = "pdf";
    if (args.length >= 3) {
      format = args[2];
    }
    File output = null;
    if (args.length >= 2) {
      output = new File(args[1]);
    } else {
      output = new File("");
    }
    if (output.exists() && output.isDirectory()) {
      output = new File(output.getAbsolutePath() + File.separator + "graph.dot");
    }
    File[] input = new File(args[0]).listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith(".gz") && pathname.getName().startsWith("chunk");
      }
    });
    new GraphCoverVisualizer().drawCover(input, output, format, duplicateObjects);
  }

  public void drawCover(File[] inputFiles, File outputFile, String format,
          boolean duplicateObjects) {
    Set<String> usedColors = new HashSet<>();
    try (Writer output = new BufferedWriter(new FileWriter(outputFile));) {
      output.write("strict digraph {");
      output.write("\nnode[shape=point]");
      output.write("\nedge[colorscheme=svg]");
      for (File inputFile : inputFiles) {
        try (EncodedFileInputStream input = new EncodedFileInputStream(EncodingFileFormat.EEE,
                inputFile);) {
          drawGraphEdges(input, output, getChunkNumber(inputFile), usedColors, duplicateObjects);
        }
      }
      output.write("\n}");
      output.close();
      executeDot(outputFile, format);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void drawGraphEdges(EncodedFileInputStream input, Writer output, long chunkNumber,
          Set<String> usedColors, boolean duplicateObjects) throws IOException {
    String color = getColor(chunkNumber, usedColors);
    // TODO remove
    System.out.println("c" + chunkNumber + "->" + color);
    for (Statement stmt : input) {
      long subject = stmt.getSubjectAsLong();
      long object = stmt.getObjectAsLong();
      output.write("\nv" + subject + "->v" + object + " [color=" + color + "]");
    }
  }

  private String getColor(long chunkNumber, Set<String> usedColors) {
    int index = ((int) chunkNumber) % GraphCoverVisualizer.COLORS.length;
    if (usedColors.size() == GraphCoverVisualizer.COLORS.length) {
      usedColors.clear();
    }
    while (usedColors.contains(GraphCoverVisualizer.COLORS[index])) {
      index++;
      index = index % GraphCoverVisualizer.COLORS.length;
    }
    String color = GraphCoverVisualizer.COLORS[index];
    usedColors.add(color);
    return color;
  }

  private long getChunkNumber(File inputFile) {
    return Long.parseLong(inputFile.getName().replaceAll("[^0-9]", ""));
  }

  private void executeDot(File outputFile, String format) throws IOException {
    ProcessBuilder pb = new ProcessBuilder("dot", "-T", format, "-o",
            outputFile.getAbsolutePath().replace(".dot", "-dot." + format),
            outputFile.getAbsolutePath());
    pb.inheritIO();
    Process process = pb.start();
    try {
      process.waitFor();
    } catch (InterruptedException e) {
    }
    ProcessBuilder pb2 = new ProcessBuilder("neato", "-T", format, "-o",
            outputFile.getAbsolutePath().replace(".dot", "-naeto." + format),
            outputFile.getAbsolutePath());
    pb2.inheritIO();
    Process process2 = pb2.start();
    try {
      process2.waitFor();
    } catch (InterruptedException e) {
    }
  }

}
