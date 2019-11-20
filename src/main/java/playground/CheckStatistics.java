package playground;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatisticsDatabase;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.MultiFileGraphStatisticsDatabase;

/**
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class CheckStatistics {

  public static void main(String[] args) {
    if (args.length != 2) {
      System.out.println("Usage: java " + StatisticsDBTest.class.getName()
          + " <encodedChunksDir> <statisticsDir>");
      return;
    }
    File encodedChunksDir = new File(args[0]);
    if (!encodedChunksDir.exists() || !encodedChunksDir.isDirectory()) {
      System.err.println("Directory does not exist: " + encodedChunksDir);
    }
    File[] encodedChunks = encodedChunksDir.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        return pathname.isFile() && pathname.getName().endsWith(".adj.gz");
      }
    });
    Arrays.sort(encodedChunks);
    File statisticsDir = new File(args[1]);
    if (!statisticsDir.exists() || !statisticsDir.isDirectory()) {
      System.err.println("Directory does not exist: " + statisticsDir);
    }
    try (GraphStatisticsDatabase statisticsDB = new MultiFileGraphStatisticsDatabase(
        statisticsDir.getAbsolutePath(), (short) encodedChunks.length, null);
        GraphStatistics statistics = new GraphStatistics(statisticsDB,
            (short) encodedChunks.length, null);) {
      for (File encodedChunk : encodedChunks) {
        System.out.println("Checking " + encodedChunk.getName());
        try (EncodedFileInputStream input = new EncodedFileInputStream(EncodingFileFormat.EEE,
            encodedChunk);) {
          long tripel = 0;
          for (Statement stmt : input) {
            tripel += 1;
            long subject = stmt.getSubjectAsLong();
            long plainSubject = subject & 0x00_00_ff_ff_ff_ff_ff_ffL;
            long newSubject = statistics.getIDWithOwner(plainSubject);
            if (newSubject != subject) {
              throw new RuntimeException("The subject " + plainSubject + " in "
                  + encodedChunk.getName() + " was originally owned by " + (subject >>> 48)
                  + " but is now owned by " + (newSubject >>> 48) + ".");
            }

            long property = stmt.getPropertyAsLong();
            long plainProperty = property & 0x00_00_ff_ff_ff_ff_ff_ffL;
            long newProperty = statistics.getIDWithOwner(plainProperty);
            if (newProperty != property) {
              throw new RuntimeException("The property " + plainProperty + " in "
                  + encodedChunk.getName() + " was originally owned by " + (property >>> 48)
                  + " but is now owned by " + (newProperty >>> 48) + ".");
            }

            long object = stmt.getObjectAsLong();
            long plainObject = object & 0x00_00_ff_ff_ff_ff_ff_ffL;
            long newObject = statistics.getIDWithOwner(plainObject);
            if (newObject != object) {
              throw new RuntimeException("The object " + plainObject + " in "
                  + encodedChunk.getName() + " was originally owned by " + (object >>> 48)
                  + " but is now owned by " + (newObject >>> 48) + ".");
            }
            if ((tripel % 1_000_000) == 0) {
              System.out.println("\tchecked " + (tripel / 1_000_000) + "M triples");
            }
          }
        } catch (IOException e) {
          e.printStackTrace();
          continue;
        }
      }
    }
  }

}
