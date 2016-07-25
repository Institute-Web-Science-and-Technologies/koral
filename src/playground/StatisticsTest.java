package playground;

import org.apache.jena.ext.com.google.common.io.Files;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.utils.GraphFileFilter;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatisticsDatabase;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.MapDBGraphStatisticsDatabase;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.SingleFileGraphStatisticsDatabase;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;

/**
 * Compares different statistics database implementations
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class StatisticsTest {

  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      System.out.println("Missing input file");
      return;
    }
    File workingDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "koralTest");
    if (!workingDir.exists()) {
      workingDir.mkdir();
    }

    File inputFile = new File(args[0]);
    Configuration conf = new Configuration();
    conf.setDictionaryDir(workingDir.getAbsolutePath() + File.separator + "dictionary");
    conf.setStatisticsDir(workingDir.getAbsolutePath() + File.separator + "statistics");

    DictionaryEncoder encoder = new DictionaryEncoder(conf, null, null);
    File encodedInput = encoder.encodeOriginalGraphFiles(inputFile.isDirectory()
            ? inputFile.listFiles(new GraphFileFilter()) : new File[] { inputFile }, workingDir,
            EncodingFileFormat.EEE, 4);
    File encodedInput2 = new File(encodedInput.getAbsolutePath() + ".copy1");
    File encodedInput3 = new File(encodedInput.getAbsolutePath() + ".copy2");
    Files.copy(encodedInput, encodedInput2);
    Files.copy(encodedInput, encodedInput3);

    System.out.println("measuring mapDB:");
    GraphStatisticsDatabase database = new MapDBGraphStatisticsDatabase(
            conf.getStatisticsStorageType(), conf.getStatisticsDataStructure(),
            conf.getStatisticsDir(), conf.useTransactionsForStatistics(),
            conf.areStatisticsAsynchronouslyWritten(), conf.getStatisticsCacheType(), (short) 4);
    StatisticsTest.collectStatistics(database, encodedInput, workingDir);

    if (!workingDir.exists()) {
      workingDir.mkdir();
    }

    System.out.println("\nmeasuring random access file:");
    database = new SingleFileGraphStatisticsDatabase(conf.getStatisticsDir(), (short) 4);
    StatisticsTest.collectStatistics(database, encodedInput2, workingDir);

    if (!workingDir.exists()) {
      workingDir.mkdir();
    }

    System.out.println("\nmeasuring SQLite:");
    database = new SingleFileGraphStatisticsDatabase(conf.getStatisticsDir(), (short) 4);
    StatisticsTest.collectStatistics(database, encodedInput3, workingDir);

    encoder.close();

    Playground.delete(workingDir);
  }

  private static void collectStatistics(GraphStatisticsDatabase database, File encodedInput,
          File workingDir) {
    DateFormat format = DateFormat.getDateTimeInstance();

    System.out.println("\tcollecting statistics");
    long start = System.currentTimeMillis();
    System.out.println("\t\tstart: " + format.format(new Date(start)));
    GraphStatistics statistics = new GraphStatistics(database, (short) 4, null);
    statistics.collectStatistics(new File[] { encodedInput });
    long end = System.currentTimeMillis();
    System.out.println("\t\tend: " + format.format(new Date(end)));
    long duration = end - start;
    System.out.println("\t\trequired time: " + duration + " msec = "
            + String.format("%d:%02d:%02d.%03d", duration / 3_600_000, (duration / 60_000) % 60,
                    ((duration / 1000) % 60), duration % 1000));

    System.out.println("\tadjusting ownership");
    start = System.currentTimeMillis();
    System.out.println("\t\tstart: " + format.format(new Date(start)));
    statistics.adjustOwnership(new File[] { encodedInput }, workingDir);
    end = System.currentTimeMillis();
    System.out.println("\t\tend: " + format.format(new Date(end)));
    duration = end - start;
    System.out.println("\t\trequired time: " + duration + " msec = "
            + String.format("%d:%02d:%02d.%03d", duration / 3_600_000, (duration / 60_000) % 60,
                    ((duration / 1000) % 60), duration % 1000));

    statistics.close();
  }

}
