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
    File encodedInput2 = new File(encodedInput.getAbsolutePath() + ".copy");
    Files.copy(encodedInput, encodedInput2);

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

    encoder.close();

    Playground.delete(workingDir);
  }

  private static void collectStatistics(GraphStatisticsDatabase database, File encodedInput,
          File workingDir) {
    long start = System.currentTimeMillis();
    GraphStatistics statistics = new GraphStatistics(database, (short) 4, null);
    statistics.collectStatistics(new File[] { encodedInput });
    System.out
            .println("\tStatistics collection = " + (System.currentTimeMillis() - start) + " msec");
    start = System.currentTimeMillis();
    statistics.adjustOwnership(new File[] { encodedInput }, workingDir);
    System.out.println("\tadjusting ownership = " + (System.currentTimeMillis() - start) + " msec");
    statistics.close();
  }

}
