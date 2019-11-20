package playground;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * This class tests the hypothesis that frequent copies of frequently used
 * arrays might result in CPU cache misses because the array moves into RAM
 * after the CPU cache runs out of space.
 *
 * @author Philipp Töws
 *
 */
public class CPUCacheBenchmark {

  private static long[] read(byte[] array, int count, boolean logTimes) {
    long[] times = null;
    if (logTimes) {
      times = new long[count];
    }
    int sum = 0;
    for (int n = 0; n < count; n++) {
      long start = System.nanoTime();
      for (int i = 0; i < array.length; i++) {
        sum += array[i];
      }
      if (logTimes) {
        long time = System.nanoTime() - start;
        times[n] = time;
      }
    }
    System.out.println(sum);
    return times;
  }

  public static void main(String[] args) {
    int arraySize = Integer.parseInt(args[0]);
    int clones = Integer.parseInt(args[1]);
    byte[] array = new byte[arraySize];
    long[] initialReadTimes = CPUCacheBenchmark.read(array, 3, true);
    // Fill with some non-zero content
    for (int i = 0; i < array.length; i++) {
      array[i] = (byte) i;
    }
    long[] afterWriteTimes = CPUCacheBenchmark.read(array, 3, true);
    // Make this array important, so it lands in CPU Cache
    CPUCacheBenchmark.read(array, 1_000_000, false);

    long[] afterReadTimes = CPUCacheBenchmark.read(array, 3, true);

    // Dont put this into a method because the array reference would not be
    // changed
    long[] afterFirstCloneReadTimes = null;
    // long[] cloneTimes = new long[100];
    byte[] copy = new byte[array.length];
    System.arraycopy(array, 0, copy, 0, array.length);
    // long start = System.nanoTime();
    for (int i = 1; i <= clones; i++) {
      byte[] copy2 = new byte[copy.length];
      System.arraycopy(copy, 0, copy2, 0, copy.length);
      copy = copy2;
      // if (((i % (clones / 100)) == 0)) {
      // long time = System.nanoTime() - start;
      // cloneTimes[(i / (clones / 100)) - 1] = time;
      // start = System.nanoTime();
      // }
      if (i == 1) {
        afterFirstCloneReadTimes = CPUCacheBenchmark.read(array, 3, true);
      }
    }

    long[] afterAllClonesReadTimes = CPUCacheBenchmark.read(array, 3, true);

    // Write to CSV
    String config = String.format("%,d", arraySize).replace(",", "_") + "-arraysize-"
            + String.format("%,d", clones).replace(",", "_") + "-clones";
    File readTimesCSV = new File("CPUCB_readtimes_" + config + ".csv");
    CPUCacheBenchmark.writeReadTimesCSV(readTimesCSV, initialReadTimes, afterWriteTimes,
            afterReadTimes, afterFirstCloneReadTimes, afterAllClonesReadTimes);
    // File cloneTimesCSV = new File("CPUCB_clonetimes_" + config + ".csv");
    // writeCloneTimesCSV(cloneTimesCSV, cloneTimes, clones / 100);
    System.out.println("Finished.");
  }

  private static void printCSVRecord(CSVPrinter printer, String label, long[] readTimes)
          throws IOException {
    printer.print(label);
    for (long time : readTimes) {
      printer.print(time);
    }
    printer.println();
  }

  private static void writeReadTimesCSV(File csvFile, long[] initialReadTimes,
          long[] afterWriteTimes, long[] afterReadTimes, long[] afterFirstCloneReadTimes,
          long[] afterAllClonesReadTimes) {
    CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
    try (CSVPrinter printer = new CSVPrinter(
            new OutputStreamWriter(new FileOutputStream(csvFile, false), "UTF-8"), csvFileFormat)) {

      // Print column headers
      printer.print("READ_TYPE");
      int maxCount = Math.max(initialReadTimes.length,
              Math.max(afterAllClonesReadTimes.length, afterFirstCloneReadTimes.length));
      for (int i = 1; i <= maxCount; i++) {
        printer.print("#" + i);
      }
      printer.println();

      // Print read times
      CPUCacheBenchmark.printCSVRecord(printer, "INITIAL_READ_TIMES", initialReadTimes);
      CPUCacheBenchmark.printCSVRecord(printer, "AFTER_WRITE_TIMES", afterWriteTimes);
      CPUCacheBenchmark.printCSVRecord(printer, "AFTER_READ_TIMES", afterReadTimes);
      CPUCacheBenchmark.printCSVRecord(printer, "AFTER_FIRST_CLONE_READ_TIMES",
              afterFirstCloneReadTimes);
      CPUCacheBenchmark.printCSVRecord(printer, "AFTER_ALL_CLONES_READ_TIMES",
              afterAllClonesReadTimes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unused")
  private static void writeCloneTimesCSV(File csvFile, long[] cloneTimes, int intervalLength) {
    CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
    try (CSVPrinter printer = new CSVPrinter(
            new OutputStreamWriter(new FileOutputStream(csvFile, false), "UTF-8"), csvFileFormat)) {

      // Print column headers
      printer.printRecord("INTERVAL", "TOTAL_CLONE_TIME");

      // Print times
      for (int i = 0; i < cloneTimes.length; i++) {
        printer.print((i + 1) * intervalLength);
        printer.print(cloneTimes[i]);
        printer.println();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
