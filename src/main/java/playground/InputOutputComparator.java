package playground;

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedLongFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.LongOutputWriter;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.master.utils.FixedSizeLongArrayComparator;
import de.uni_koblenz.west.koral.master.utils.InitialChunkProducer;
import de.uni_koblenz.west.koral.master.utils.LongIterator;
import de.uni_koblenz.west.koral.master.utils.Merger;
import de.uni_koblenz.west.koral.master.utils.NWayMergeSort;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class InputOutputComparator {

  private static final int MAX_NUMBER_OF_OPEN_FILES = 100;

  private static final long MAX_CASH_SIZE = 0x80_00_00L;

  public static void main(String[] args) {
    if (args.length < 3) {
      System.out.println("usage: java " + InputOutputComparator.class.getName()
              + " <inputFile> <chunkDir> <workingDir>");
      return;
    }
    File inputFile = new File(args[0]);
    File workingDir = new File(args[2] + File.separator + "comparatorWorkingDir");
    if (!workingDir.exists()) {
      workingDir.mkdirs();
    }

    File sortedInput = InputOutputComparator.createSortedInputDir(inputFile, workingDir,
            InputOutputComparator.MAX_NUMBER_OF_OPEN_FILES, InputOutputComparator.MAX_CASH_SIZE);

    File chunkDir = new File(args[1]);
    File sortedChunks = InputOutputComparator.createSortedChunks(chunkDir, workingDir,
            InputOutputComparator.MAX_NUMBER_OF_OPEN_FILES, InputOutputComparator.MAX_CASH_SIZE);

    InputOutputComparator.compareFiles(sortedInput, sortedChunks);

    sortedInput.delete();
    sortedChunks.delete();
  }

  private static File createSortedInputDir(File inputFile, File workingDir,
          int maxNumberOfOpenFiles, long maxCashSize) {
    InitialChunkProducer producer = null;
    Merger merger = null;
    try {
      producer = new InitialChunkProducer() {

        /**
         * [subject, property, object]*
         */
        private long[][] elements;

        private int nextIndex;

        private EncodedFileInputStream graphInput;

        private Iterator<Statement> iterator;

        private long numberOfReadChunks = 0;

        @Override
        public void loadNextChunk() throws IOException {
          if (graphInput == null) {
            graphInput = new EncodedFileInputStream(EncodingFileFormat.EEE, inputFile);
          }
          if (elements == null) {
            elements = new long[(int) (maxCashSize / Long.BYTES / 3)][3];
          }
          if (iterator == null) {
            iterator = graphInput.iterator();
          }
          for (nextIndex = 0; (nextIndex < elements.length) && iterator.hasNext(); nextIndex++) {
            Statement statement = iterator.next();
            long subject = statement.getSubjectAsLong();
            long property = statement.getPropertyAsLong();
            long object = statement.getObjectAsLong();
            elements[nextIndex][0] = subject;
            elements[nextIndex][1] = property;
            elements[nextIndex][2] = object;
            numberOfReadChunks++;
          }
        }

        @Override
        public void sort(Comparator<long[]> comparator) {
          Arrays.sort(elements, 0, nextIndex, comparator);
        }

        @Override
        public boolean hasNextChunk() {
          return nextIndex > 0;
        }

        @Override
        public void writeChunk(LongOutputWriter output) throws IOException {
          for (int i = 0; i < nextIndex; i++) {
            output.writeLong(elements[i][0]);
            output.writeLong(elements[i][1]);
            output.writeLong(elements[i][2]);
          }
        }

        @Override
        public void close() {
          System.out.println("Read from input file: " + numberOfReadChunks + " triples");
          if (graphInput != null) {
            try {
              graphInput.close();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      };
      merger = new Merger() {

        @Override
        public void startNextMergeLevel() {
        }

        @Override
        public long[] readNextElement(LongIterator iterator) throws IOException {
          return new long[] { iterator.next(), iterator.next(), iterator.next() };
        }

        @Override
        public void mergeAndWrite(BitSet indicesOfSmallestElement, long[][] elements,
                LongIterator[] iterators, LongOutputWriter out) throws IOException {
          for (int i = indicesOfSmallestElement.nextSetBit(0); i >= 0; i = indicesOfSmallestElement
                  .nextSetBit(i + 1)) {
            out.writeLong(elements[i][0]);
            out.writeLong(elements[i][1]);
            out.writeLong(elements[i][2]);
          }
        }

        @Override
        public void close() {
        }
      };
      Comparator<long[]> comparator = new FixedSizeLongArrayComparator(true, 0, 1, 2);
      File sortedInputTriples = File.createTempFile("sortedInputTriples", "", workingDir);
      NWayMergeSort sort = new NWayMergeSort();
      sort.sort(producer, merger, comparator, workingDir, maxNumberOfOpenFiles, sortedInputTriples);
      return sortedInputTriples;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (producer != null) {
        producer.close();
      }
      if (merger != null) {
        merger.close();
      }
    }
  }

  private static File createSortedChunks(File chunkDir, File workingDir, int maxNumberOfOpenFiles,
          long maxCashSize) {
    InitialChunkProducer producer = null;
    Merger merger = null;
    try {
      producer = new InitialChunkProducer() {

        /**
         * [subject, property, object]*
         */
        private long[][] elements;

        private int nextIndex;

        private final File[] chunks = chunkDir.listFiles(new FilenameFilter() {

          @Override
          public boolean accept(File dir, String name) {
            return name.matches(Pattern.quote("chunk") + "[0-9]+" + Pattern.quote(".adj.gz"));
          }
        });

        private int nextChunk = 0;

        private EncodedFileInputStream graphInput;

        private Iterator<Statement> iterator;

        private long numberOfReadChunks = 0;

        @Override
        public void loadNextChunk() throws IOException {
          if (elements == null) {
            elements = new long[(int) (maxCashSize / Long.BYTES / 3)][3];
          }
          for (nextIndex = 0; nextIndex < elements.length; nextIndex++) {
            loadNextChunkFile();
            if ((iterator == null) || !iterator.hasNext()) {
              break;
            }
            Statement statement = iterator.next();
            long subject = statement.getSubjectAsLong() & 0x00_00_ff_ff_ff_ff_ff_ffL;
            long property = statement.getPropertyAsLong() & 0x00_00_ff_ff_ff_ff_ff_ffL;
            long object = statement.getObjectAsLong() & 0x00_00_ff_ff_ff_ff_ff_ffL;
            elements[nextIndex][0] = subject;
            elements[nextIndex][1] = property;
            elements[nextIndex][2] = object;
            numberOfReadChunks++;
          }
        }

        private void loadNextChunkFile() throws FileNotFoundException, IOException {
          while (((iterator == null) || !iterator.hasNext()) && (nextChunk < chunks.length)) {
            graphInput = new EncodedFileInputStream(EncodingFileFormat.EEE, chunks[nextChunk]);
            iterator = graphInput.iterator();
            nextChunk++;
          }
        }

        @Override
        public void sort(Comparator<long[]> comparator) {
          Arrays.sort(elements, 0, nextIndex, comparator);
        }

        @Override
        public boolean hasNextChunk() {
          return nextIndex > 0;
        }

        @Override
        public void writeChunk(LongOutputWriter output) throws IOException {
          for (int i = 0; i < nextIndex; i++) {
            output.writeLong(elements[i][0]);
            output.writeLong(elements[i][1]);
            output.writeLong(elements[i][2]);
          }
        }

        @Override
        public void close() {
          System.out.println("Read from chunks: " + numberOfReadChunks + " triples");
          if (graphInput != null) {
            try {
              graphInput.close();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      };
      merger = new Merger() {

        @Override
        public void startNextMergeLevel() {
        }

        @Override
        public long[] readNextElement(LongIterator iterator) throws IOException {
          return new long[] { iterator.next(), iterator.next(), iterator.next() };
        }

        @Override
        public void mergeAndWrite(BitSet indicesOfSmallestElement, long[][] elements,
                LongIterator[] iterators, LongOutputWriter out) throws IOException {
          for (int i = indicesOfSmallestElement.nextSetBit(0); i >= 0; i = indicesOfSmallestElement
                  .nextSetBit(i + 1)) {
            out.writeLong(elements[i][0]);
            out.writeLong(elements[i][1]);
            out.writeLong(elements[i][2]);
          }
        }

        @Override
        public void close() {
        }
      };
      Comparator<long[]> comparator = new FixedSizeLongArrayComparator(true, 0, 1, 2);
      File sortedChunkTriples = File.createTempFile("sortedChunkTriples", "", workingDir);
      NWayMergeSort sort = new NWayMergeSort();
      sort.sort(producer, merger, comparator, workingDir, maxNumberOfOpenFiles, sortedChunkTriples);
      return sortedChunkTriples;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (producer != null) {
        producer.close();
      }
      if (merger != null) {
        merger.close();
      }
    }
  }

  private static void compareFiles(File sortedInput, File sortedChunks) {
    long numberOfReadTriples = 0;
    try (EncodedLongFileInputStream sInput = new EncodedLongFileInputStream(sortedInput);
            LongIterator sIter = sInput.iterator();
            EncodedLongFileInputStream cInput = new EncodedLongFileInputStream(sortedChunks);
            LongIterator cIter = cInput.iterator();) {
      Comparator<long[]> comparator = new FixedSizeLongArrayComparator(true, 0, 1, 2);
      while (sIter.hasNext() && cIter.hasNext()) {
        numberOfReadTriples++;
        long[] sTriple = new long[] { sIter.next(), sIter.next(), sIter.next() };
        long[] cTriple = new long[] { cIter.next(), cIter.next(), cIter.next() };
        int comparison = comparator.compare(sTriple, cTriple);
        if (comparison < 0) {
          throw new RuntimeException("The triple " + Arrays.toString(sTriple)
                  + " from the input is not contained in the chunks. The next chunk triple would be: "
                  + Arrays.toString(cTriple));
        } else if (comparison > 0) {
          throw new RuntimeException("The triple " + Arrays.toString(cTriple)
                  + " from the chunks was not contained in the original input. The next original input triple would be: "
                  + Arrays.toString(sTriple));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      System.out.println("Number of compared triples: " + numberOfReadTriples);
    }
  }

}
