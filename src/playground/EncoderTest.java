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

import org.apache.jena.graph.Node;

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.common.utils.RDFFileIterator;
import de.uni_koblenz.west.koral.master.dictionary.Dictionary;
import de.uni_koblenz.west.koral.master.dictionary.impl.RocksDBDictionary;
import de.uni_koblenz.west.koral.master.utils.DeSerializer;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * Tests encoding of graph files
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class EncoderTest {
  public static void main(String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: java " + DictionaryTest.class.getName()
              + " <storageDir> <graphDataFileOrFolder> <EncodingFormat>");
      return;
    }
    File dir = new File(args[0]);
    EncodingFileFormat format = EncodingFileFormat.valueOf(args[2]);
    Dictionary dictionary = new RocksDBDictionary(
            dir.getAbsolutePath() + File.separator + "dictionary");
    File outputFile = new File(dir.getAbsolutePath() + File.separator + "encoded.bin.gz");
    EncoderTest.encode(args[1], dictionary, outputFile, format);
    EncoderTest.decode(outputFile, dictionary, format);
    dictionary.close();
  }

  private static void decode(File inputFile, Dictionary dictionary, EncodingFileFormat format) {
    try (EncodedFileInputStream in = new EncodedFileInputStream(format, inputFile);) {
      for (Statement statement : in) {
        System.out.print("\n"
                + (statement.isSubjectEncoded() ? dictionary.decode(statement.getSubjectAsLong())
                        : statement.getSubjectAsString()));
        System.out.print(" "
                + (statement.isPropertyEncoded() ? dictionary.decode(statement.getPropertyAsLong())
                        : statement.getPropertyAsString()));
        System.out.print(" " + (statement.isObjectEncoded()
                ? dictionary.decode(statement.getObjectAsLong()) : statement.getObjectAsString()));
        System.out.print(" " + Arrays.toString(statement.getContainment()));
      }
    } catch (EOFException e) {
      // file was read completely
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
  }

  private static void encode(String graphFile, Dictionary dictionary, File outputFile,
          EncodingFileFormat format) {
    long start = System.currentTimeMillis();
    byte[] containment = new byte[] { 1 };
    try (RDFFileIterator iter = new RDFFileIterator(new File(graphFile), false, null);
            EncodedFileOutputStream out = new EncodedFileOutputStream(outputFile)) {
      long numberOfTriples = 0;
      SimpleDateFormat dateFormat = new SimpleDateFormat();
      for (Node[] quad : iter) {
        String subject = DeSerializer.serializeNode(quad[0]);
        long subjectEnc = dictionary.encode(subject, true);
        String property = DeSerializer.serializeNode(quad[1]);
        long propertyEnc = dictionary.encode(property, true);
        String object = DeSerializer.serializeNode(quad[2]);
        long objectEnc = dictionary.encode(object, true);

        byte[] subjectA = format.isSubjectEncoded() ? NumberConversion.long2bytes(subjectEnc)
                : subject.getBytes("UTF-8");
        byte[] propertyA = format.isPropertyEncoded() ? NumberConversion.long2bytes(propertyEnc)
                : property.getBytes("UTF-8");
        byte[] objectA = format.isObjectEncoded() ? NumberConversion.long2bytes(objectEnc)
                : object.getBytes("UTF-8");

        out.writeStatement(
                Statement.getStatement(format, subjectA, propertyA, objectA, containment));
        numberOfTriples++;
        if ((numberOfTriples % 1_000_000) == 0) {
          System.out.println(dateFormat.format(new Date(System.currentTimeMillis())) + " "
                  + (numberOfTriples / 1_000_000) + "M triples");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    dictionary.flush();
    System.out.println("duration: " + (System.currentTimeMillis() - start) + " msec");
  }
}
