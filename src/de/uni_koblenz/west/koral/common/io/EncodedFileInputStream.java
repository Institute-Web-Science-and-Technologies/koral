package de.uni_koblenz.west.koral.common.io;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

/**
 * Reads data from a file respecting the {@link EncodingFileFormat}. The v-byte
 * encoded long values in the input file are decoded.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class EncodedFileInputStream implements AutoCloseable {

  private final EncodingFileFormat inputFormat;

  private final DataInputStream input;

  public EncodedFileInputStream(EncodingFileFormat inputFormat, File inputFile)
          throws FileNotFoundException, IOException {
    super();
    this.inputFormat = inputFormat;
    input = new DataInputStream(
            new BufferedInputStream(new GZIPInputStream(new FileInputStream(inputFile))));
  }

  /**
   * @return {@link Statement} singleton whose content is changed for each call
   *         of this method
   * @throws EOFException
   *           if the end of the file is reached
   * @throws IOException
   */
  public Statement read() throws EOFException, IOException {
    byte[] subject = inputFormat.isSubjectEncoded() ? readEncodedLong() : readString();
    byte[] property = inputFormat.isPropertyEncoded() ? readEncodedLong() : readString();
    byte[] object = inputFormat.isObjectEncoded() ? readEncodedLong() : readString();

    int length = input.readShort() & 0xff_ff;
    byte[] containment = new byte[length];
    input.readFully(containment);
    return Statement.getStatement(inputFormat, subject, property, object, containment);
  }

  private byte[] readString() throws IOException {
    int length = input.readInt();
    byte[] stringContent = new byte[length];
    input.readFully(stringContent);
    return stringContent;
  }

  private byte[] readEncodedLong() throws IOException {
    long result = 0;
    byte currentBlock;
    do {
      currentBlock = input.readByte();
      result = result << 7;
      long value = currentBlock & 0b0111_1111;
      result = result | value;
    } while (currentBlock >= 0);
    return NumberConversion.long2bytes(currentBlock);
  }

  @Override
  public void close() throws Exception {
    if (input != null) {
      input.close();
    }
  }

}
