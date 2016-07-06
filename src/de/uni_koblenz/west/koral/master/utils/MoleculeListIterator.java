package de.uni_koblenz.west.koral.master.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.NoSuchElementException;

/**
 * Iterator for a molecule list created by {@link MoleculeLists}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MoleculeListIterator implements AutoCloseable {

  private final RandomAccessFile statementLists;

  private long nextOffset;

  private final long size;

  public MoleculeListIterator(File statementListsFile, long offsetOfStatementListsHead,
          long sizeOfStatementLists, boolean readOnly) {
    size = sizeOfStatementLists;
    nextOffset = offsetOfStatementListsHead;
    try {
      statementLists = new RandomAccessFile(statementListsFile, readOnly ? "r" : "rw");
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public long size() {
    return size;
  }

  public boolean hasNext() {
    return nextOffset != MoleculeLists.TAIL_OFFSET_OF_LIST;
  }

  public byte[][] next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    try {
      statementLists.seek(nextOffset);
      nextOffset = statementLists.readLong();
      byte[][] statement = new byte[4][];
      for (int i = 0; i < 3; i++) {
        statement[i] = new byte[Long.BYTES];
        statementLists.readFully(statement[i]);
      }
      statement[3] = new byte[statementLists.readInt()];
      statementLists.readFully(statement[3]);
      return statement;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void updateContainment(byte[][] statement, int targetChunk) {
    byte[] containment = statement[statement.length - 1];
    int bitsetIndex = targetChunk / Byte.SIZE;
    byte bitsetMask = getBitMaskFor(targetChunk + 1);
    containment[bitsetIndex] |= bitsetMask;
    try {
      statementLists.seek(statementLists.getFilePointer() - containment.length);
      statementLists.write(containment);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private byte getBitMaskFor(int computerId) {
    computerId -= 1;
    switch (computerId % Byte.SIZE) {
      case 0:
        return (byte) 0x80;
      case 1:
        return (byte) 0x40;
      case 2:
        return (byte) 0x20;
      case 3:
        return (byte) 0x10;
      case 4:
        return (byte) 0x08;
      case 5:
        return (byte) 0x04;
      case 6:
        return (byte) 0x02;
      case 7:
        return (byte) 0x01;
    }
    return 0;
  }

  @Override
  public void close() {
    try {
      statementLists.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
