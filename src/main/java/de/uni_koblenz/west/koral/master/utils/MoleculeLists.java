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
package de.uni_koblenz.west.koral.master.utils;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Creates a list of statements for each molecule as one file of linked lists
 * where the head points to the next element. Each statement consists of (i) the
 * previous pointer, (ii) the subject, (iii) the property, (iv) the object, (v)
 * the containment length and (vi) the containment.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MoleculeLists implements Closeable {

  static final long TAIL_OFFSET_OF_LIST = -1;

  /**
   * values are two long values: (i) offset of the head of the corresponding
   * statement list (ii) the size of the statement list
   */
  private RocksDB vertex2lastElementOffset;

  private final File vertex2lastElementOffsetFolder;

  private final File statementListsFile;

  private long fileSize;

  private DataOutputStream statementLists;

  public MoleculeLists(File workingDir) {
    this(workingDir,
            new File(workingDir.getAbsolutePath() + File.separator + "vertex2lastElementOffset"),
            new File(workingDir.getAbsolutePath() + File.separator + "moleculeLists"));
  }

  private MoleculeLists(File workingDir, File rocksDBFolder, File statementListsFile) {
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setMaxOpenFiles(50);
    options.setWriteBufferSize(64 * 1024 * 1024); 
    vertex2lastElementOffsetFolder = rocksDBFolder;
    if (!vertex2lastElementOffsetFolder.exists()) {
      vertex2lastElementOffsetFolder.mkdirs();
    }
    try {
      vertex2lastElementOffset = RocksDB.open(options, vertex2lastElementOffsetFolder.getAbsolutePath()
              + File.separator + "vertex2lastElementOffset");
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    this.statementListsFile = statementListsFile;
    fileSize = 0;
  }

  public void add(Statement statement) {
    try {
      if (statementLists == null) {
        statementLists = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(statementListsFile, true)));
      }

      long previous = MoleculeLists.TAIL_OFFSET_OF_LIST;
      long length = 0;
      byte[] vertexArray = statement.getSubject();
      byte[] offsetLengthArray = vertex2lastElementOffset.get(vertexArray);
      if (offsetLengthArray != null) {
        previous = NumberConversion.bytes2long(offsetLengthArray, 0);
        length = NumberConversion.bytes2long(offsetLengthArray, Long.BYTES);
      } else {
        offsetLengthArray = new byte[Long.BYTES * 2];
      }
      long offset = fileSize;

      statementLists.writeLong(previous);
      fileSize += Long.BYTES;
      statementLists.write(statement.getSubject());
      fileSize += statement.getSubject().length;
      statementLists.write(statement.getProperty());
      fileSize += statement.getProperty().length;
      statementLists.write(statement.getObject());
      fileSize += statement.getObject().length;
      statementLists.writeInt(statement.getContainment().length);
      fileSize += Integer.BYTES;
      statementLists.write(statement.getContainment());
      fileSize += statement.getContainment().length;
      length++;

      NumberConversion.long2bytes(offset, offsetLengthArray, 0);
      NumberConversion.long2bytes(length, offsetLengthArray, Long.BYTES);
      vertex2lastElementOffset.put(vertexArray, offsetLengthArray);
    } catch (IOException | RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public MoleculeListIterator iterator(long vertex, boolean readOnly) {
    try {
      if (statementLists != null) {
        statementLists.close();
        statementLists = null;
      }
      byte[] vertexArray = NumberConversion.long2bytes(vertex);
      byte[] offsetLengthArray = vertex2lastElementOffset.get(vertexArray);
      if (offsetLengthArray != null) {
        return new MoleculeListIterator(statementListsFile,
                NumberConversion.bytes2long(offsetLengthArray, 0),
                NumberConversion.bytes2long(offsetLengthArray, Long.BYTES), readOnly);
      } else {
        return new MoleculeListIterator(statementListsFile, MoleculeLists.TAIL_OFFSET_OF_LIST, 0,
                true);
      }
    } catch (IOException | RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    vertex2lastElementOffset.close();
    if (vertex2lastElementOffsetFolder.exists()) {
      for (File file : vertex2lastElementOffsetFolder.listFiles()) {
        file.delete();
      }
      vertex2lastElementOffsetFolder.delete();
    }
    try {
      if (statementLists != null) {
        statementLists.close();
      }
      statementListsFile.delete();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
