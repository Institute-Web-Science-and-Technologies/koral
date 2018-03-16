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
package de.uni_koblenz.west.koral.master.dictionary.impl;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.dictionary.Dictionary;
import de.uni_koblenz.west.koral.master.dictionary.LongDictionary;

/**
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class RocksDBDictionary implements Dictionary, LongDictionary {

	public static final int DEFAULT_MAX_BATCH_SIZE = 100000;

	private final String storageDir;

	private final int maxOpenFiles;

	private RocksDB encoder;

	private WriteBatch encoderBatch;

	private RocksDB decoder;

	private WriteBatch decoderBatch;

	private Map<ArrayWrapper, byte[]> entriesInBatch;

	private final int maxBatchEntries;

	/**
	 * id 0 indicates that a string in a query has not been encoded yet
	 */
	private long nextID = 1;

	private final long maxID = 0x0000ffffffffffffL;

	public RocksDBDictionary(String storageDir) {
		this(storageDir, RocksDBDictionary.DEFAULT_MAX_BATCH_SIZE);
	}

	public RocksDBDictionary(String storageDir, int maxBatchEntries) {
		this(storageDir, maxBatchEntries, 400);
	}

	public RocksDBDictionary(String storageDir, int maxBatchEntries, int maxOpenFiles) {
		this.maxOpenFiles = maxOpenFiles;
		this.maxBatchEntries = maxBatchEntries;
		this.storageDir = storageDir;
		File dictionaryDir = new File(storageDir);
		if (!dictionaryDir.exists()) {
			dictionaryDir.mkdirs();
		}
		Options options = getOptions(maxOpenFiles);
		try {
			encoder = RocksDB.open(options, storageDir + File.separator + "encoder");
			decoder = RocksDB.open(options, storageDir + File.separator + "decoder");
		} catch (RocksDBException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	private Options getOptions(int maxOpenFiles) {
		Options options = new Options();
		options.setCreateIfMissing(true);
		options.setMaxOpenFiles(maxOpenFiles);
		options.setMaxFileOpeningThreads(1);
		options.setWriteBufferSize(64 * 1024 * 1024);
		return options;
	}

	@Override
	public long encode(String value, boolean createNewEncodingForUnknownNodes) {
		byte[] valueBytes = null;
		try {
			valueBytes = value.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e1) {
			throw new RuntimeException(e1);
		}
		return internalEncode(valueBytes, createNewEncodingForUnknownNodes);
	}

	@Override
	public long encode(long value, boolean createNewEncodingForUnknownNodes) {
		byte[] valueBytes = NumberConversion.long2bytes(value);
		return internalEncode(valueBytes, createNewEncodingForUnknownNodes);
	}

	private long internalEncode(byte[] valueBytes, boolean createNewEncodingForUnknownNodes) {
		byte[] id = null;
		try {
			// check cache, first
			if (entriesInBatch != null) {
				id = entriesInBatch.get(new ArrayWrapper(valueBytes));
			}
			if (id == null) {
				id = encoder.get(valueBytes);
			}
		} catch (RocksDBException e) {
			close();
			throw new RuntimeException(e);
		}
		if (id == null) {
			if (nextID > maxID) {
				throw new RuntimeException("The maximum number of Strings have been encoded.");
			} else if (!createNewEncodingForUnknownNodes) {
				return 0;
			} else {
				id = NumberConversion.long2bytes(nextID);
				put(valueBytes, id);
				nextID++;
			}
		}
		return NumberConversion.bytes2long(id);
	}

	private void put(byte[] valueBytes, byte[] id) {
		if (entriesInBatch == null) {
			entriesInBatch = new HashMap<>();
		}
		entriesInBatch.put(new ArrayWrapper(valueBytes), id);
		if (encoderBatch == null) {
			encoderBatch = new WriteBatch();
		}
		encoderBatch.put(valueBytes, id);
		if (decoderBatch == null) {
			decoderBatch = new WriteBatch();
		}
		decoderBatch.put(id, valueBytes);
		if (entriesInBatch.size() == maxBatchEntries) {
			internalFlush();
		}
	}

	@Override
	public String decode(long id) {
		try {
			byte[] valueBytes = decoder.get(NumberConversion.long2bytes(id));
			if (valueBytes == null) {
				return null;
			}
			return new String(valueBytes, "UTF-8");
		} catch (RocksDBException | UnsupportedEncodingException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	@Override
	public long decodeLong(long id) {
		try {
			byte[] valueBytes = decoder.get(NumberConversion.long2bytes(id));
			if (valueBytes == null) {
				throw new NoSuchElementException("The id " + id + " has not been encoded, yet.");
			}
			return NumberConversion.bytes2long(valueBytes);
		} catch (RocksDBException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean isEmpty() {
		return nextID == 1;
	}

	@Override
	public long size() {
		return nextID - 1;
	}

	@Override
	public void flush() {
		internalFlush();
		try {
			encoder.compactRange();
			decoder.compactRange();
		} catch (RocksDBException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	private void internalFlush() {
		try {
			WriteOptions writeOpts = new WriteOptions();
			if (encoderBatch != null) {
				encoder.write(writeOpts, encoderBatch);
				encoderBatch = null;
			}
			if (decoderBatch != null) {
				decoder.write(writeOpts, decoderBatch);
				decoderBatch = null;
			}
			if (entriesInBatch != null) {
				entriesInBatch.clear();
			}
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void clear() {
		close();
		Options options = getOptions(maxOpenFiles);
		try {
			File encoderFile = new File(storageDir + File.separator + "encoder");
			deleteFile(encoderFile);
			encoder = RocksDB.open(options, encoderFile.getAbsolutePath());
			File decoderFile = new File(storageDir + File.separator + "decoder");
			deleteFile(decoderFile);
			decoder = RocksDB.open(options, decoderFile.getAbsolutePath());
		} catch (RocksDBException e) {
			close();
			throw new RuntimeException(e);
		}
		nextID = 1;
	}

	private void deleteFile(File file) {
		if (!file.exists()) {
			return;
		}
		if (file.isDirectory()) {
			for (File f : file.listFiles()) {
				deleteFile(f);
			}
		}
		int attempts = 0;
		while (!file.delete() && (attempts < 10)) {
			attempts++;
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}
	}

	@Override
	public void close() {
		internalFlush();
		if (encoder != null) {
			encoder.close();
		}
		if (decoder != null) {
			decoder.close();
		}
	}

	private static class ArrayWrapper {
		private final byte[] array;

		public ArrayWrapper(byte[] array) {
			this.array = array;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + Arrays.hashCode(array);
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			ArrayWrapper other = (ArrayWrapper) obj;
			if (!Arrays.equals(array, other.array)) {
				return false;
			}
			return true;
		}

	}

}
